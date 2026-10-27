package gateway

import (
	"encoding/base64"
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
)

// mediaStreamConn wraps a WebSocket connection for Twilio Media Streams.
type mediaStreamConn struct {
	wsConn   *websocket.Conn
	gateway  *Gateway
	session  *Session
	events   chan Event
	audioIn  chan []byte // Audio from Twilio (user speech)
	audioOut chan []byte // Audio to Twilio (agent speech)
	done     chan struct{}

	sessionReady  chan struct{}
	pipelineReady chan struct{} // Signaled when pipeline starts reading
	streamSID     string
	callSID       string

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once

	// writeMu serializes WebSocket writes, which gorilla/websocket does not
	// allow concurrently.
	writeMu sync.Mutex
}

// Twilio Media Streams message types.
type mediaMessage struct {
	Event          string            `json:"event"`
	SequenceNumber string            `json:"sequenceNumber,omitempty"`
	StreamSID      string            `json:"streamSid,omitempty"`
	Start          *startMessage     `json:"start,omitempty"`
	Media          *mediaPayload     `json:"media,omitempty"`
	Mark           *markMessage      `json:"mark,omitempty"`
	Stop           *stopMessage      `json:"stop,omitempty"`
	DTMF           *dtmfMessage      `json:"dtmf,omitempty"`
	CustomParams   map[string]string `json:"customParameters,omitempty"`
}

type startMessage struct {
	StreamSID    string            `json:"streamSid"`
	AccountSID   string            `json:"accountSid"`
	CallSID      string            `json:"callSid"`
	Tracks       []string          `json:"tracks"`
	MediaFormat  mediaFormat       `json:"mediaFormat"`
	CustomParams map[string]string `json:"customParameters"`
}

type mediaFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sampleRate"`
	Channels   int    `json:"channels"`
}

type mediaPayload struct {
	Track     string `json:"track"`
	Chunk     string `json:"chunk"`
	Timestamp string `json:"timestamp"`
	Payload   string `json:"payload"` // Base64 encoded audio
}

type markMessage struct {
	Name string `json:"name"`
}

type stopMessage struct {
	AccountSID string `json:"accountSid"`
	CallSID    string `json:"callSid"`
}

type dtmfMessage struct {
	Digit string `json:"digit"`
}

// readLoop reads messages from the WebSocket.
func (c *mediaStreamConn) readLoop() {
	defer func() {
		c.close()
	}()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		_, data, err := c.wsConn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.gateway.logger.Debug("websocket read error", "error", err)
			}
			return
		}

		var msg mediaMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			c.gateway.logger.Debug("failed to parse media message", "error", err)
			continue
		}

		switch msg.Event {
		case "connected":
			c.gateway.logger.Debug("media stream connected")

		case "start":
			if msg.Start != nil {
				c.mu.Lock()
				c.streamSID = msg.Start.StreamSID
				c.callSID = msg.Start.CallSID
				c.mu.Unlock()

				c.gateway.logger.Info("media stream started",
					"stream_sid", msg.Start.StreamSID,
					"call_sid", msg.Start.CallSID,
					"encoding", msg.Start.MediaFormat.Encoding,
					"sample_rate", msg.Start.MediaFormat.SampleRate)

				// Find the session by call SID
				session, ok := c.gateway.getSessionInternal(msg.Start.CallSID)
				if !ok {
					// Create session if not exists (for outbound calls)
					from := msg.Start.CustomParams["from"]
					to := msg.Start.CustomParams["to"]
					session = c.gateway.createSession(msg.Start.CallSID, from, to, "outbound")
				}

				c.session = session

				// Signal that session is ready
				close(c.sessionReady)
			}

		case "media":
			if msg.Media != nil && msg.Media.Payload != "" {
				// Only process audio after session is ready
				if c.session == nil {
					continue
				}

				// Wait for pipeline to be ready before sending audio
				select {
				case <-c.pipelineReady:
					// Pipeline is ready to receive audio
				default:
					// Pipeline not ready yet, skip this audio chunk
					continue
				}

				// Decode base64 audio
				audio, err := base64.StdEncoding.DecodeString(msg.Media.Payload)
				if err != nil {
					c.gateway.logger.Debug("failed to decode audio", "error", err)
					continue
				}

				// Send to audio input channel
				select {
				case c.audioIn <- audio:
				default:
					// Channel full, drop audio (shouldn't happen often now)
				}
			}

		case "dtmf":
			if msg.DTMF != nil {
				c.gateway.logger.Debug("received DTMF", "digit", msg.DTMF.Digit)
				if c.session != nil {
					c.session.emitEvent(EventType("dtmf"), msg.DTMF.Digit)
				}
			}

		case "stop":
			c.gateway.logger.Info("media stream stopped",
				"call_sid", c.callSID)
			return

		case "mark":
			// Mark event - used for synchronization
			if msg.Mark != nil {
				c.gateway.logger.Debug("received mark", "name", msg.Mark.Name)
			}
		}
	}
}

// writeLoop writes audio to the WebSocket.
func (c *mediaStreamConn) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case audio := <-c.audioOut:
			if err := c.sendAudio(audio); err != nil {
				c.gateway.logger.Debug("failed to send audio", "error", err)
				return
			}
		}
	}
}

// sendAudio sends audio data to Twilio.
func (c *mediaStreamConn) sendAudio(audio []byte) error {
	c.mu.RLock()
	streamSID := c.streamSID
	closed := c.closed
	c.mu.RUnlock()

	if closed || streamSID == "" {
		return nil
	}

	// Encode audio to base64
	encoded := base64.StdEncoding.EncodeToString(audio)

	msg := map[string]any{
		"event":     "media",
		"streamSid": streamSID,
		"media": map[string]string{
			"payload": encoded,
		},
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.wsConn.WriteJSON(msg)
}

// sendMark sends a mark message for synchronization.
//
//nolint:unused // Reserved for barge-in detection and audio sync
func (c *mediaStreamConn) sendMark(name string) error {
	c.mu.RLock()
	streamSID := c.streamSID
	c.mu.RUnlock()

	if streamSID == "" {
		return nil
	}

	msg := map[string]any{
		"event":     "mark",
		"streamSid": streamSID,
		"mark": map[string]string{
			"name": name,
		},
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.wsConn.WriteJSON(msg)
}

// clear sends a clear message to clear the audio buffer.
func (c *mediaStreamConn) clear() error {
	c.mu.RLock()
	streamSID := c.streamSID
	c.mu.RUnlock()

	if streamSID == "" {
		return nil
	}

	msg := map[string]any{
		"event":     "clear",
		"streamSid": streamSID,
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.wsConn.WriteJSON(msg)
}

// close closes the connection.
func (c *mediaStreamConn) close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		close(c.done)
		close(c.audioIn)
		close(c.audioOut)
		_ = c.wsConn.Close()
	})
}

// AudioIn returns the channel for receiving audio from Twilio.
func (c *mediaStreamConn) AudioIn() <-chan []byte {
	return c.audioIn
}

// AudioOut returns the channel for sending audio to Twilio.
func (c *mediaStreamConn) AudioOut() chan<- []byte {
	return c.audioOut
}
