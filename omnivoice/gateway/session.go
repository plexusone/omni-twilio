package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	coregateway "github.com/plexusone/omnivoice-core/gateway"
	"github.com/plexusone/omnivoice-core/realtime"
)

// Verify interface compliance at compile time.
var _ coregateway.Session = (*Session)(nil)

// Session represents an active voice conversation session.
type Session struct {
	id        string
	gateway   *Gateway
	from      string
	to        string
	direction string
	startTime time.Time
	events    chan coregateway.Event
	done      chan struct{}
	logger    *slog.Logger

	mu               sync.RWMutex
	conn             *mediaStreamConn
	pipeline         *Pipeline                   // Text mode pipeline (STT→LLM→TTS)
	realtimeBridge   *coregateway.RealtimeBridge // Realtime mode bridge
	realtimeProvider realtime.Provider           // Realtime provider instance
	transcript       []coregateway.Turn
	metrics          coregateway.Metrics
	closed           bool
	closeOnce        sync.Once

	// audioRecv carries caller audio in PipelineModeAudio; nil otherwise.
	// runAudioPassthrough closes it once it has started forwarding,
	// otherwise Close does.
	audioRecv       chan []byte
	audioForwarding bool
}

// Type aliases for core gateway types.
type (
	Turn      = coregateway.Turn
	ToolCall  = coregateway.ToolCall
	Metrics   = coregateway.Metrics
	Event     = coregateway.Event
	EventType = coregateway.EventType
)

// Event type constants - aliases for core gateway constants.
const (
	EventSessionStarted   = coregateway.EventSessionStarted
	EventSessionEnded     = coregateway.EventSessionEnded
	EventUserSpeechStart  = coregateway.EventUserSpeechStart
	EventUserSpeechEnd    = coregateway.EventUserSpeechEnd
	EventUserTranscript   = coregateway.EventUserTranscript
	EventAgentThinking    = coregateway.EventAgentThinking
	EventAgentSpeechStart = coregateway.EventAgentSpeechStart
	EventAgentSpeechEnd   = coregateway.EventAgentSpeechEnd
	EventAgentTranscript  = coregateway.EventAgentTranscript
	EventToolCall         = coregateway.EventToolCall
	EventInterruption     = coregateway.EventInterruption
	EventError            = coregateway.EventError
)

// ID returns the session identifier (call SID).
func (s *Session) ID() string {
	return s.id
}

// From returns the caller phone number.
func (s *Session) From() string {
	return s.from
}

// To returns the called phone number.
func (s *Session) To() string {
	return s.to
}

// Direction returns "inbound" or "outbound".
func (s *Session) Direction() string {
	return s.direction
}

// StartTime returns when the session started.
func (s *Session) StartTime() time.Time {
	return s.startTime
}

// Duration returns the session duration.
func (s *Session) Duration() time.Duration {
	return time.Since(s.startTime)
}

// Events returns a channel for session events.
func (s *Session) Events() <-chan coregateway.Event {
	return s.events
}

// Transcript returns the conversation transcript.
func (s *Session) Transcript() []coregateway.Turn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	transcript := make([]coregateway.Turn, len(s.transcript))
	copy(transcript, s.transcript)
	return transcript
}

// Metrics returns session performance metrics.
func (s *Session) Metrics() coregateway.Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics := s.metrics
	metrics.SessionDurationMs = int(time.Since(s.startTime).Milliseconds())
	return metrics
}

// SendText sends text input to the agent (bypasses STT).
// Note: In realtime mode, this method is not supported as audio is processed directly.
func (s *Session) SendText(text string) error {
	s.mu.RLock()
	pipeline := s.pipeline
	s.mu.RUnlock()

	if pipeline == nil {
		return nil
	}

	return pipeline.ProcessText(context.Background(), text)
}

// Interrupt stops the current agent speech.
func (s *Session) Interrupt() {
	s.mu.RLock()
	pipeline := s.pipeline
	bridge := s.realtimeBridge
	s.mu.RUnlock()

	if pipeline != nil {
		pipeline.Interrupt()
	}

	if bridge != nil {
		bridge.Interrupt()
	}

	s.emitEvent(EventInterruption, nil)
	s.mu.Lock()
	s.metrics.InterruptionCount++
	s.mu.Unlock()
}

// Hangup ends the call and closes the session. Close alone only stops the
// session's media stream; Hangup also tells Twilio to end the call, including
// before the media stream connects, for example while keypad digits play.
func (s *Session) Hangup(ctx context.Context) error {
	if _, err := s.gateway.client.HangupCall(ctx, s.id); err != nil {
		return fmt.Errorf("hang up call %s: %w", s.id, err)
	}
	return s.Close()
}

// Close ends the session.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		if s.audioRecv != nil && !s.audioForwarding {
			close(s.audioRecv)
		}
		s.mu.Unlock()

		close(s.done)

		// Stop text pipeline
		if s.pipeline != nil {
			s.pipeline.Stop()
		}

		// Stop realtime bridge and provider
		if s.realtimeBridge != nil {
			_ = s.realtimeBridge.Close()
		}
		if s.realtimeProvider != nil {
			_ = s.realtimeProvider.Close()
		}

		// Close connection
		if s.conn != nil {
			s.conn.close()
		}

		// Remove from gateway
		s.gateway.removeSession(s.id)

		// Close events channel
		close(s.events)

		s.logger.Info("session closed",
			"duration", s.Duration().String(),
			"turns", len(s.transcript))
	})
	return nil
}

// startPipeline starts the voice processing pipeline.
func (s *Session) startPipeline(conn *mediaStreamConn) {
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), s.gateway.config.MaxSessionDuration)
	defer cancel()

	// Audio mode: hand the call's audio to the application
	if s.gateway.config.Mode == coregateway.PipelineModeAudio {
		s.runAudioPassthrough(ctx, conn)
		return
	}

	// Check if we should use realtime mode
	if s.gateway.config.Mode == coregateway.PipelineModeRealtime {
		s.startRealtimePipeline(ctx, conn)
		return
	}

	// Text mode: use STT→LLM→TTS pipeline
	s.startTextPipeline(ctx, conn)
}

// startTextPipeline starts the traditional STT→LLM→TTS pipeline.
func (s *Session) startTextPipeline(ctx context.Context, _ *mediaStreamConn) {
	// Create pipeline
	pipeline, err := NewPipeline(s)
	if err != nil {
		s.logger.Error("failed to create pipeline", "error", err)
		s.emitEvent(EventError, err)
		return
	}

	s.mu.Lock()
	s.pipeline = pipeline
	s.mu.Unlock()

	s.emitEvent(EventSessionStarted, nil)

	// Start pipeline
	if err := pipeline.Start(ctx); err != nil {
		s.logger.Error("pipeline error", "error", err)
		s.emitEvent(EventError, err)
	}
}

// startRealtimePipeline starts the native voice-to-voice pipeline using RealtimeBridge.
func (s *Session) startRealtimePipeline(ctx context.Context, conn *mediaStreamConn) {
	cfg := s.gateway.config

	// Create the realtime provider
	var provider realtime.Provider
	var err error

	if cfg.RealtimeProvider != nil && cfg.RealtimeConfig != nil {
		provider, err = cfg.RealtimeProvider.Create(cfg.RealtimeConfig)
		if err != nil {
			s.logger.Error("failed to create realtime provider", "error", err)
			s.emitEvent(EventError, err)
			return
		}
	} else {
		s.logger.Error("realtime mode requires RealtimeProvider and RealtimeConfig")
		s.emitEvent(EventError, err)
		return
	}

	s.mu.Lock()
	s.realtimeProvider = provider
	s.mu.Unlock()

	// Create the realtime bridge
	bridge := coregateway.NewRealtimeBridgeForTwilio(provider, cfg.RealtimeConfig.ToProcessConfig())

	s.mu.Lock()
	s.realtimeBridge = bridge
	s.mu.Unlock()

	// Start the bridge
	if err := bridge.Start(ctx); err != nil {
		s.logger.Error("failed to start realtime bridge", "error", err)
		s.emitEvent(EventError, err)
		return
	}

	s.emitEvent(EventSessionStarted, nil)

	// Let the read loop deliver caller audio; it drops audio until this is
	// closed, which the text pipeline does when it starts reading.
	close(conn.pipelineReady)

	// Forward bridge events to session events
	go s.forwardBridgeEvents(ctx, bridge)

	// Forward audio from Twilio to bridge
	go s.forwardInboundAudio(ctx, conn, bridge)

	// Forward audio from bridge to Twilio
	go s.forwardOutboundAudio(ctx, conn, bridge)

	// Wait for context cancellation
	<-ctx.Done()
}

// forwardBridgeEvents forwards events from the bridge to the session event channel.
func (s *Session) forwardBridgeEvents(ctx context.Context, bridge *coregateway.RealtimeBridge) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case event, ok := <-bridge.Events():
			if !ok {
				return
			}

			// Convert bridge event to session event
			s.emitEvent(event.Type, event.Data)

			// Update transcript for transcript events
			if event.Type == EventUserTranscript || event.Type == EventAgentTranscript {
				if text, ok := event.Data.(string); ok {
					role := "user"
					if event.Type == EventAgentTranscript {
						role = "agent"
					}
					s.addTurn(coregateway.Turn{
						Role:      role,
						Text:      text,
						Timestamp: event.Timestamp,
					})
				}
			}
		}
	}
}

// forwardInboundAudio forwards audio from Twilio WebSocket to the realtime bridge.
func (s *Session) forwardInboundAudio(ctx context.Context, conn *mediaStreamConn, bridge *coregateway.RealtimeBridge) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case audio, ok := <-conn.audioIn:
			if !ok {
				return
			}
			if err := bridge.SendAudio(audio); err != nil {
				s.logger.Warn("failed to send audio to bridge", "error", err)
			}
		}
	}
}

// forwardOutboundAudio forwards audio from the realtime bridge to Twilio WebSocket.
func (s *Session) forwardOutboundAudio(ctx context.Context, conn *mediaStreamConn, bridge *coregateway.RealtimeBridge) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case audio, ok := <-bridge.AudioOut():
			if !ok {
				return
			}
			if err := conn.sendAudio(audio); err != nil {
				s.logger.Warn("failed to send audio to Twilio", "error", err)
			}
		}
	}
}

// emitEvent sends an event to the events channel.
func (s *Session) emitEvent(eventType coregateway.EventType, data any) {
	event := coregateway.Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
	}

	// Hold the read lock so Close cannot close the channel mid-send.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}

	select {
	case s.events <- event:
	default:
		// Channel full, drop event
		s.logger.Warn("event channel full, dropping event", "type", eventType)
	}
}

// addTurn adds a conversation turn to the transcript.
func (s *Session) addTurn(turn coregateway.Turn) {
	s.mu.Lock()
	s.transcript = append(s.transcript, turn)
	s.metrics.TurnCount++
	s.mu.Unlock()
}

// updateMetrics updates session metrics.
func (s *Session) updateMetrics(sttLatency, llmLatency, ttsLatency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update averages (simple running average)
	n := float64(s.metrics.TurnCount)
	if n == 0 {
		n = 1
	}

	updateAvg := func(current int, newVal time.Duration) int {
		return int((float64(current)*(n-1) + float64(newVal.Milliseconds())) / n)
	}

	s.metrics.AvgSTTLatencyMs = updateAvg(s.metrics.AvgSTTLatencyMs, sttLatency)
	s.metrics.AvgLLMLatencyMs = updateAvg(s.metrics.AvgLLMLatencyMs, llmLatency)
	s.metrics.AvgTTSLatencyMs = updateAvg(s.metrics.AvgTTSLatencyMs, ttsLatency)
	s.metrics.AvgTotalLatencyMs = updateAvg(s.metrics.AvgTotalLatencyMs, sttLatency+llmLatency+ttsLatency)
}
