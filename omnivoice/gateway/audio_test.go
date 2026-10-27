package gateway

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/plexusone/omnivoice-core/audio/format"
	coregateway "github.com/plexusone/omnivoice-core/gateway"
)

const testCallSID = "CA00000000000000000000000000000001"

func newTestGateway(t *testing.T, mode coregateway.PipelineMode) *Gateway {
	t.Helper()
	g, err := New(Config{
		AccountSID: "AC00000000000000000000000000000000",
		AuthToken:  "testtoken",
		Mode:       mode,
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return g
}

// dialMediaStream connects to the gateway's media stream handler as Twilio
// would.
func dialMediaStream(t *testing.T, g *Gateway) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(g.handleMediaStream))
	t.Cleanup(srv.Close)

	ws, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial media stream: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close handshake body: %v", err)
	}
	t.Cleanup(func() {
		// The gateway may already have closed the connection.
		if err := ws.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	})
	return ws
}

func sendJSON(t *testing.T, ws *websocket.Conn, msg any) {
	t.Helper()
	if err := ws.WriteJSON(msg); err != nil {
		t.Fatalf("write %v: %v", msg, err)
	}
}

func startStream(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	sendJSON(t, ws, map[string]any{"event": "connected"})
	sendJSON(t, ws, map[string]any{
		"event": "start",
		"start": map[string]any{
			"streamSid": "MZ0001",
			"callSid":   testCallSID,
			"mediaFormat": map[string]any{
				"encoding": "audio/x-mulaw", "sampleRate": 8000, "channels": 1,
			},
		},
	})
}

func sendMedia(t *testing.T, ws *websocket.Conn, audio []byte) {
	t.Helper()
	sendJSON(t, ws, map[string]any{
		"event":     "media",
		"streamSid": "MZ0001",
		"media":     map[string]any{"payload": base64.StdEncoding.EncodeToString(audio)},
	})
}

// waitEvent returns the first event of the given type.
func waitEvent(t *testing.T, sess coregateway.Session, want coregateway.EventType) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-sess.Events():
			if !ok {
				t.Fatalf("events closed before %s", want)
			}
			if ev.Type == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}

// readMessage reads the next message the gateway wrote to Twilio.
func readMessage(t *testing.T, ws *websocket.Conn) map[string]any {
	t.Helper()
	if err := ws.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	return msg
}

func TestAudioModeSessionCarriesCallAudio(t *testing.T) {
	g := newTestGateway(t, coregateway.PipelineModeAudio)
	created := g.createSession(testCallSID, "+15550000000", "+16695550100", "outbound")

	sess, ok := g.publicSession(created).(coregateway.AudioSession)
	if !ok {
		t.Fatal("audio mode session does not implement AudioSession")
	}
	if sess.AudioFormat() != format.Twilio {
		t.Fatalf("AudioFormat() = %+v, want Twilio mu-law 8 kHz", sess.AudioFormat())
	}
	if err := sess.SendAudio([]byte{1}); !errors.Is(err, coregateway.ErrAudioNotConnected) {
		t.Fatalf("SendAudio before connect = %v, want ErrAudioNotConnected", err)
	}

	ws := dialMediaStream(t, g)
	startStream(t, ws)
	waitEvent(t, sess, coregateway.EventSessionStarted)

	// Caller audio reaches the application unchanged.
	sendMedia(t, ws, []byte{0x7f, 0x00, 0xff})
	select {
	case got := <-sess.ReceiveAudio():
		if string(got) != string([]byte{0x7f, 0x00, 0xff}) {
			t.Fatalf("ReceiveAudio() = %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for caller audio")
	}

	// Application audio is sent to Twilio as a media message.
	if err := sess.SendAudio([]byte{0x01, 0x02}); err != nil {
		t.Fatalf("SendAudio() error: %v", err)
	}
	msg := readMessage(t, ws)
	media, _ := msg["media"].(map[string]any)
	if msg["event"] != "media" || msg["streamSid"] != "MZ0001" ||
		media["payload"] != base64.StdEncoding.EncodeToString([]byte{0x01, 0x02}) {
		t.Fatalf("sent message = %v", msg)
	}

	if err := sess.ClearAudio(); err != nil {
		t.Fatalf("ClearAudio() error: %v", err)
	}
	if msg := readMessage(t, ws); msg["event"] != "clear" {
		t.Fatalf("clear message = %v", msg)
	}

	// When Twilio stops the stream, the session ends.
	sendJSON(t, ws, map[string]any{"event": "stop", "stop": map[string]any{"callSid": testCallSID}})
	waitEvent(t, sess, coregateway.EventSessionEnded)
	select {
	case _, ok := <-sess.ReceiveAudio():
		if ok {
			t.Fatal("ReceiveAudio() delivered audio after stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReceiveAudio() not closed after stop")
	}
	if _, ok := g.GetSession(testCallSID); ok {
		t.Fatal("session still registered after the stream stopped")
	}
	if err := sess.SendAudio([]byte{1}); !errors.Is(err, coregateway.ErrSessionClosed) {
		t.Fatalf("SendAudio after stop = %v, want ErrSessionClosed", err)
	}
}

func TestAudioModeCloseBeforeConnect(t *testing.T) {
	g := newTestGateway(t, coregateway.PipelineModeAudio)
	sess, ok := g.publicSession(g.createSession(testCallSID, "", "", "outbound")).(coregateway.AudioSession)
	if !ok {
		t.Fatal("audio mode session does not implement AudioSession")
	}

	if err := sess.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if _, ok := <-sess.ReceiveAudio(); ok {
		t.Fatal("ReceiveAudio() open after Close")
	}
}

func TestOtherModesAreNotAudioSessions(t *testing.T) {
	for _, mode := range []coregateway.PipelineMode{"", coregateway.PipelineModeText, coregateway.PipelineModeRealtime} {
		g := newTestGateway(t, mode)
		g.createSession(testCallSID, "", "", "outbound")
		sess, ok := g.GetSession(testCallSID)
		if !ok {
			t.Fatalf("mode %q: session not found", mode)
		}
		if _, isAudio := sess.(coregateway.AudioSession); isAudio {
			t.Fatalf("mode %q: session implements AudioSession", mode)
		}
	}
}
