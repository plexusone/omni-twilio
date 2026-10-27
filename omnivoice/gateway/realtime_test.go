package gateway

import (
	"context"
	"testing"
	"time"

	coregateway "github.com/plexusone/omnivoice-core/gateway"
	"github.com/plexusone/omnivoice-core/realtime"
)

// fakeRealtime is a realtime provider that reports the audio it receives.
type fakeRealtime struct {
	received chan []byte
}

func (f *fakeRealtime) ProcessAudioStream(ctx context.Context, audioIn <-chan []byte, _ realtime.ProcessConfig) (<-chan realtime.AudioChunk, <-chan realtime.Transcript, error) {
	audioOut := make(chan realtime.AudioChunk)
	transcripts := make(chan realtime.Transcript)
	go func() {
		defer close(audioOut)
		defer close(transcripts)
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-audioIn:
				if !ok {
					return
				}
				select {
				case f.received <- chunk:
				default:
				}
			}
		}
	}()
	return audioOut, transcripts, nil
}

func (f *fakeRealtime) Name() string { return "fake" }
func (f *fakeRealtime) Close() error { return nil }

type fakeRealtimeFactory struct {
	provider *fakeRealtime
}

func (f fakeRealtimeFactory) Create(*coregateway.RealtimeConfig) (realtime.Provider, error) {
	return f.provider, nil
}
func (f fakeRealtimeFactory) Name() string { return "fake" }

func TestRealtimeModeReceivesCallerAudio(t *testing.T) {
	provider := &fakeRealtime{received: make(chan []byte, 8)}
	g, err := New(Config{
		AccountSID:       "AC00000000000000000000000000000000",
		AuthToken:        "testtoken",
		Mode:             coregateway.PipelineModeRealtime,
		RealtimeProvider: fakeRealtimeFactory{provider},
		RealtimeConfig:   &coregateway.RealtimeConfig{Provider: "fake"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	created := g.createSession(testCallSID, "+15550000000", "+16695550100", "outbound")
	t.Cleanup(func() {
		if err := created.Close(); err != nil {
			t.Errorf("Close() error: %v", err)
		}
	})

	ws := dialMediaStream(t, g)
	startStream(t, ws)
	waitEvent(t, created, coregateway.EventSessionStarted)

	// Media can race the pipeline start; send until the provider hears it.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sendMedia(t, ws, []byte{0x7f, 0x7f, 0x7f, 0x7f})
		select {
		case <-provider.received:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("realtime provider never received caller audio")
}
