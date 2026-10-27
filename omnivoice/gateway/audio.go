package gateway

import (
	"context"

	"github.com/plexusone/omnivoice-core/audio/format"
	coregateway "github.com/plexusone/omnivoice-core/gateway"
)

// audioRecvBuffer holds about 10 seconds of 8 kHz mu-law audio in 20 ms
// chunks, so a briefly slow reader does not lose audio.
const audioRecvBuffer = 500

// audioSession is the session returned in PipelineModeAudio. Wrapping
// *Session keeps the AudioSession type assertion false in other modes.
type audioSession struct {
	*Session
}

var _ coregateway.AudioSession = audioSession{}

// AudioFormat returns Twilio's Media Streams format: mu-law, 8 kHz, mono.
func (a audioSession) AudioFormat() format.AudioFormat {
	return format.Twilio
}

// ReceiveAudio returns the caller's audio. It is closed when the call ends.
func (a audioSession) ReceiveAudio() <-chan []byte {
	return a.audioRecv
}

// SendAudio plays mu-law audio into the call.
func (a audioSession) SendAudio(audio []byte) error {
	conn, err := a.mediaConn()
	if err != nil {
		return err
	}
	return conn.sendAudio(audio)
}

// ClearAudio discards audio that Twilio has queued but not played yet.
func (a audioSession) ClearAudio() error {
	conn, err := a.mediaConn()
	if err != nil {
		return err
	}
	return conn.clear()
}

// mediaConn returns the session's media stream connection.
func (s *Session) mediaConn() (*mediaStreamConn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, coregateway.ErrSessionClosed
	}
	if s.conn == nil {
		return nil, coregateway.ErrAudioNotConnected
	}
	return s.conn, nil
}

// publicSession returns the session as callers see it: wrapped as an
// AudioSession in PipelineModeAudio, unwrapped otherwise.
func (g *Gateway) publicSession(s *Session) coregateway.Session {
	if g.config.Mode == coregateway.PipelineModeAudio {
		return audioSession{s}
	}
	return s
}

// runAudioPassthrough hands the media stream's audio to the application
// until the stream or session ends, then closes the session.
func (s *Session) runAudioPassthrough(ctx context.Context, conn *mediaStreamConn) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.audioForwarding = true
	s.mu.Unlock()

	// The forwarder owns audioRecv from here on and closes it on exit.
	defer close(s.audioRecv)

	// Let the read loop deliver audio; it drops audio until this is closed.
	close(conn.pipelineReady)
	s.emitEvent(EventSessionStarted, nil)

	defer func() {
		s.emitEvent(EventSessionEnded, nil)
		if err := s.Close(); err != nil {
			s.logger.Error("close session", "error", err)
		}
	}()

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
			select {
			case s.audioRecv <- audio:
			case <-ctx.Done():
				return
			case <-s.done:
				return
			}
		}
	}
}
