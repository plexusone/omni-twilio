// Package gateway provides an HTTP/WebSocket gateway for handling Twilio voice calls
// with full-duplex bidirectional audio using Twilio Media Streams.
//
// Architecture:
//
//	┌──────────┐        ┌─────────────────┐        ┌───────────────────┐
//	│  Caller  │◄──────►│     Twilio      │◄──────►│   OmniVoice       │
//	│  (PSTN)  │  PSTN  │  Media Streams  │   WS   │   Voice Gateway   │
//	└──────────┘        └─────────────────┘        └───────────────────┘
//
// Flow:
//  1. Caller dials your Twilio phone number
//  2. Twilio webhook hits /voice/inbound
//  3. Server returns TwiML connecting to Media Streams
//  4. Twilio opens WebSocket to /ws/media-stream
//  5. Gateway receives audio, processes with STT → LLM → TTS
//  6. Gateway sends audio back through the same WebSocket
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/plexusone/omnillm-core/provider"
	corecallsystem "github.com/plexusone/omnivoice-core/callsystem"
	coregateway "github.com/plexusone/omnivoice-core/gateway"

	"github.com/plexusone/omni-twilio/client"
	"github.com/plexusone/omni-twilio/omnivoice/callsystem"
	"github.com/plexusone/omni-twilio/omnivoice/transport"
)

// Verify interface compliance at compile time.
var (
	_ coregateway.Gateway      = (*Gateway)(nil)
	_ coregateway.OptionCaller = (*Gateway)(nil)
)

// Config configures the voice gateway.
type Config struct {
	// Twilio credentials
	AccountSID  string
	AuthToken   string
	PhoneNumber string

	// Server configuration
	ListenAddr string       // e.g., ":8080"
	PublicURL  string       // e.g., "https://your-server.com"
	Listener   net.Listener // Optional external listener (e.g., ngrok)

	// Pipeline mode: "text" (STT→LLM→TTS) or "realtime" (voice-to-voice)
	Mode coregateway.PipelineMode

	// Voice pipeline configuration (used when Mode == "text")
	STTProvider string // e.g., "deepgram", "whisper"
	STTAPIKey   string
	STTModel    string
	STTLanguage string

	TTSProvider string // e.g., "elevenlabs", "openai"
	TTSAPIKey   string
	TTSVoiceID  string
	TTSModel    string

	LLMProvider     string // e.g., "anthropic", "openai" (used with omnillm-core registry)
	LLMAPIKey       string
	LLMModel        string // e.g., "claude-sonnet-4-20250514"
	LLMSystemPrompt string

	// LLMClient is an optional pre-configured LLM provider.
	// If provided, LLMProvider/LLMAPIKey are ignored and this client is used directly.
	// This allows injecting thick providers (official SDKs) from the application.
	LLMClient provider.Provider

	// Realtime pipeline configuration (used when Mode == "realtime")
	// Provide either RealtimeProvider directly or RealtimeConfig to create one.
	RealtimeProvider coregateway.RealtimeProviderFactory
	RealtimeConfig   *coregateway.RealtimeConfig

	// Tools available to the LLM
	Tools        []ToolDefinition
	ToolHandlers map[string]ToolHandler

	// Greeting is the initial message spoken when a call connects.
	Greeting string

	// Session configuration
	MaxSessionDuration time.Duration
	InterruptionMode   string // "immediate", "after_sentence", "disabled"

	// Logging
	Logger *slog.Logger
}

// Gateway handles Twilio voice calls with full-duplex audio.
type Gateway struct {
	config      Config
	client      *client.Client
	callSystem  *callsystem.Provider
	transport   *transport.Provider
	logger      *slog.Logger
	upgrader    websocket.Upgrader
	callHandler CallHandler

	mu       sync.RWMutex
	sessions map[string]*Session
	server   *http.Server
}

// CallHandler is an alias for the core gateway CallHandler type.
type CallHandler = coregateway.CallHandler

// CallInfo is an alias for the core gateway CallInfo type.
type CallInfo = coregateway.CallInfo

// New creates a new voice gateway.
func New(cfg Config) (*Gateway, error) {
	if cfg.AccountSID == "" {
		return nil, fmt.Errorf("AccountSID is required")
	}
	if cfg.AuthToken == "" {
		return nil, fmt.Errorf("AuthToken is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxSessionDuration == 0 {
		cfg.MaxSessionDuration = 30 * time.Minute
	}
	if cfg.InterruptionMode == "" {
		cfg.InterruptionMode = "immediate"
	}

	// Create Twilio client
	twilioClient, err := client.New(&client.Config{
		AccountSID: cfg.AccountSID,
		AuthToken:  cfg.AuthToken,
	})
	if err != nil {
		return nil, fmt.Errorf("create twilio client: %w", err)
	}

	// Create call system
	cs, err := callsystem.New(
		callsystem.WithAccountSID(cfg.AccountSID),
		callsystem.WithAuthToken(cfg.AuthToken),
		callsystem.WithPhoneNumber(cfg.PhoneNumber),
		callsystem.WithWebhookURL(cfg.PublicURL+"/ws/media-stream"),
	)
	if err != nil {
		return nil, fmt.Errorf("create call system: %w", err)
	}

	return &Gateway{
		config:     cfg,
		client:     twilioClient,
		callSystem: cs,
		transport:  cs.Transport(),
		logger:     cfg.Logger,
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		sessions: make(map[string]*Session),
	}, nil
}

// Name returns the provider name.
func (g *Gateway) Name() coregateway.ProviderName {
	return coregateway.ProviderTwilio
}

// OnCall sets the handler for incoming calls.
func (g *Gateway) OnCall(handler CallHandler) {
	g.mu.Lock()
	g.callHandler = handler
	g.mu.Unlock()
}

// Start starts the gateway server.
func (g *Gateway) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Twilio webhook handlers
	mux.HandleFunc("/voice/inbound", g.handleInboundCall)
	mux.HandleFunc("/voice/status", g.handleCallStatus)

	// WebSocket handler for Media Streams
	mux.HandleFunc("/ws/media-stream", g.handleMediaStream)

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	g.server = &http.Server{
		Addr:              g.config.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	g.logger.Info("starting voice gateway",
		"addr", g.config.ListenAddr,
		"public_url", g.config.PublicURL,
		"external_listener", g.config.Listener != nil)

	errCh := make(chan error, 1)
	go func() {
		var err error
		if g.config.Listener != nil {
			// Use external listener (e.g., ngrok)
			err = g.server.Serve(g.config.Listener)
		} else {
			// Create our own listener
			err = g.server.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		return g.Stop()
	case err := <-errCh:
		return err
	}
}

// Stop gracefully shuts down the gateway.
func (g *Gateway) Stop() error {
	g.logger.Info("stopping voice gateway")

	// Close all active sessions
	g.mu.Lock()
	for _, session := range g.sessions {
		_ = session.Close()
	}
	g.sessions = make(map[string]*Session)
	g.mu.Unlock()

	// Shutdown server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if g.server != nil {
		return g.server.Shutdown(ctx)
	}
	return nil
}

// MakeCall initiates an outbound call.
func (g *Gateway) MakeCall(ctx context.Context, to string) (coregateway.Session, error) {
	return g.MakeCallWithOptions(ctx, to)
}

// MakeCallWithOptions initiates an outbound call with call options, such as
// keypad digits to play once the call is answered (callsystem.WithSendDigits).
func (g *Gateway) MakeCallWithOptions(ctx context.Context, to string, opts ...corecallsystem.CallOption) (coregateway.Session, error) {
	call, err := g.callSystem.MakeCall(ctx, to, opts...)
	if err != nil {
		return nil, fmt.Errorf("make call: %w", err)
	}

	session := g.createSession(call.ID(), call.From(), to, "outbound")
	return session, nil
}

// GetSession retrieves an active session by call SID.
func (g *Gateway) GetSession(callSID string) (coregateway.Session, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	session, ok := g.sessions[callSID]
	if !ok {
		return nil, false
	}
	return session, true
}

// getSessionInternal retrieves the concrete session type (for internal use).
func (g *Gateway) getSessionInternal(callSID string) (*Session, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	session, ok := g.sessions[callSID]
	return session, ok
}

// ListSessions returns all active sessions.
func (g *Gateway) ListSessions() []coregateway.Session {
	g.mu.RLock()
	defer g.mu.RUnlock()

	sessions := make([]coregateway.Session, 0, len(g.sessions))
	for _, s := range g.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

// createSession creates a new voice session.
func (g *Gateway) createSession(callSID, from, to, direction string) *Session {
	session := &Session{
		id:        callSID,
		gateway:   g,
		from:      from,
		to:        to,
		direction: direction,
		startTime: time.Now(),
		events:    make(chan coregateway.Event, 100),
		done:      make(chan struct{}),
		logger:    g.logger.With("call_sid", callSID),
	}

	g.mu.Lock()
	g.sessions[callSID] = session
	g.mu.Unlock()

	return session
}

// removeSession removes a session from the gateway.
func (g *Gateway) removeSession(callSID string) {
	g.mu.Lock()
	delete(g.sessions, callSID)
	g.mu.Unlock()
}
