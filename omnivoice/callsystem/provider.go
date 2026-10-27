// Package callsystem provides a Twilio implementation of callsystem.CallSystem.
package callsystem

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/plexusone/omnivoice-core/agent"
	"github.com/plexusone/omnivoice-core/callsystem"
	omnitransport "github.com/plexusone/omnivoice-core/transport"

	"github.com/plexusone/omni-twilio/client"
	"github.com/plexusone/omni-twilio/omnivoice/transport"
)

// Verify interface compliance at compile time.
var (
	_ callsystem.CallSystem  = (*Provider)(nil)
	_ callsystem.SMSProvider = (*Provider)(nil)
)

// Provider implements callsystem.CallSystem using Twilio.
type Provider struct {
	client      *client.Client
	config      callsystem.CallSystemConfig
	handler     callsystem.CallHandler
	transport   *transport.Provider
	defaultFrom string

	mu    sync.RWMutex
	calls map[string]*Call
}

// Option configures the Provider.
type Option func(*options)

type options struct {
	accountSID  string
	authToken   string
	phoneNumber string
	webhookURL  string
}

// WithAccountSID sets the Twilio Account SID.
func WithAccountSID(sid string) Option {
	return func(o *options) {
		o.accountSID = sid
	}
}

// WithAuthToken sets the Twilio Auth Token.
func WithAuthToken(token string) Option {
	return func(o *options) {
		o.authToken = token
	}
}

// WithPhoneNumber sets the default outbound phone number.
func WithPhoneNumber(number string) Option {
	return func(o *options) {
		o.phoneNumber = number
	}
}

// WithWebhookURL sets the webhook URL for incoming calls.
func WithWebhookURL(url string) Option {
	return func(o *options) {
		o.webhookURL = url
	}
}

// New creates a new Twilio CallSystem provider.
func New(opts ...Option) (*Provider, error) {
	cfg := &options{}
	for _, opt := range opts {
		opt(cfg)
	}

	twilioClient, err := client.New(&client.Config{
		AccountSID: cfg.accountSID,
		AuthToken:  cfg.authToken,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Twilio client: %w", err)
	}

	// Create transport provider for Media Streams
	tr, err := transport.New(
		transport.WithAccountSID(cfg.accountSID),
		transport.WithAuthToken(cfg.authToken),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	return &Provider{
		client:      twilioClient,
		transport:   tr,
		defaultFrom: cfg.phoneNumber,
		calls:       make(map[string]*Call),
		config: callsystem.CallSystemConfig{
			AccountSID:  cfg.accountSID,
			AuthToken:   cfg.authToken,
			PhoneNumber: cfg.phoneNumber,
			WebhookURL:  cfg.webhookURL,
		},
	}, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return "twilio"
}

// Configure configures the call system.
func (p *Provider) Configure(config callsystem.CallSystemConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.config = config
	if config.PhoneNumber != "" {
		p.defaultFrom = config.PhoneNumber
	}

	return nil
}

// OnIncomingCall sets the handler for incoming calls.
func (p *Provider) OnIncomingCall(handler callsystem.CallHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = handler
}

// MakeCall initiates an outbound call.
func (p *Provider) MakeCall(ctx context.Context, to string, opts ...callsystem.CallOption) (callsystem.Call, error) {
	// Apply options using the exported CallOptions type
	callOpts := &callsystem.CallOptions{}
	for _, opt := range opts {
		opt(callOpts)
	}

	if err := callsystem.ValidateSendDigits(callOpts.SendDigits); err != nil {
		return nil, err
	}

	from := callOpts.From
	if from == "" {
		from = p.defaultFrom
	}
	if from == "" {
		return nil, fmt.Errorf("from number is required (use WithFrom or set default phone number)")
	}

	// Build TwiML for Media Streams, playing any keypad digits first
	twiml := buildOutboundTwiML(p.config.WebhookURL, callOpts.SendDigits)

	params := &client.MakeCallParams{
		To:    to,
		From:  from,
		Twiml: twiml,
	}

	if callOpts.StatusCallback != "" {
		params.StatusCallback = callOpts.StatusCallback
		params.StatusCallbackEvent = []string{"initiated", "ringing", "answered", "completed"}
	}

	if callOpts.Timeout > 0 {
		params.Timeout = int(callOpts.Timeout.Seconds())
	}

	if callOpts.MachineDetect {
		params.MachineDetection = "Enable"
	}

	if callOpts.Record {
		params.Record = true
		params.RecordingChannels = "dual"
	}

	twilioCall, err := p.client.MakeCall(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to make call: %w", err)
	}

	call := &Call{
		id:        twilioCall.SID,
		direction: callsystem.Outbound,
		status:    mapCallStatus(twilioCall.Status),
		from:      from,
		to:        to,
		startTime: time.Now(),
		provider:  p,
	}

	p.mu.Lock()
	p.calls[call.id] = call
	p.mu.Unlock()

	return call, nil
}

// GetCall retrieves a call by ID.
func (p *Provider) GetCall(ctx context.Context, callID string) (callsystem.Call, error) {
	// Check local cache first
	p.mu.RLock()
	if call, ok := p.calls[callID]; ok {
		p.mu.RUnlock()
		return call, nil
	}
	p.mu.RUnlock()

	// Fetch from Twilio
	twilioCall, err := p.client.GetCall(ctx, callID)
	if err != nil {
		return nil, fmt.Errorf("failed to get call: %w", err)
	}

	call := &Call{
		id:        twilioCall.SID,
		direction: mapDirection(twilioCall.Direction),
		status:    mapCallStatus(twilioCall.Status),
		from:      twilioCall.From,
		to:        twilioCall.To,
		provider:  p,
	}

	return call, nil
}

// ListCalls lists active calls.
func (p *Provider) ListCalls(ctx context.Context) ([]callsystem.Call, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	calls := make([]callsystem.Call, 0, len(p.calls))
	for _, call := range p.calls {
		calls = append(calls, call)
	}
	return calls, nil
}

// Close shuts down the call system.
func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Hangup all active calls
	ctx := context.Background()
	for _, call := range p.calls {
		_ = call.Hangup(ctx)
	}

	p.calls = make(map[string]*Call)

	if p.transport != nil {
		return p.transport.Close()
	}

	return nil
}

// HandleIncomingWebhook processes a Twilio incoming call webhook.
// This should be called from your HTTP handler.
func (p *Provider) HandleIncomingWebhook(callSID, from, to string) (callsystem.Call, string, error) {
	call := &Call{
		id:        callSID,
		direction: callsystem.Inbound,
		status:    callsystem.StatusRinging,
		from:      from,
		to:        to,
		startTime: time.Now(),
		provider:  p,
	}

	p.mu.Lock()
	p.calls[callSID] = call
	handler := p.handler
	p.mu.Unlock()

	// Call the handler
	if handler != nil {
		if err := handler(call); err != nil {
			return nil, "", err
		}
	}

	// Return TwiML for Media Streams
	twiml := buildMediaStreamTwiML(p.config.WebhookURL)
	return call, twiml, nil
}

// HandleStatusCallback processes a Twilio status callback webhook.
func (p *Provider) HandleStatusCallback(callSID, status string) {
	p.mu.Lock()
	call, ok := p.calls[callSID]
	if ok {
		call.status = mapCallStatus(status)
		if call.status == callsystem.StatusEnded {
			delete(p.calls, callSID)
		}
	}
	p.mu.Unlock()
}

// Transport returns the transport provider for Media Streams.
func (p *Provider) Transport() *transport.Provider {
	return p.transport
}

// SendSMS sends an SMS message using the default phone number.
func (p *Provider) SendSMS(ctx context.Context, to, body string) (*callsystem.SMSMessage, error) {
	return p.SendSMSFrom(ctx, to, p.defaultFrom, body)
}

// SendSMSFrom sends an SMS message from a specific phone number.
func (p *Provider) SendSMSFrom(ctx context.Context, to, from, body string) (*callsystem.SMSMessage, error) {
	if from == "" {
		from = p.defaultFrom
	}
	if from == "" {
		return nil, fmt.Errorf("from number is required")
	}

	msg, err := p.client.SendSMS(ctx, &client.SendSMSParams{
		To:   to,
		From: from,
		Body: body,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send SMS: %w", err)
	}

	return &callsystem.SMSMessage{
		ID:     msg.SID,
		To:     msg.To,
		From:   msg.From,
		Body:   msg.Body,
		Status: msg.Status,
	}, nil
}

// Call implements callsystem.Call for Twilio calls.
type Call struct {
	id        string
	direction callsystem.CallDirection
	status    callsystem.CallStatus
	from      string
	to        string
	startTime time.Time
	provider  *Provider

	mu        sync.RWMutex
	transport omnitransport.Connection
	agent     agent.Session
}

// ID returns the call identifier.
func (c *Call) ID() string {
	return c.id
}

// Direction returns inbound or outbound.
func (c *Call) Direction() callsystem.CallDirection {
	return c.direction
}

// Status returns the current call status.
func (c *Call) Status() callsystem.CallStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// From returns the caller ID.
func (c *Call) From() string {
	return c.from
}

// To returns the called number.
func (c *Call) To() string {
	return c.to
}

// StartTime returns when the call started.
func (c *Call) StartTime() time.Time {
	return c.startTime
}

// Duration returns the call duration.
func (c *Call) Duration() time.Duration {
	return time.Since(c.startTime)
}

// Answer answers an inbound call.
func (c *Call) Answer(ctx context.Context) error {
	c.mu.Lock()
	c.status = callsystem.StatusAnswered
	c.mu.Unlock()
	return nil
}

// Hangup ends the call.
func (c *Call) Hangup(ctx context.Context) error {
	_, err := c.provider.client.HangupCall(ctx, c.id)
	if err != nil {
		return fmt.Errorf("failed to hangup: %w", err)
	}

	c.mu.Lock()
	c.status = callsystem.StatusEnded
	if c.transport != nil {
		_ = c.transport.Close()
	}
	c.mu.Unlock()

	return nil
}

// Transport returns the underlying transport connection.
func (c *Call) Transport() omnitransport.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transport
}

// SetTransport sets the transport connection (called when Media Streams connects).
func (c *Call) SetTransport(conn omnitransport.Connection) {
	c.mu.Lock()
	c.transport = conn
	c.mu.Unlock()
}

// AttachAgent attaches a voice agent to handle the call.
func (c *Call) AttachAgent(ctx context.Context, session agent.Session) error {
	c.mu.Lock()
	c.agent = session
	c.mu.Unlock()

	// Start the agent session
	return session.Start(ctx)
}

// DetachAgent detaches the voice agent.
func (c *Call) DetachAgent(ctx context.Context) error {
	c.mu.Lock()
	session := c.agent
	c.agent = nil
	c.mu.Unlock()

	if session != nil {
		return session.Stop(ctx)
	}
	return nil
}

// buildMediaStreamTwiML creates TwiML for Media Streams.
func buildMediaStreamTwiML(webhookURL string) string {
	return buildOutboundTwiML(webhookURL, "")
}

// buildOutboundTwiML creates TwiML that plays sendDigits as keypad tones and
// then connects the call to Media Streams. The digits are played with
// <Play digits> before <Connect><Stream>, because a bidirectional media stream
// cannot send DTMF. sendDigits must already be validated with
// callsystem.ValidateSendDigits.
func buildOutboundTwiML(webhookURL, sendDigits string) string {
	streamURL := webhookURL
	if streamURL == "" {
		streamURL = "wss://your-server.com/media-stream"
	}

	play := ""
	if sendDigits != "" {
		play = fmt.Sprintf("\n    <Play digits=\"%s\"/>", twimlDigits(sendDigits))
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Response>%s
    <Connect>
        <Stream url="%s">
            <Parameter name="direction" value="both"/>
        </Stream>
    </Connect>
</Response>`, play, streamURL)
}

// twimlDigits converts dial-string digits to the <Play digits> form, which
// supports only w (a half-second pause): each W (one second) becomes ww.
func twimlDigits(digits string) string {
	return strings.ReplaceAll(digits, "W", "ww")
}

// mapCallStatus maps Twilio status to OmniVoice status.
func mapCallStatus(status string) callsystem.CallStatus {
	switch status {
	case "queued", "ringing":
		return callsystem.StatusRinging
	case "in-progress":
		return callsystem.StatusAnswered
	case "completed":
		return callsystem.StatusEnded
	case "busy":
		return callsystem.StatusBusy
	case "no-answer":
		return callsystem.StatusNoAnswer
	case "failed", "canceled":
		return callsystem.StatusFailed
	default:
		return callsystem.StatusRinging
	}
}

// mapDirection maps Twilio direction to OmniVoice direction.
func mapDirection(dir string) callsystem.CallDirection {
	if dir == "inbound" {
		return callsystem.Inbound
	}
	return callsystem.Outbound
}
