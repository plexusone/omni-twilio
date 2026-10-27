package callsystem

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	twilioclient "github.com/twilio/twilio-go/client"

	"github.com/plexusone/omnivoice-core/callsystem"
)

// captureTransport records the form body of each request and answers with a
// minimal Twilio call resource.
type captureTransport struct {
	forms []url.Values
}

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	c.forms = append(c.forms, form)
	return &http.Response{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"sid":"CA123","status":"queued"}`)),
		Request:    req,
	}, nil
}

func newTestProvider(t *testing.T) (*Provider, *captureTransport) {
	t.Helper()
	p, err := New(
		WithAccountSID("AC00000000000000000000000000000000"),
		WithAuthToken("testtoken"),
		WithPhoneNumber("+15550000000"),
		WithWebhookURL("wss://example.com/media-stream"),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	tr := &captureTransport{}
	base, ok := p.client.RestClient().Client.(*twilioclient.Client)
	if !ok {
		t.Fatalf("unexpected Twilio base client type %T", p.client.RestClient().Client)
	}
	base.HTTPClient = &http.Client{Transport: tr}
	return p, tr
}

func TestMakeCallSendDigitsPlaysBeforeStream(t *testing.T) {
	p, tr := newTestProvider(t)

	_, err := p.MakeCall(context.Background(), "+15551234567", callsystem.WithSendDigits("W85012345678#"))
	if err != nil {
		t.Fatalf("MakeCall() error: %v", err)
	}
	if len(tr.forms) != 1 {
		t.Fatalf("requests = %d, want 1", len(tr.forms))
	}

	twiml := tr.forms[0].Get("Twiml")
	play := strings.Index(twiml, `<Play digits="ww85012345678#"/>`)
	connect := strings.Index(twiml, "<Connect>")
	if play < 0 {
		t.Fatalf("TwiML has no <Play digits> for the keypad digits:\n%s", twiml)
	}
	if connect < 0 || play > connect {
		t.Fatalf("<Play digits> must come before <Connect>:\n%s", twiml)
	}
	if got := tr.forms[0].Get("SendDigits"); got != "" {
		t.Fatalf("SendDigits parameter = %q, want unset (digits are played in TwiML)", got)
	}
}

func TestMakeCallWithoutDigitsHasNoPlay(t *testing.T) {
	p, tr := newTestProvider(t)

	if _, err := p.MakeCall(context.Background(), "+15551234567"); err != nil {
		t.Fatalf("MakeCall() error: %v", err)
	}
	if twiml := tr.forms[0].Get("Twiml"); strings.Contains(twiml, "<Play") {
		t.Fatalf("TwiML has <Play> without send digits:\n%s", twiml)
	}
}

func TestMakeCallRejectsInvalidDigits(t *testing.T) {
	p, tr := newTestProvider(t)

	_, err := p.MakeCall(context.Background(), "+15551234567", callsystem.WithSendDigits(`1"/><Hangup/>`))
	if err == nil {
		t.Fatal("MakeCall() with invalid digits returned nil error")
	}
	if len(tr.forms) != 0 {
		t.Fatalf("a call was placed despite invalid digits")
	}
}

func TestTwimlDigits(t *testing.T) {
	tests := map[string]string{
		"":              "",
		"123#":          "123#",
		"W1#":           "ww1#",
		"WWw85012#W1#":  "wwwww85012#ww1#",
		"0123456789*#A": "0123456789*#A",
	}
	for in, want := range tests {
		if got := twimlDigits(in); got != want {
			t.Errorf("twimlDigits(%q) = %q, want %q", in, got, want)
		}
	}
}
