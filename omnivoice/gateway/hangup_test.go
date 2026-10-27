package gateway

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	twilioclient "github.com/twilio/twilio-go/client"

	coregateway "github.com/plexusone/omnivoice-core/gateway"
)

// captureTwilio records Twilio REST requests made by the gateway's client.
type captureTwilio struct {
	mu       sync.Mutex
	requests []string
	forms    []url.Values
}

func (c *captureTwilio) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.requests = append(c.requests, req.Method+" "+req.URL.Path)
	c.forms = append(c.forms, form)
	c.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"sid":"` + testCallSID + `","status":"completed"}`)),
		Request:    req,
	}, nil
}

func TestHangupEndsCallAndClosesSession(t *testing.T) {
	g := newTestGateway(t, coregateway.PipelineModeAudio)
	capture := &captureTwilio{}
	base, ok := g.client.RestClient().Client.(*twilioclient.Client)
	if !ok {
		t.Fatalf("unexpected Twilio base client type %T", g.client.RestClient().Client)
	}
	base.HTTPClient = &http.Client{Transport: capture}

	g.createSession(testCallSID, "+15550000000", "+16695550100", "outbound")
	sess, ok := g.GetSession(testCallSID)
	if !ok {
		t.Fatal("session not found")
	}
	hanger, ok := sess.(interface{ Hangup(context.Context) error })
	if !ok {
		t.Fatal("audio mode session does not implement Hangup")
	}
	if err := hanger.Hangup(context.Background()); err != nil {
		t.Fatalf("Hangup() error: %v", err)
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.requests) != 1 ||
		!strings.HasSuffix(capture.requests[0], "/Calls/"+testCallSID+".json") ||
		capture.forms[0].Get("Status") != "completed" {
		t.Fatalf("requests = %v forms = %v, want one call update to completed", capture.requests, capture.forms)
	}
	if _, ok := g.GetSession(testCallSID); ok {
		t.Fatal("session still registered after Hangup")
	}
}
