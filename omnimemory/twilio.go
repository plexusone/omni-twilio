// Package omnimemory provides a Twilio Memory provider for omnimemory.
//
// This package implements the omnimemory Provider interface using the Twilio
// Memory API as the backend. It maps omnimemory concepts to Twilio Memory:
//
//   - TenantID → Twilio Store ID
//   - SubjectID → Twilio Profile ID
//   - Memory → Twilio Observation
//   - Search/Recall → Twilio Recall API
//
// # Usage
//
// Import this package to register the Twilio provider:
//
//	import (
//	    "github.com/plexusone/omnimemory"
//	    "github.com/plexusone/omnimemory/core"
//	    _ "github.com/plexusone/omni-twilio/omnimemory" // Register Twilio provider
//	)
//
//	func main() {
//	    client, err := omnimemory.NewClient(core.ClientConfig{
//	        Providers: []core.ProviderConfig{
//	            {
//	                Name: core.ProviderNameTwilio,
//	                Options: map[string]any{
//	                    "account_sid": "ACxxx",
//	                    "auth_token":  "xxx",
//	                },
//	            },
//	        },
//	    })
//	    // ...
//	}
//
// # Configuration
//
// The provider accepts the following options:
//
//   - account_sid: Twilio Account SID (required, or use TWILIO_ACCOUNT_SID env)
//   - auth_token: Twilio Auth Token (required, or use TWILIO_AUTH_TOKEN env)
//
// # Mapping
//
// Twilio Memory API uses a hierarchical structure:
//
//   - Store: A container for profiles and observations (maps to TenantID)
//   - Profile: A user identity that owns observations (maps to SubjectID)
//   - Observation: A piece of memory content (maps to Memory)
//
// The TenantID in omnimemory should be set to a valid Twilio Store ID.
// The SubjectID should be set to a valid Twilio Profile ID.
package omnimemory

import (
	"context"
	"os"
	"time"

	"github.com/plexusone/omnimemory/core"
	"github.com/twilio/twilio-go"
	memoryv1 "github.com/twilio/twilio-go/rest/memory/v1"
)

func init() {
	core.RegisterProvider(core.ProviderNameTwilio, NewProvider, core.PriorityThick)
}

// Provider implements core.Provider using Twilio Memory API.
type Provider struct {
	client   *twilio.RestClient
	memory   *memoryv1.ApiService
	embedder core.Embedder
	config   core.ProviderConfig
}

// NewProvider creates a new Twilio Memory Provider.
func NewProvider(config core.ProviderConfig, embedder core.Embedder) (core.Provider, error) {
	accountSid := getOption(config.Options, "account_sid", os.Getenv("TWILIO_ACCOUNT_SID"))
	authToken := getOption(config.Options, "auth_token", os.Getenv("TWILIO_AUTH_TOKEN"))

	if accountSid == "" {
		return nil, core.NewValidationError("account_sid", "Twilio Account SID is required")
	}
	if authToken == "" {
		return nil, core.NewValidationError("auth_token", "Twilio Auth Token is required")
	}

	client := twilio.NewRestClientWithParams(twilio.ClientParams{
		Username: accountSid,
		Password: authToken,
	})

	return &Provider{
		client:   client,
		memory:   client.MemoryV1,
		embedder: embedder,
		config:   config,
	}, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return core.ProviderNameTwilio.String()
}

// Close closes the provider.
func (p *Provider) Close() error {
	// Twilio client doesn't need explicit closing
	return nil
}

// Add adds a new memory (observation) to Twilio Memory.
func (p *Provider) Add(ctx context.Context, req *core.AddRequest) (*core.Memory, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	storeID := req.TenantID
	profileID := req.SubjectID

	// Build observation
	now := time.Now()
	source := "omnimemory"
	if req.AgentID != "" {
		source = req.AgentID
	}

	observation := memoryv1.ObservationCreateRequest{
		Content:    req.Content,
		OccurredAt: now,
		Source:     source,
	}

	// Add conversation ID if present
	if req.ConversationID != "" {
		observation.ConversationIds = []string{req.ConversationID}
	}

	params := &memoryv1.CreateProfileObservationParams{
		CreateObservationsRequest: &memoryv1.CreateObservationsRequest{
			Observations: []memoryv1.ObservationCreateRequest{observation},
		},
	}

	_, err := p.memory.CreateProfileObservation(storeID, profileID, params)
	if err != nil {
		return nil, core.NewProviderError(p.Name(), "Add", err)
	}

	// Twilio doesn't return the observation ID on create, so we construct a partial memory
	// For full details, caller would need to list observations
	memory := &core.Memory{
		TenantID:  storeID,
		SubjectID: profileID,
		AgentID:   req.AgentID,
		Scope:     req.Scope,
		Type:      req.Type,
		Content:   req.Content,
		Metadata:  req.Metadata,
		CreatedAt: now,
		UpdatedAt: now,
	}

	return memory, nil
}

// Get retrieves a memory (observation) by ID.
func (p *Provider) Get(ctx context.Context, req *core.GetRequest) (*core.Memory, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	storeID := req.TenantID
	profileID := req.SubjectID

	obs, err := p.memory.FetchProfileObservation(storeID, profileID, req.ID)
	if err != nil {
		return nil, core.NewProviderError(p.Name(), "Get", err)
	}

	return observationInfoToMemory(storeID, profileID, obs), nil
}

// Update updates an existing memory.
// Note: Twilio Memory API doesn't support updating observations directly.
// This implementation deletes and recreates the observation.
func (p *Provider) Update(ctx context.Context, req *core.UpdateRequest) (*core.Memory, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// Get existing observation first
	existing, err := p.Get(ctx, &core.GetRequest{
		Context: req.Context,
		ID:      req.ID,
	})
	if err != nil {
		return nil, err
	}

	// Delete the old observation
	if err := p.Delete(ctx, &core.DeleteRequest{
		Context: req.Context,
		ID:      req.ID,
	}); err != nil {
		return nil, err
	}

	// Create new observation with updated content
	content := existing.Content
	if req.Content != "" {
		content = req.Content
	}

	metadata := existing.Metadata
	if req.Metadata != nil {
		if metadata == nil {
			metadata = make(map[string]any)
		}
		for k, v := range req.Metadata {
			metadata[k] = v
		}
	}

	return p.Add(ctx, &core.AddRequest{
		Context:  req.Context,
		Type:     existing.Type,
		Content:  content,
		Metadata: metadata,
	})
}

// Delete deletes a memory (observation) by ID.
func (p *Provider) Delete(ctx context.Context, req *core.DeleteRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}

	storeID := req.TenantID
	profileID := req.SubjectID

	_, err := p.memory.DeleteProfileObservation(storeID, profileID, req.ID)
	if err != nil {
		return core.NewProviderError(p.Name(), "Delete", err)
	}

	return nil
}

// List lists memories (observations) with optional filters.
func (p *Provider) List(ctx context.Context, req *core.ListRequest) (*core.ListResponse, error) {
	storeID := req.TenantID
	profileID := req.SubjectID

	if storeID == "" {
		return nil, core.ErrTenantRequired
	}
	if profileID == "" {
		return nil, core.ErrSubjectRequired
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}

	params := &memoryv1.ListProfileObservationsParams{}
	params.SetLimit(limit)
	params.SetOrderBy("DESC") // Most recent first

	observations, err := p.memory.ListProfileObservations(storeID, profileID, params)
	if err != nil {
		return nil, core.NewProviderError(p.Name(), "List", err)
	}

	memories := make([]*core.Memory, len(observations))
	for i := range observations {
		memories[i] = observationInfoToMemory(storeID, profileID, &observations[i])
	}

	return &core.ListResponse{
		Memories:   memories,
		TotalCount: len(memories),
		HasMore:    len(memories) == limit,
	}, nil
}

// Search performs semantic search using Twilio Recall API.
func (p *Provider) Search(ctx context.Context, req *core.SearchRequest) (*core.SearchResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	storeID := req.TenantID
	profileID := req.SubjectID

	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}

	memReq := memoryv1.MemoryRetrievalRequest{
		Query:             req.Query,
		ObservationsLimit: limit,
	}

	// Apply threshold if specified
	if req.Threshold > 0 {
		memReq.RelevanceThreshold = req.Threshold
	}

	params := &memoryv1.CreateFetchProfileMemoryParams{
		MemoryRetrievalRequest: &memReq,
	}

	resp, err := p.memory.CreateFetchProfileMemory(storeID, profileID, params)
	if err != nil {
		return nil, core.NewProviderError(p.Name(), "Search", err)
	}

	var results []*core.SearchResult

	// Process observations
	for i := range resp.Observations {
		obs := &resp.Observations[i]
		result := &core.SearchResult{
			Memory: recallObservationToMemory(storeID, profileID, obs),
			Score:  obs.Score,
		}
		results = append(results, result)
	}

	return &core.SearchResponse{
		Results: results,
	}, nil
}

// Recall retrieves relevant memories using Twilio Recall API.
func (p *Provider) Recall(ctx context.Context, req *core.RecallRequest) (*core.RecallResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	storeID := req.TenantID
	profileID := req.SubjectID

	maxResults := req.MaxResults
	if maxResults <= 0 {
		maxResults = 20
	}

	memReq := memoryv1.MemoryRetrievalRequest{
		Query:             req.Query,
		ObservationsLimit: maxResults,
		SummariesLimit:    5,
	}

	params := &memoryv1.CreateFetchProfileMemoryParams{
		MemoryRetrievalRequest: &memReq,
	}

	resp, err := p.memory.CreateFetchProfileMemory(storeID, profileID, params)
	if err != nil {
		return nil, core.NewProviderError(p.Name(), "Recall", err)
	}

	var memories []*core.Memory
	var summary string

	// Process observations
	for i := range resp.Observations {
		memories = append(memories, recallObservationToMemory(storeID, profileID, &resp.Observations[i]))
	}

	// Combine summaries if available
	if len(resp.Summaries) > 0 {
		if len(resp.Summaries) == 1 {
			summary = resp.Summaries[0].Content
		} else {
			for i, s := range resp.Summaries {
				if i > 0 {
					summary += "\n\n"
				}
				summary += s.Content
			}
		}
	}

	return &core.RecallResponse{
		Memories: memories,
		Summary:  summary,
	}, nil
}

// observationInfoToMemory converts a Twilio ObservationInfo to core.Memory.
func observationInfoToMemory(storeID, profileID string, obs *memoryv1.ObservationInfo) *core.Memory {
	m := &core.Memory{
		ID:        obs.Id,
		TenantID:  storeID,
		SubjectID: profileID,
		Type:      core.MemoryTypeObservation,
		Content:   obs.Content,
		AgentID:   obs.Source,
		CreatedAt: obs.CreatedAt,
		UpdatedAt: obs.UpdatedAt,
	}

	if !obs.OccurredAt.IsZero() {
		m.Metadata = map[string]any{
			"occurred_at": obs.OccurredAt,
		}
	}

	return m
}

// recallObservationToMemory converts a Twilio RecallObservationInfo to core.Memory.
func recallObservationToMemory(storeID, profileID string, obs *memoryv1.RecallObservationInfo) *core.Memory {
	m := &core.Memory{
		ID:        obs.Id,
		TenantID:  storeID,
		SubjectID: profileID,
		Type:      core.MemoryTypeObservation,
		Content:   obs.Content,
		AgentID:   obs.Source,
		CreatedAt: obs.CreatedAt,
		UpdatedAt: obs.UpdatedAt,
	}

	if !obs.OccurredAt.IsZero() {
		m.Metadata = map[string]any{
			"occurred_at": obs.OccurredAt,
		}
	}

	return m
}

// getOption retrieves an option from the map with a default value.
func getOption(options map[string]any, key, defaultValue string) string {
	if options == nil {
		return defaultValue
	}
	if v, ok := options[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultValue
}
