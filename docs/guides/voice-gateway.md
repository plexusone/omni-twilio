# Voice Gateway Guide

The voice gateway enables full-duplex phone conversations with AI. It supports two pipeline modes optimized for different use cases.

## Pipeline Modes

| Mode | Latency | Best For |
|------|---------|----------|
| **Text** | 500-1000ms | Custom voices, domain-specific STT, tool calling |
| **Realtime** | 100-200ms | Natural conversation, low latency requirements |
| **Audio** | None added | Recording, transcription, agents that choose when to speak |

### Text Mode (STT → LLM → TTS)

Traditional pipeline that chains speech-to-text, language model, and text-to-speech:

```
Audio In → STT → Text → LLM → Text → TTS → Audio Out
```

**Advantages:**

- Mix and match providers (Deepgram STT + Claude + ElevenLabs TTS)
- Use custom/cloned voices
- Full tool/function calling support
- Domain-specific STT models

**Trade-offs:**

- Higher latency (500-1000ms round-trip)
- Multiple API calls per turn

### Realtime Mode (Voice-to-Voice)

Native voice-to-voice using OpenAI Realtime API or Gemini Live:

```
Audio In → Realtime API → Audio Out
```

**Advantages:**

- Ultra-low latency (~100-200ms)
- Natural conversation flow
- Single API connection

**Trade-offs:**

- Limited to OpenAI or Google voices
- Provider-specific tool calling

### Audio Mode (Raw Call Audio)

No pipeline: the gateway hands the call's audio to your application and plays the audio your application sends.

```
Audio In → your application → Audio Out
```

Set `Mode: coregateway.PipelineModeAudio` (from `github.com/plexusone/omnivoice-core/gateway`). Sessions then implement `coregateway.AudioSession`:

```go
sess, err := gw.MakeCallWithOptions(ctx, "+16695550100")
if err != nil {
    return err
}
audio := sess.(coregateway.AudioSession)

go func() {
    for chunk := range audio.ReceiveAudio() { // mu-law, 8 kHz, mono
        recorder.Write(chunk)
    }
}()

// Speak only when your application decides to.
if err := audio.SendAudio(mulawSpeech); err != nil {
    return err
}
```

`ReceiveAudio` is closed and the session ends when the media stream stops. `SendAudio` returns `ErrAudioNotConnected` before the stream connects and `ErrSessionClosed` after the call ends; `ClearAudio` stops audio that has not played yet. See omnivoice-core's [Raw Call Audio](https://plexusone.github.io/omnivoice-core/guides/call-audio/) guide.

**Use it for:**

- Agents attending meetings by phone, which listen to everyone but speak only when addressed
- Call recording and live transcription
- Custom pipelines

---

## Quick Start

### Text Mode

```go
package main

import (
    "context"
    "os"

    "github.com/plexusone/omni-twilio/omnivoice/gateway"
)

func main() {
    gw, err := gateway.New(gateway.Config{
        // Twilio credentials
        AccountSID:  os.Getenv("TWILIO_ACCOUNT_SID"),
        AuthToken:   os.Getenv("TWILIO_AUTH_TOKEN"),
        PhoneNumber: "+15551234567",

        // Server
        PublicURL:  "https://your-server.com",
        ListenAddr: ":8080",

        // STT provider
        STTProvider: "deepgram",
        STTAPIKey:   os.Getenv("DEEPGRAM_API_KEY"),

        // LLM provider
        LLMProvider:     "anthropic",
        LLMAPIKey:       os.Getenv("ANTHROPIC_API_KEY"),
        LLMModel:        "claude-sonnet-4-20250514",
        LLMSystemPrompt: "You are a helpful voice assistant. Keep responses brief.",

        // TTS provider
        TTSProvider: "elevenlabs",
        TTSAPIKey:   os.Getenv("ELEVENLABS_API_KEY"),
        TTSVoiceID:  "21m00Tcm4TlvDq8ikWAM", // Rachel
    })
    if err != nil {
        panic(err)
    }

    // Handle incoming calls
    gw.OnCall(func(call *gateway.CallInfo) error {
        return nil // Accept all calls
    })

    // Start gateway
    ctx := context.Background()
    gw.Start(ctx)
}
```

### Realtime Mode

```go
package main

import (
    "context"
    "os"

    "github.com/plexusone/omni-twilio/omnivoice/gateway"
    coregateway "github.com/plexusone/omnivoice-core/gateway"
    openaiRealtime "github.com/plexusone/omni-openai/omnivoice/realtime"
)

func main() {
    gw, err := gateway.New(gateway.Config{
        // Twilio credentials
        AccountSID:  os.Getenv("TWILIO_ACCOUNT_SID"),
        AuthToken:   os.Getenv("TWILIO_AUTH_TOKEN"),
        PhoneNumber: "+15551234567",

        // Server
        PublicURL:  "https://your-server.com",
        ListenAddr: ":8080",

        // Enable realtime mode
        Mode:             coregateway.PipelineModeRealtime,
        RealtimeProvider: openaiRealtime.NewFactory(),
        RealtimeConfig: &coregateway.RealtimeConfig{
            Provider:     "openai",
            APIKey:       os.Getenv("OPENAI_API_KEY"),
            Model:        "gpt-4o-realtime-preview-2024-12-17",
            Voice:        "alloy",
            Instructions: "You are a helpful voice assistant. Keep responses brief.",
        },
    })
    if err != nil {
        panic(err)
    }

    gw.OnCall(func(call *gateway.CallInfo) error {
        return nil
    })

    ctx := context.Background()
    gw.Start(ctx)
}
```

---

## Provider Configuration

### Text Mode Providers

#### STT Providers

| Provider | Environment Variable | Model |
|----------|---------------------|-------|
| Deepgram | `DEEPGRAM_API_KEY` | nova-2 (default) |
| OpenAI | `OPENAI_API_KEY` | whisper-1 |
| ElevenLabs | `ELEVENLABS_API_KEY` | scribe |

```go
// Deepgram (recommended for real-time)
STTProvider: "deepgram",
STTAPIKey:   os.Getenv("DEEPGRAM_API_KEY"),
STTModel:    "nova-2",
STTLanguage: "en",

// OpenAI Whisper
STTProvider: "openai",
STTAPIKey:   os.Getenv("OPENAI_API_KEY"),
```

#### TTS Providers

| Provider | Environment Variable | Voices |
|----------|---------------------|--------|
| ElevenLabs | `ELEVENLABS_API_KEY` | 100+ voices, cloning |
| OpenAI | `OPENAI_API_KEY` | alloy, echo, fable, onyx, nova, shimmer |
| Deepgram | `DEEPGRAM_API_KEY` | aura voices |

```go
// ElevenLabs (recommended for quality)
TTSProvider: "elevenlabs",
TTSAPIKey:   os.Getenv("ELEVENLABS_API_KEY"),
TTSVoiceID:  "21m00Tcm4TlvDq8ikWAM", // Rachel

// OpenAI
TTSProvider: "openai",
TTSAPIKey:   os.Getenv("OPENAI_API_KEY"),
TTSVoiceID:  "alloy",
```

#### LLM Providers

| Provider | Environment Variable | Models |
|----------|---------------------|--------|
| Anthropic | `ANTHROPIC_API_KEY` | claude-sonnet-4, claude-opus-4 |
| OpenAI | `OPENAI_API_KEY` | gpt-4o, gpt-4-turbo |

```go
// Anthropic Claude
LLMProvider:     "anthropic",
LLMAPIKey:       os.Getenv("ANTHROPIC_API_KEY"),
LLMModel:        "claude-sonnet-4-20250514",
LLMSystemPrompt: "You are a helpful assistant...",

// OpenAI
LLMProvider:     "openai",
LLMAPIKey:       os.Getenv("OPENAI_API_KEY"),
LLMModel:        "gpt-4o",
```

#### LLM Provider Injection (Advanced)

By default, the gateway uses **thin providers** from omnillm-core (native HTTP implementations). For applications that need **thick providers** (official SDKs with additional features), you can inject a pre-configured LLM client.

**Thin vs Thick Providers:**

| Type | Package | Features |
|------|---------|----------|
| **Thin** | `omnillm-core` | Basic chat completion, minimal dependencies |
| **Thick** | `omnillm` | Official SDKs, streaming, better error handling, full API support |

**Injecting a Thick Provider:**

```go
import (
    "github.com/plexusone/omni-twilio/omnivoice/gateway"
    "github.com/plexusone/omnillm"
)

// Create omnillm client (imports thick providers automatically)
llmClient, err := omnillm.NewClient(omnillm.ClientConfig{
    Providers: []omnillm.ProviderConfig{
        {
            Provider: omnillm.ProviderNameAnthropic,
            APIKey:   os.Getenv("ANTHROPIC_API_KEY"),
        },
    },
})
if err != nil {
    panic(err)
}

// Inject the provider into gateway config
gw, err := gateway.New(gateway.Config{
    // ... Twilio config ...

    // Inject thick provider (LLMProvider/LLMAPIKey are ignored when LLMClient is set)
    LLMClient:       llmClient.Provider(),
    LLMModel:        "claude-sonnet-4-20250514",
    LLMSystemPrompt: "You are a helpful voice assistant.",
})
```

**When to use thick providers:**

- You need streaming responses
- You want official SDK error handling and retries
- You need features like conversation memory or caching from omnillm
- You're building a larger application that already uses omnillm

### Realtime Providers

#### OpenAI Realtime (~100ms latency)

```go
import openaiRealtime "github.com/plexusone/omni-openai/omnivoice/realtime"

Mode:             coregateway.PipelineModeRealtime,
RealtimeProvider: openaiRealtime.NewFactory(),
RealtimeConfig: &coregateway.RealtimeConfig{
    Provider:     "openai",
    APIKey:       os.Getenv("OPENAI_API_KEY"),
    Model:        "gpt-4o-realtime-preview-2024-12-17",
    Voice:        "alloy", // alloy, echo, fable, onyx, nova, shimmer
    Instructions: "You are a helpful assistant.",
},
```

#### Gemini Live (~200ms latency)

```go
import googleRealtime "github.com/plexusone/omni-google/omnivoice/realtime"

Mode:             coregateway.PipelineModeRealtime,
RealtimeProvider: googleRealtime.NewFactory(),
RealtimeConfig: &coregateway.RealtimeConfig{
    Provider:     "gemini",
    APIKey:       os.Getenv("GOOGLE_API_KEY"),
    Model:        "gemini-2.0-flash-live",
    Voice:        "Puck", // Puck, Charon, Kore, Fenrir, Aoede
    Instructions: "You are a helpful assistant.",
},
```

---

## Session Management

### Handling Calls

```go
// Accept/reject incoming calls
gw.OnCall(func(call *gateway.CallInfo) error {
    log.Printf("Call from %s to %s", call.From, call.To)

    // Return nil to accept, error to reject
    if isBlocked(call.From) {
        return fmt.Errorf("blocked caller")
    }
    return nil
})
```

### Session Events

```go
session, ok := gw.GetSession(callSID)
if !ok {
    return
}

for event := range session.Events() {
    switch event.Type {
    case gateway.EventSessionStarted:
        log.Println("Call connected")

    case gateway.EventUserTranscript:
        log.Printf("User: %s", event.Data)

    case gateway.EventAgentTranscript:
        log.Printf("Agent: %s", event.Data)

    case gateway.EventToolCall:
        log.Printf("Tool called: %v", event.Data)

    case gateway.EventInterruption:
        log.Println("User interrupted")

    case gateway.EventSessionEnded:
        log.Println("Call ended")
        return

    case gateway.EventError:
        log.Printf("Error: %v", event.Error)
    }
}
```

### Session Metrics

```go
metrics := session.Metrics()
log.Printf("Duration: %s", metrics.Duration)
log.Printf("User turns: %d", metrics.UserTurnCount)
log.Printf("Agent turns: %d", metrics.AgentTurnCount)
log.Printf("Interruptions: %d", metrics.InterruptionCount)
```

---

## Outbound Calls and Keypad Digits

`MakeCall` places an outbound call and returns its session. To send keypad tones (DTMF) once the call is answered, for example to join a meeting's phone dial-in line, pass `callsystem.WithSendDigits` to `MakeCallWithOptions`:

```go
import "github.com/plexusone/omnivoice-core/callsystem"

// Wait 4 seconds, enter the meeting ID and #, wait 8 seconds, then enter
// * and the passcode and #.
session, err := gw.MakeCallWithOptions(ctx, "+16695550100",
    callsystem.WithSendDigits("WWWW85012345678#WWWWWWWW*482913#"),
)
if err != nil {
    return err
}
```

Digits use dial-string notation: `0`-`9`, `*`, `#`, and `A`-`D` are tones, `w` waits half a second, and `W` waits one second. The gateway plays them with `<Play digits>` before connecting the media stream, because a bidirectional media stream cannot send DTMF, so the agent starts hearing the call once the digits are entered. Invalid digits return an error before the call is placed.

The gateway implements omnivoice-core's `gateway.OptionCaller`, so code written against the provider-neutral interface can use `gateway.MakeCallWithOptions(ctx, gw, to, opts...)` from `github.com/plexusone/omnivoice-core/gateway`. See omnivoice-core's [Outbound Calls and Keypad Digits](https://plexusone.github.io/omnivoice-core/guides/outbound-calls/) guide for the notation and provider support.

## Tool Calling (Text Mode)

Define tools for the LLM to call during conversation:

```go
gw, err := gateway.New(gateway.Config{
    // ... other config ...

    Tools: []gateway.ToolDefinition{
        {
            Name:        "get_weather",
            Description: "Get current weather for a location",
            Parameters: map[string]any{
                "type": "object",
                "properties": map[string]any{
                    "location": map[string]any{
                        "type":        "string",
                        "description": "City name",
                    },
                },
                "required": []string{"location"},
            },
        },
        {
            Name:        "schedule_appointment",
            Description: "Schedule an appointment",
            Parameters: map[string]any{
                "type": "object",
                "properties": map[string]any{
                    "date": map[string]any{"type": "string"},
                    "time": map[string]any{"type": "string"},
                },
                "required": []string{"date", "time"},
            },
        },
    },

    ToolHandlers: map[string]gateway.ToolHandler{
        "get_weather": func(ctx context.Context, args map[string]any) (string, error) {
            location := args["location"].(string)
            // Call weather API
            return fmt.Sprintf("It's 72°F and sunny in %s", location), nil
        },
        "schedule_appointment": func(ctx context.Context, args map[string]any) (string, error) {
            // Schedule the appointment
            return "Appointment scheduled successfully", nil
        },
    },
})
```

---

## Greeting Message

Play a greeting when the call connects:

```go
gw, err := gateway.New(gateway.Config{
    // ... other config ...

    Greeting: "Hello! Thank you for calling. How can I help you today?",
})
```

---

## Session Limits

Configure session behavior:

```go
gw, err := gateway.New(gateway.Config{
    // ... other config ...

    // Maximum call duration (default: 30 minutes)
    MaxSessionDuration: 15 * time.Minute,

    // Interruption handling
    // "immediate" - Stop speaking immediately when user talks
    // "after_sentence" - Finish current sentence
    // "disabled" - Don't allow interruptions
    InterruptionMode: "immediate",
})
```

---

## Choosing a Pipeline Mode

### Use Text Mode When:

- You need custom or cloned voices (ElevenLabs)
- You require domain-specific STT (medical, legal terminology)
- You need complex tool calling workflows
- Latency of 500-1000ms is acceptable
- You want to mix best-of-breed providers

### Use Realtime Mode When:

- Low latency is critical (<300ms)
- Natural conversation flow is important
- OpenAI or Google voices are acceptable
- You want simpler architecture (single API)

---

## Environment Variables

### Text Mode

```bash
# Twilio
export TWILIO_ACCOUNT_SID="ACxxxxxxxx"
export TWILIO_AUTH_TOKEN="your-token"

# STT (choose one)
export DEEPGRAM_API_KEY="your-key"
export OPENAI_API_KEY="your-key"

# LLM (choose one)
export ANTHROPIC_API_KEY="your-key"
export OPENAI_API_KEY="your-key"

# TTS (choose one)
export ELEVENLABS_API_KEY="your-key"
export OPENAI_API_KEY="your-key"
```

### Realtime Mode

```bash
# Twilio
export TWILIO_ACCOUNT_SID="ACxxxxxxxx"
export TWILIO_AUTH_TOKEN="your-token"

# Realtime provider (choose one)
export OPENAI_API_KEY="your-key"
export GOOGLE_API_KEY="your-key"
```

---

## Registry Pattern

Instead of direct construction with `gateway.New()`, you can use omnivoice-core's provider registry for automatic discovery. This is useful when building applications that work with multiple gateway providers.

```go
import (
    omnivoice "github.com/plexusone/omnivoice-core"
    "github.com/plexusone/omnivoice-core/registry"
    _ "github.com/plexusone/omni-twilio/omnivoice/gateway" // Auto-register
)

// Get gateway via registry
gw, err := omnivoice.GetGatewayProvider("twilio",
    registry.WithAPIKey(os.Getenv("TWILIO_AUTH_TOKEN")),
    registry.WithExtension("accountSID", os.Getenv("TWILIO_ACCOUNT_SID")),
    registry.WithExtension("phoneNumber", "+15551234567"),
    registry.WithExtension("publicURL", "https://your-server.com"),
    registry.WithExtension("listenAddr", ":8080"),
)
```

### Type-Safe Options

For provider-specific configuration, use the type-safe option functions:

```go
import "github.com/plexusone/omni-twilio/omnivoice/gateway"

gw, err := omnivoice.GetGatewayProvider("twilio",
    registry.WithAPIKey(os.Getenv("TWILIO_AUTH_TOKEN")),
    // Type-safe options
    gateway.WithTools([]gateway.ToolDefinition{...}),
    gateway.WithToolHandlers(handlers),
    gateway.WithLLMClient(myLLMProvider),
    gateway.WithRealtimeProviderFactory(openaiRealtime.NewFactory()),
    gateway.WithRealtimeConfig(&coregateway.RealtimeConfig{...}),
)
```

### Accessing the Underlying Gateway

The registry returns a wrapper type. To access the full Twilio Gateway API:

```go
wrapper := gw.(*gateway.GatewayWrapper)
twilioGw := wrapper.Gateway()

// Now use Twilio-specific methods
twilioGw.OnCall(func(call *gateway.CallInfo) error {
    return nil
})
```

---

## Next Steps

- [Twilio Console Setup](twilio-console.md) - Configure webhooks
- [v0.8.0 Release Notes](../releases/v0.8.0.md) - Registry integration details
