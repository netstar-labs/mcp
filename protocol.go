package mcp

import (
	"encoding/base64"
	"encoding/json"
)

// Protocol revisions this implementation understands. Negotiation echoes the
// client's requested version when it appears here, otherwise it answers with
// LatestProtocolVersion and lets the client decide whether to continue.
//
// Notable differences between revisions:
//   - 2024-11-05: baseline.
//   - 2025-03-26: adds audio content, tool annotations, completions capability,
//     progress messages, and JSON-RPC batching.
//   - 2025-06-18: adds structured tool output, output schemas, resource links,
//     elicitation, and _removes_ JSON-RPC batching again.
const (
	ProtocolVersion20241105 = "2024-11-05"
	ProtocolVersion20250326 = "2025-03-26"
	ProtocolVersion20250618 = "2025-06-18"

	LatestProtocolVersion = ProtocolVersion20250618
)

// DefaultProtocolVersions is the negotiation set, newest first.
var DefaultProtocolVersions = []string{
	ProtocolVersion20250618,
	ProtocolVersion20250326,
	ProtocolVersion20241105,
}

// Method names.
const (
	MethodInitialize           = "initialize"
	MethodPing                 = "ping"
	MethodToolsList            = "tools/list"
	MethodToolsCall            = "tools/call"
	MethodResourcesList        = "resources/list"
	MethodResourcesTemplates   = "resources/templates/list"
	MethodResourcesRead        = "resources/read"
	MethodResourcesSubscribe   = "resources/subscribe"
	MethodResourcesUnsubscribe = "resources/unsubscribe"
	MethodPromptsList          = "prompts/list"
	MethodPromptsGet           = "prompts/get"
	MethodLoggingSetLevel      = "logging/setLevel"
	MethodCompletionComplete   = "completion/complete"

	NotifyInitialized      = "notifications/initialized"
	NotifyCancelled        = "notifications/cancelled"
	NotifyProgress         = "notifications/progress"
	NotifyMessage          = "notifications/message"
	NotifyToolsChanged     = "notifications/tools/list_changed"
	NotifyResourcesChanged = "notifications/resources/list_changed"
	NotifyResourceUpdated  = "notifications/resources/updated"
	NotifyPromptsChanged   = "notifications/prompts/list_changed"
	NotifyRootsChanged     = "notifications/roots/list_changed"

	// Server-initiated requests.
	MethodSamplingCreateMessage = "sampling/createMessage"
	MethodRootsList             = "roots/list"
	MethodElicitationCreate     = "elicitation/create"
)

// Implementation identifies a party on the connection.
type Implementation struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type emptyCap struct{}

// ClientCapabilities is what the client advertises at initialize time.
type ClientCapabilities struct {
	Experimental map[string]json.RawMessage `json:"experimental,omitempty"`
	Roots        *RootsCapability           `json:"roots,omitempty"`
	Sampling     *emptyCap                  `json:"sampling,omitempty"`
	Elicitation  *emptyCap                  `json:"elicitation,omitempty"`
}

type RootsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// ServerCapabilities is derived from what has actually been registered on the
// Server; it is never set by hand.
type ServerCapabilities struct {
	Experimental map[string]json.RawMessage `json:"experimental,omitempty"`
	Logging      *emptyCap                  `json:"logging,omitempty"`
	Completions  *emptyCap                  `json:"completions,omitempty"`
	Prompts      *PromptsCapability         `json:"prompts,omitempty"`
	Resources    *ResourcesCapability       `json:"resources,omitempty"`
	Tools        *ToolsCapability           `json:"tools,omitempty"`
}

type PromptsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

type ResourcesCapability struct {
	Subscribe   bool `json:"subscribe,omitempty"`
	ListChanged bool `json:"listChanged,omitempty"`
}

type ToolsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

type InitializeParams struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ClientCapabilities `json:"capabilities"`
	ClientInfo      Implementation     `json:"clientInfo"`
}

type InitializeResult struct {
	ProtocolVersion string             `json:"protocolVersion"`
	Capabilities    ServerCapabilities `json:"capabilities"`
	ServerInfo      Implementation     `json:"serverInfo"`
	Instructions    string             `json:"instructions,omitempty"`
}

// PaginatedParams is embedded by every list request.
type PaginatedParams struct {
	Cursor string `json:"cursor,omitempty"`
}

// Annotations are optional hints attached to content and resources.
type Annotations struct {
	Audience     []string `json:"audience,omitempty"` // "user", "assistant"
	Priority     *float64 `json:"priority,omitempty"` // 0..1
	LastModified string   `json:"lastModified,omitempty"`
}

// Content block types.
const (
	ContentTypeText         = "text"
	ContentTypeImage        = "image"
	ContentTypeAudio        = "audio"
	ContentTypeResource     = "resource"      // embedded resource
	ContentTypeResourceLink = "resource_link" // 2025-06-18
)

// Content is the union of every content block shape. Fields are emitted only
// when populated, so a single struct covers all variants without forcing
// callers through an interface and a type switch on the read path.
type Content struct {
	Type        string            `json:"type"`
	Text        string            `json:"text,omitempty"`
	Data        string            `json:"data,omitempty"` // base64, image/audio
	MIMEType    string            `json:"mimeType,omitempty"`
	Resource    *ResourceContents `json:"resource,omitempty"`
	URI         string            `json:"uri,omitempty"`
	Name        string            `json:"name,omitempty"`
	Title       string            `json:"title,omitempty"`
	Description string            `json:"description,omitempty"`
	Annotations *Annotations      `json:"annotations,omitempty"`
	Meta        json.RawMessage   `json:"_meta,omitempty"`
}

func TextContent(s string) Content { return Content{Type: ContentTypeText, Text: s} }

func ImageContent(data []byte, mimeType string) Content {
	return Content{Type: ContentTypeImage, Data: base64.StdEncoding.EncodeToString(data), MIMEType: mimeType}
}

func AudioContent(data []byte, mimeType string) Content {
	return Content{Type: ContentTypeAudio, Data: base64.StdEncoding.EncodeToString(data), MIMEType: mimeType}
}

func EmbeddedResource(rc ResourceContents) Content {
	return Content{Type: ContentTypeResource, Resource: &rc}
}

func ResourceLink(uri, name, mimeType string) Content {
	return Content{Type: ContentTypeResourceLink, URI: uri, Name: name, MIMEType: mimeType}
}

// Tool describes a callable tool.
type Tool struct {
	Name         string           `json:"name"`
	Title        string           `json:"title,omitempty"`
	Description  string           `json:"description,omitempty"`
	InputSchema  json.RawMessage  `json:"inputSchema"`
	OutputSchema json.RawMessage  `json:"outputSchema,omitempty"`
	Annotations  *ToolAnnotations `json:"annotations,omitempty"`
	Meta         json.RawMessage  `json:"_meta,omitempty"`
}

// ToolAnnotations are advisory hints for the client. They are untrusted by
// definition: a client must not make a security decision on the basis of a
// server-supplied annotation.
type ToolAnnotations struct {
	Title           string `json:"title,omitempty"`
	ReadOnlyHint    *bool  `json:"readOnlyHint,omitempty"`
	DestructiveHint *bool  `json:"destructiveHint,omitempty"`
	IdempotentHint  *bool  `json:"idempotentHint,omitempty"`
	OpenWorldHint   *bool  `json:"openWorldHint,omitempty"`
}

type ListToolsResult struct {
	Tools      []Tool `json:"tools"`
	NextCursor string `json:"nextCursor,omitempty"`
}

type CallToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// CallToolResult carries the outcome of a tool invocation.
//
// IsError is not the same thing as a JSON-RPC error. A JSON-RPC error means the
// call could not be dispatched (unknown tool, malformed arguments); IsError
// means the tool ran and failed, and the failure text is intended to be fed
// back to the model so it can adapt.
type CallToolResult struct {
	Content           []Content       `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	IsError           bool            `json:"isError,omitempty"`
	Meta              json.RawMessage `json:"_meta,omitempty"`
}

// ToolError builds a failed-execution result.
func ToolError(format string, args ...any) *CallToolResult {
	return &CallToolResult{Content: []Content{TextContent(sprintf(format, args...))}, IsError: true}
}

// ToolText builds a successful single-text-block result.
func ToolText(format string, args ...any) *CallToolResult {
	return &CallToolResult{Content: []Content{TextContent(sprintf(format, args...))}}
}

// Resource is a static, directly addressable resource.
type Resource struct {
	URI         string          `json:"uri"`
	Name        string          `json:"name"`
	Title       string          `json:"title,omitempty"`
	Description string          `json:"description,omitempty"`
	MIMEType    string          `json:"mimeType,omitempty"`
	Size        *int64          `json:"size,omitempty"`
	Annotations *Annotations    `json:"annotations,omitempty"`
	Meta        json.RawMessage `json:"_meta,omitempty"`
}

// ResourceTemplate is a parameterised resource family (RFC 6570 URI template).
type ResourceTemplate struct {
	URITemplate string          `json:"uriTemplate"`
	Name        string          `json:"name"`
	Title       string          `json:"title,omitempty"`
	Description string          `json:"description,omitempty"`
	MIMEType    string          `json:"mimeType,omitempty"`
	Annotations *Annotations    `json:"annotations,omitempty"`
	Meta        json.RawMessage `json:"_meta,omitempty"`
}

// ResourceContents is one blob of resource data. Exactly one of Text or Blob
// should be set; Blob is base64.
type ResourceContents struct {
	URI      string `json:"uri"`
	Name     string `json:"name,omitempty"`
	Title    string `json:"title,omitempty"`
	MIMEType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"`
}

func TextResource(uri, mimeType, text string) ResourceContents {
	return ResourceContents{URI: uri, MIMEType: mimeType, Text: text}
}

func BlobResource(uri, mimeType string, data []byte) ResourceContents {
	return ResourceContents{URI: uri, MIMEType: mimeType, Blob: base64.StdEncoding.EncodeToString(data)}
}

type ListResourcesResult struct {
	Resources  []Resource `json:"resources"`
	NextCursor string     `json:"nextCursor,omitempty"`
}

type ListResourceTemplatesResult struct {
	ResourceTemplates []ResourceTemplate `json:"resourceTemplates"`
	NextCursor        string             `json:"nextCursor,omitempty"`
}

type ReadResourceParams struct {
	URI string `json:"uri"`
}

type ReadResourceResult struct {
	Contents []ResourceContents `json:"contents"`
}

type SubscribeParams struct {
	URI string `json:"uri"`
}

// Prompt is a reusable prompt template.
type Prompt struct {
	Name        string           `json:"name"`
	Title       string           `json:"title,omitempty"`
	Description string           `json:"description,omitempty"`
	Arguments   []PromptArgument `json:"arguments,omitempty"`
	Meta        json.RawMessage  `json:"_meta,omitempty"`
}

type PromptArgument struct {
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

type ListPromptsResult struct {
	Prompts    []Prompt `json:"prompts"`
	NextCursor string   `json:"nextCursor,omitempty"`
}

type GetPromptParams struct {
	Name      string            `json:"name"`
	Arguments map[string]string `json:"arguments,omitempty"`
}

type PromptMessage struct {
	Role    string  `json:"role"` // "user" | "assistant"
	Content Content `json:"content"`
}

type GetPromptResult struct {
	Description string          `json:"description,omitempty"`
	Messages    []PromptMessage `json:"messages"`
}

// Logging levels, ordered by RFC 5424 severity.
const (
	LogDebug     = "debug"
	LogInfo      = "info"
	LogNotice    = "notice"
	LogWarning   = "warning"
	LogError     = "error"
	LogCritical  = "critical"
	LogAlert     = "alert"
	LogEmergency = "emergency"
)

var logSeverity = map[string]int{
	LogDebug: 0, LogInfo: 1, LogNotice: 2, LogWarning: 3,
	LogError: 4, LogCritical: 5, LogAlert: 6, LogEmergency: 7,
}

type SetLevelParams struct {
	Level string `json:"level"`
}

type LoggingMessageParams struct {
	Level  string          `json:"level"`
	Logger string          `json:"logger,omitempty"`
	Data   json.RawMessage `json:"data"`
}

type ProgressParams struct {
	ProgressToken json.RawMessage `json:"progressToken"`
	Progress      float64         `json:"progress"`
	Total         *float64        `json:"total,omitempty"`
	Message       string          `json:"message,omitempty"`
}

type CancelledParams struct {
	RequestID json.RawMessage `json:"requestId"`
	Reason    string          `json:"reason,omitempty"`
}

type ResourceUpdatedParams struct {
	URI string `json:"uri"`
}

// Completion support (completion/complete).
type CompletionReference struct {
	Type string `json:"type"` // "ref/prompt" | "ref/resource"
	Name string `json:"name,omitempty"`
	URI  string `json:"uri,omitempty"`
}

type CompleteParams struct {
	Ref      CompletionReference `json:"ref"`
	Argument struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"argument"`
	Context *struct {
		Arguments map[string]string `json:"arguments,omitempty"`
	} `json:"context,omitempty"`
}

type CompleteResult struct {
	Completion struct {
		Values  []string `json:"values"`
		Total   *int     `json:"total,omitempty"`
		HasMore bool     `json:"hasMore,omitempty"`
	} `json:"completion"`
}

// Server-initiated request payloads.

type Root struct {
	URI  string `json:"uri"`
	Name string `json:"name,omitempty"`
}

type ListRootsResult struct {
	Roots []Root `json:"roots"`
}

type SamplingMessage struct {
	Role    string  `json:"role"`
	Content Content `json:"content"`
}

type ModelHint struct {
	Name string `json:"name,omitempty"`
}

type ModelPreferences struct {
	Hints                []ModelHint `json:"hints,omitempty"`
	CostPriority         *float64    `json:"costPriority,omitempty"`
	SpeedPriority        *float64    `json:"speedPriority,omitempty"`
	IntelligencePriority *float64    `json:"intelligencePriority,omitempty"`
}

type CreateMessageParams struct {
	Messages         []SamplingMessage `json:"messages"`
	ModelPreferences *ModelPreferences `json:"modelPreferences,omitempty"`
	SystemPrompt     string            `json:"systemPrompt,omitempty"`
	IncludeContext   string            `json:"includeContext,omitempty"` // none|thisServer|allServers
	Temperature      *float64          `json:"temperature,omitempty"`
	MaxTokens        int               `json:"maxTokens"`
	StopSequences    []string          `json:"stopSequences,omitempty"`
	Metadata         json.RawMessage   `json:"metadata,omitempty"`
}

type CreateMessageResult struct {
	Role       string  `json:"role"`
	Content    Content `json:"content"`
	Model      string  `json:"model"`
	StopReason string  `json:"stopReason,omitempty"`
}

type ElicitParams struct {
	Message         string          `json:"message"`
	RequestedSchema json.RawMessage `json:"requestedSchema"`
}

type ElicitResult struct {
	Action  string          `json:"action"` // accept|decline|cancel
	Content json.RawMessage `json:"content,omitempty"`
}
