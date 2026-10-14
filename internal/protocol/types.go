package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

type QueryID uint32
type SessionID string
type IdentityVersion uint32
type QuerySetVersion uint32
type Timestamp uint64
type RequestSequenceNumber uint32

type StateVersion struct {
	QuerySet QuerySetVersion `json:"querySet"`
	Identity IdentityVersion `json:"identity"`
	TS       Timestamp       `json:"-"`
}

type stateVersionJSON struct {
	QuerySet QuerySetVersion `json:"querySet"`
	Identity IdentityVersion `json:"identity"`
	TS       string          `json:"ts"`
}

func (version StateVersion) MarshalJSON() ([]byte, error) {
	return json.Marshal(stateVersionJSON{
		QuerySet: version.QuerySet,
		Identity: version.Identity,
		TS:       EncodeTimestamp(version.TS.Uint64()),
	})
}

func (version *StateVersion) UnmarshalJSON(data []byte) error {
	var wire stateVersionJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	ts, err := DecodeTimestamp(wire.TS)
	if err != nil {
		return fmt.Errorf("invalid state version timestamp: %w", err)
	}
	version.QuerySet = wire.QuerySet
	version.Identity = wire.Identity
	version.TS = NewTimestamp(ts)
	return nil
}

type Query struct {
	QueryID       QueryID         `json:"queryId"`
	UDFPath       string          `json:"udfPath"`
	Args          json.RawMessage `json:"args,omitempty"`
	Journal       *string         `json:"journal,omitempty"`
	ComponentPath *string         `json:"componentPath,omitempty"`
}

type QuerySetModification struct {
	add    *Query
	remove *QueryID
}

func NewQuerySetAdd(query Query) QuerySetModification {
	copy := query
	return QuerySetModification{add: &copy}
}

func NewQuerySetRemove(queryID QueryID) QuerySetModification {
	copy := queryID
	return QuerySetModification{remove: &copy}
}

func (mod QuerySetModification) IsAdd() bool {
	return mod.add != nil
}

func (mod QuerySetModification) IsRemove() bool {
	return mod.remove != nil
}

func (mod QuerySetModification) QueryID() QueryID {
	if mod.add != nil {
		return mod.add.QueryID
	}
	if mod.remove != nil {
		return *mod.remove
	}
	return 0
}

func (mod QuerySetModification) Query() (Query, bool) {
	if mod.add == nil {
		return Query{}, false
	}
	return *mod.add, true
}

func (mod QuerySetModification) MarshalJSON() ([]byte, error) {
	if mod.add != nil && mod.remove != nil {
		return nil, fmt.Errorf("query set modification must be either add or remove")
	}
	if mod.add != nil {
		if mod.add.UDFPath == "" {
			return nil, fmt.Errorf("query set add modification requires udfPath")
		}
		return json.Marshal(struct {
			Type          string          `json:"type"`
			QueryID       QueryID         `json:"queryId"`
			UDFPath       string          `json:"udfPath"`
			Args          json.RawMessage `json:"args,omitempty"`
			Journal       *string         `json:"journal,omitempty"`
			ComponentPath *string         `json:"componentPath,omitempty"`
		}{
			Type:          "Add",
			QueryID:       mod.add.QueryID,
			UDFPath:       mod.add.UDFPath,
			Args:          mod.add.Args,
			Journal:       mod.add.Journal,
			ComponentPath: mod.add.ComponentPath,
		})
	}
	if mod.remove != nil {
		return json.Marshal(struct {
			Type    string  `json:"type"`
			QueryID QueryID `json:"queryId"`
		}{
			Type:    "Remove",
			QueryID: *mod.remove,
		})
	}
	return nil, fmt.Errorf("query set modification variant not set")
}

func (mod *QuerySetModification) UnmarshalJSON(data []byte) error {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}
	switch base.Type {
	case "Add":
		var payload struct {
			QueryID       QueryID         `json:"queryId"`
			UDFPath       string          `json:"udfPath"`
			Args          json.RawMessage `json:"args,omitempty"`
			Journal       *string         `json:"journal,omitempty"`
			ComponentPath *string         `json:"componentPath,omitempty"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.UDFPath == "" {
			return fmt.Errorf("query set add modification missing udfPath")
		}
		query := Query{
			QueryID:       payload.QueryID,
			UDFPath:       payload.UDFPath,
			Args:          payload.Args,
			Journal:       payload.Journal,
			ComponentPath: payload.ComponentPath,
		}
		mod.add = &query
		mod.remove = nil
		return nil
	case "Remove":
		var payload struct {
			QueryID *QueryID `json:"queryId"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.QueryID == nil {
			return fmt.Errorf("query set remove modification missing queryId")
		}
		copy := *payload.QueryID
		mod.remove = &copy
		mod.add = nil
		return nil
	default:
		return fmt.Errorf("unknown query set modification type %q", base.Type)
	}
}

type StateModification struct {
	updated *StateQueryUpdated
	failed  *StateQueryFailed
	removed *QueryID
}

type StateQueryUpdated struct {
	QueryID QueryID
	Value   json.RawMessage
	Journal *string
}

type StateQueryFailed struct {
	QueryID      QueryID
	ErrorMessage string
	ErrorData    json.RawMessage
	Journal      *string
}

func NewStateModificationQueryUpdated(queryID QueryID, value json.RawMessage, journal *string) StateModification {
	copy := StateQueryUpdated{QueryID: queryID, Value: value, Journal: journal}
	return StateModification{updated: &copy}
}

func NewStateModificationQueryFailed(queryID QueryID, message string, errorData json.RawMessage, journal *string) StateModification {
	copy := StateQueryFailed{QueryID: queryID, ErrorMessage: message, ErrorData: errorData, Journal: journal}
	return StateModification{failed: &copy}
}

func NewStateModificationQueryRemoved(queryID QueryID) StateModification {
	copy := queryID
	return StateModification{removed: &copy}
}

func (mod StateModification) Kind() string {
	if mod.updated != nil {
		return "QueryUpdated"
	}
	if mod.failed != nil {
		return "QueryFailed"
	}
	if mod.removed != nil {
		return "QueryRemoved"
	}
	return ""
}

func (mod StateModification) QueryUpdated() (StateQueryUpdated, bool) {
	if mod.updated == nil {
		return StateQueryUpdated{}, false
	}
	return *mod.updated, true
}

func (mod StateModification) QueryFailed() (StateQueryFailed, bool) {
	if mod.failed == nil {
		return StateQueryFailed{}, false
	}
	return *mod.failed, true
}

func (mod StateModification) QueryRemoved() (QueryID, bool) {
	if mod.removed == nil {
		return 0, false
	}
	return *mod.removed, true
}

func (mod StateModification) MarshalJSON() ([]byte, error) {
	variants := 0
	if mod.updated != nil {
		variants++
	}
	if mod.failed != nil {
		variants++
	}
	if mod.removed != nil {
		variants++
	}
	if variants != 1 {
		return nil, fmt.Errorf("state modification must contain exactly one variant")
	}

	if mod.updated != nil {
		if len(mod.updated.Value) == 0 {
			return nil, fmt.Errorf("query updated modification requires value")
		}
		return json.Marshal(struct {
			Type    string          `json:"type"`
			QueryID QueryID         `json:"queryId"`
			Value   json.RawMessage `json:"value"`
			Journal *string         `json:"journal,omitempty"`
		}{
			Type:    "QueryUpdated",
			QueryID: mod.updated.QueryID,
			Value:   mod.updated.Value,
			Journal: mod.updated.Journal,
		})
	}

	if mod.failed != nil {
		if mod.failed.ErrorMessage == "" {
			return nil, fmt.Errorf("query failed modification requires errorMessage")
		}
		return json.Marshal(struct {
			Type         string          `json:"type"`
			QueryID      QueryID         `json:"queryId"`
			ErrorMessage string          `json:"errorMessage"`
			ErrorData    json.RawMessage `json:"errorData,omitempty"`
			Journal      *string         `json:"journal,omitempty"`
		}{
			Type:         "QueryFailed",
			QueryID:      mod.failed.QueryID,
			ErrorMessage: mod.failed.ErrorMessage,
			ErrorData:    mod.failed.ErrorData,
			Journal:      mod.failed.Journal,
		})
	}

	return json.Marshal(struct {
		Type    string  `json:"type"`
		QueryID QueryID `json:"queryId"`
	}{
		Type:    "QueryRemoved",
		QueryID: *mod.removed,
	})
}

func (mod *StateModification) UnmarshalJSON(data []byte) error {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}

	switch base.Type {
	case "QueryUpdated":
		var payload struct {
			QueryID QueryID          `json:"queryId"`
			Value   *json.RawMessage `json:"value"`
			Journal *string          `json:"journal,omitempty"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.Value == nil {
			// The Convex server may send QueryUpdated with value=null when a
			// query function returns null, or when a stale transition arrives
			// for a recently-removed query (race between ModifyQuerySet Remove
			// and pending server-side transitions). Go's JSON decoder maps
			// both "value":null and absent "value" to *json.RawMessage=nil,
			// so we treat nil as JSON null — a valid Convex value.
			// (Supersedes the equivalent fix from fix/query-updated-null-value;
			// its regression test lives in internal/protocol/codec_test.go.)
			nullVal := json.RawMessage("null")
			payload.Value = &nullVal
		}
		mod.updated = &StateQueryUpdated{QueryID: payload.QueryID, Value: *payload.Value, Journal: payload.Journal}
		mod.failed = nil
		mod.removed = nil
		return nil
	case "QueryFailed":
		var payload struct {
			QueryID      QueryID         `json:"queryId"`
			ErrorMessage *string         `json:"errorMessage"`
			ErrorData    json.RawMessage `json:"errorData,omitempty"`
			Journal      *string         `json:"journal,omitempty"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.ErrorMessage == nil {
			return fmt.Errorf("query failed modification missing errorMessage")
		}
		mod.failed = &StateQueryFailed{QueryID: payload.QueryID, ErrorMessage: *payload.ErrorMessage, ErrorData: payload.ErrorData, Journal: payload.Journal}
		mod.updated = nil
		mod.removed = nil
		return nil
	case "QueryRemoved":
		var payload struct {
			QueryID *QueryID `json:"queryId"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.QueryID == nil {
			return fmt.Errorf("query removed modification missing queryId")
		}
		copy := *payload.QueryID
		mod.removed = &copy
		mod.updated = nil
		mod.failed = nil
		return nil
	default:
		return fmt.Errorf("unknown state modification type %q", base.Type)
	}
}

type AuthenticationToken struct {
	admin *AdminAuthenticationToken
	user  *string
	none  bool
}

type AdminAuthenticationToken struct {
	Value    string
	ActingAs json.RawMessage
}

func NewAdminAuthenticationToken(value string, actingAs json.RawMessage) AuthenticationToken {
	copy := AdminAuthenticationToken{Value: value, ActingAs: actingAs}
	return AuthenticationToken{admin: &copy}
}

func NewUserAuthenticationToken(value string) AuthenticationToken {
	copy := value
	return AuthenticationToken{user: &copy}
}

func NewNoAuthenticationToken() AuthenticationToken {
	return AuthenticationToken{none: true}
}

func (token AuthenticationToken) Kind() string {
	if token.admin != nil {
		return "Admin"
	}
	if token.user != nil {
		return "User"
	}
	if token.none {
		return "None"
	}
	return ""
}

func (token AuthenticationToken) Admin() (AdminAuthenticationToken, bool) {
	if token.admin == nil {
		return AdminAuthenticationToken{}, false
	}
	return *token.admin, true
}

func (token AuthenticationToken) User() (string, bool) {
	if token.user == nil {
		return "", false
	}
	return *token.user, true
}

func (token AuthenticationToken) MarshalJSON() ([]byte, error) {
	variants := 0
	if token.admin != nil {
		variants++
	}
	if token.user != nil {
		variants++
	}
	if token.none {
		variants++
	}
	if variants != 1 {
		return nil, fmt.Errorf("authentication token must contain exactly one variant")
	}

	if token.admin != nil {
		return json.Marshal(struct {
			TokenType string          `json:"tokenType"`
			Value     string          `json:"value"`
			ActingAs  json.RawMessage `json:"actingAs,omitempty"`
		}{
			TokenType: "Admin",
			Value:     token.admin.Value,
			ActingAs:  token.admin.ActingAs,
		})
	}

	if token.user != nil {
		return json.Marshal(struct {
			TokenType string `json:"tokenType"`
			Value     string `json:"value"`
		}{
			TokenType: "User",
			Value:     *token.user,
		})
	}

	return json.Marshal(struct {
		TokenType string `json:"tokenType"`
	}{
		TokenType: "None",
	})
}

func (token *AuthenticationToken) UnmarshalJSON(data []byte) error {
	var base struct {
		TokenType string `json:"tokenType"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}

	switch base.TokenType {
	case "Admin":
		var payload struct {
			Value        *string         `json:"value"`
			ActingAs     json.RawMessage `json:"actingAs,omitempty"`
			Impersonated json.RawMessage `json:"impersonating,omitempty"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.Value == nil {
			return fmt.Errorf("admin authentication token missing value")
		}
		actingAs := payload.ActingAs
		if len(actingAs) == 0 {
			actingAs = payload.Impersonated
		}
		token.admin = &AdminAuthenticationToken{Value: *payload.Value, ActingAs: actingAs}
		token.user = nil
		token.none = false
		return nil
	case "User":
		var payload struct {
			Value *string `json:"value"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.Value == nil {
			return fmt.Errorf("user authentication token missing value")
		}
		copy := *payload.Value
		token.user = &copy
		token.admin = nil
		token.none = false
		return nil
	case "None":
		token.none = true
		token.admin = nil
		token.user = nil
		return nil
	default:
		return fmt.Errorf("unknown authentication token type %q", base.TokenType)
	}
}

type ClientMessage struct {
	Type                 string                 `json:"type"`
	SessionID            SessionID              `json:"sessionId,omitempty"`
	ConnectionCount      uint32                 `json:"connectionCount,omitempty"`
	LastCloseReason      string                 `json:"lastCloseReason,omitempty"`
	MaxObservedTimestamp string                 `json:"maxObservedTimestamp,omitempty"`
	ClientTS             *int64                 `json:"clientTs,omitempty"`
	BaseVersion          uint32                 `json:"baseVersion,omitempty"`
	NewVersion           uint32                 `json:"newVersion,omitempty"`
	Modifications        []QuerySetModification `json:"modifications,omitempty"`
	RequestID            RequestSequenceNumber  `json:"requestId,omitempty"`
	UDFPath              string                 `json:"udfPath,omitempty"`
	Args                 json.RawMessage        `json:"args,omitempty"`
	ComponentPath        *string                `json:"componentPath,omitempty"`
	Token                AuthenticationToken    `json:"-"`
	EventType            string                 `json:"eventType,omitempty"`
	Event                json.RawMessage        `json:"event,omitempty"`
}

type clientMessageJSON struct {
	Type                 string                 `json:"type"`
	SessionID            string                 `json:"sessionId,omitempty"`
	ConnectionCount      *uint32                `json:"connectionCount,omitempty"`
	LastCloseReason      *string                `json:"lastCloseReason,omitempty"`
	MaxObservedTimestamp *string                `json:"maxObservedTimestamp,omitempty"`
	ClientTS             *int64                 `json:"clientTs,omitempty"`
	BaseVersion          *uint32                `json:"baseVersion,omitempty"`
	NewVersion           *uint32                `json:"newVersion,omitempty"`
	Modifications        []QuerySetModification `json:"modifications,omitempty"`
	RequestID            *RequestSequenceNumber `json:"requestId,omitempty"`
	UDFPath              string                 `json:"udfPath,omitempty"`
	Args                 json.RawMessage        `json:"args,omitempty"`
	ComponentPath        *string                `json:"componentPath,omitempty"`
	EventType            string                 `json:"eventType,omitempty"`
	Event                json.RawMessage        `json:"event,omitempty"`
}

func (msg ClientMessage) MarshalJSON() ([]byte, error) {
	switch msg.Type {
	case "Connect":
		if msg.SessionID == "" {
			return nil, fmt.Errorf("connect message missing sessionId")
		}
		if _, err := NewSessionID(msg.SessionID.String()); err != nil {
			return nil, err
		}
		lastCloseReason := msg.LastCloseReason
		if lastCloseReason == "" {
			lastCloseReason = "unknown"
		}
		payload := clientMessageJSON{
			Type:            "Connect",
			SessionID:       msg.SessionID.String(),
			ConnectionCount: &msg.ConnectionCount,
			LastCloseReason: &lastCloseReason,
			ClientTS:        msg.ClientTS,
		}
		if msg.MaxObservedTimestamp != "" {
			payload.MaxObservedTimestamp = &msg.MaxObservedTimestamp
		}
		return json.Marshal(payload)
	case "ModifyQuerySet":
		baseVersion := msg.BaseVersion
		newVersion := msg.NewVersion
		if msg.Modifications == nil {
			return nil, fmt.Errorf("modify query set message missing modifications")
		}
		return json.Marshal(clientMessageJSON{
			Type:          "ModifyQuerySet",
			BaseVersion:   &baseVersion,
			NewVersion:    &newVersion,
			Modifications: msg.Modifications,
		})
	case "Mutation", "Action":
		if msg.UDFPath == "" {
			return nil, fmt.Errorf("%s message missing udfPath", msg.Type)
		}
		if len(msg.Args) == 0 {
			return nil, fmt.Errorf("%s message missing args", msg.Type)
		}
		requestID := msg.RequestID
		return json.Marshal(clientMessageJSON{
			Type:          msg.Type,
			RequestID:     &requestID,
			UDFPath:       msg.UDFPath,
			Args:          msg.Args,
			ComponentPath: msg.ComponentPath,
		})
	case "Authenticate":
		if msg.Token.Kind() == "" {
			return nil, fmt.Errorf("authenticate message missing token")
		}
		tokenBytes, err := json.Marshal(msg.Token)
		if err != nil {
			return nil, err
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(tokenBytes, &payload); err != nil {
			return nil, err
		}
		baseVersion, err := json.Marshal(msg.BaseVersion)
		if err != nil {
			return nil, err
		}
		typeValue, err := json.Marshal("Authenticate")
		if err != nil {
			return nil, err
		}
		payload["type"] = typeValue
		payload["baseVersion"] = baseVersion
		return json.Marshal(payload)
	case "Event":
		if msg.EventType == "" {
			return nil, fmt.Errorf("event message missing eventType")
		}
		if len(msg.Event) == 0 {
			return nil, fmt.Errorf("event message missing event payload")
		}
		return json.Marshal(clientMessageJSON{
			Type:      "Event",
			EventType: msg.EventType,
			Event:     msg.Event,
		})
	default:
		return nil, fmt.Errorf("unknown client message type %q", msg.Type)
	}
}

func (msg *ClientMessage) UnmarshalJSON(data []byte) error {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}

	var payload clientMessageJSON
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	msg.Type = payload.Type

	switch base.Type {
	case "Connect":
		if payload.SessionID == "" {
			return fmt.Errorf("connect message missing sessionId")
		}
		sessionID, err := NewSessionID(payload.SessionID)
		if err != nil {
			return err
		}
		if payload.ConnectionCount == nil {
			return fmt.Errorf("connect message missing connectionCount")
		}
		lastCloseReason := "unknown"
		if payload.LastCloseReason != nil {
			lastCloseReason = *payload.LastCloseReason
		}
		maxObservedTimestamp := ""
		if payload.MaxObservedTimestamp != nil {
			if _, err := DecodeTimestamp(*payload.MaxObservedTimestamp); err != nil {
				return fmt.Errorf("connect message invalid maxObservedTimestamp: %w", err)
			}
			maxObservedTimestamp = *payload.MaxObservedTimestamp
		}
		msg.SessionID = sessionID
		msg.ConnectionCount = *payload.ConnectionCount
		msg.LastCloseReason = lastCloseReason
		msg.MaxObservedTimestamp = maxObservedTimestamp
		msg.ClientTS = payload.ClientTS
		return nil
	case "ModifyQuerySet":
		if payload.BaseVersion == nil {
			return fmt.Errorf("modify query set message missing baseVersion")
		}
		if payload.NewVersion == nil {
			return fmt.Errorf("modify query set message missing newVersion")
		}
		if payload.Modifications == nil {
			return fmt.Errorf("modify query set message missing modifications")
		}
		msg.BaseVersion = *payload.BaseVersion
		msg.NewVersion = *payload.NewVersion
		msg.Modifications = payload.Modifications
		return nil
	case "Mutation", "Action":
		if payload.RequestID == nil {
			return fmt.Errorf("%s message missing requestId", base.Type)
		}
		if payload.UDFPath == "" {
			return fmt.Errorf("%s message missing udfPath", base.Type)
		}
		if len(payload.Args) == 0 {
			return fmt.Errorf("%s message missing args", base.Type)
		}
		msg.RequestID = *payload.RequestID
		msg.UDFPath = payload.UDFPath
		msg.Args = payload.Args
		msg.ComponentPath = payload.ComponentPath
		return nil
	case "Authenticate":
		if payload.BaseVersion == nil {
			return fmt.Errorf("authenticate message missing baseVersion")
		}
		var token AuthenticationToken
		if err := json.Unmarshal(data, &token); err != nil {
			var legacy struct {
				Token    *string         `json:"token,omitempty"`
				Admin    bool            `json:"admin,omitempty"`
				ActingAs json.RawMessage `json:"actingAs,omitempty"`
			}
			if legacyErr := json.Unmarshal(data, &legacy); legacyErr != nil {
				return err
			}
			switch {
			case legacy.Token == nil:
				token = NewNoAuthenticationToken()
			case legacy.Admin:
				token = NewAdminAuthenticationToken(*legacy.Token, legacy.ActingAs)
			default:
				token = NewUserAuthenticationToken(*legacy.Token)
			}
		}
		msg.BaseVersion = *payload.BaseVersion
		msg.Token = token
		return nil
	case "Event":
		if payload.EventType == "" {
			return fmt.Errorf("event message missing eventType")
		}
		if len(payload.Event) == 0 {
			return fmt.Errorf("event message missing event payload")
		}
		msg.EventType = payload.EventType
		msg.Event = payload.Event
		return nil
	default:
		return fmt.Errorf("unknown client message type %q", base.Type)
	}
}

type ServerMessage struct {
	Type                string                `json:"type"`
	StartVersion        *StateVersion         `json:"startVersion,omitempty"`
	EndVersion          *StateVersion         `json:"endVersion,omitempty"`
	Modifications       []StateModification   `json:"modifications,omitempty"`
	RequestID           RequestSequenceNumber `json:"requestId,omitempty"`
	Success             *bool                 `json:"success,omitempty"`
	Result              json.RawMessage       `json:"result,omitempty"`
	TS                  string                `json:"ts,omitempty"`
	ErrorData           json.RawMessage       `json:"errorData,omitempty"`
	Error               string                `json:"error,omitempty"`
	BaseVersion         *IdentityVersion      `json:"baseVersion,omitempty"`
	AuthUpdateAttempted *bool                 `json:"authUpdateAttempted,omitempty"`
	Chunk               string                `json:"chunk,omitempty"`
	PartNumber          uint32                `json:"partNumber,omitempty"`
	TotalParts          uint32                `json:"totalParts,omitempty"`
	TransitionID        string                `json:"transitionId,omitempty"`
}

type serverMessageJSON struct {
	Type                string                 `json:"type"`
	StartVersion        *StateVersion          `json:"startVersion,omitempty"`
	EndVersion          *StateVersion          `json:"endVersion,omitempty"`
	Modifications       []StateModification    `json:"modifications,omitempty"`
	RequestID           *RequestSequenceNumber `json:"requestId,omitempty"`
	Success             *bool                  `json:"success,omitempty"`
	Result              json.RawMessage        `json:"result,omitempty"`
	TS                  *string                `json:"ts,omitempty"`
	ErrorData           json.RawMessage        `json:"errorData,omitempty"`
	Error               string                 `json:"error,omitempty"`
	BaseVersion         *IdentityVersion       `json:"baseVersion,omitempty"`
	AuthUpdateAttempted *bool                  `json:"authUpdateAttempted,omitempty"`
	Chunk               *string                `json:"chunk,omitempty"`
	PartNumber          *uint32                `json:"partNumber,omitempty"`
	TotalParts          *uint32                `json:"totalParts,omitempty"`
	TransitionID        *string                `json:"transitionId,omitempty"`
}

func (msg ServerMessage) MarshalJSON() ([]byte, error) {
	switch msg.Type {
	case "Transition":
		if msg.StartVersion == nil {
			return nil, fmt.Errorf("transition message missing startVersion")
		}
		if msg.EndVersion == nil {
			return nil, fmt.Errorf("transition message missing endVersion")
		}
		if msg.Modifications == nil {
			return nil, fmt.Errorf("transition message missing modifications")
		}
		return json.Marshal(serverMessageJSON{
			Type:          "Transition",
			StartVersion:  msg.StartVersion,
			EndVersion:    msg.EndVersion,
			Modifications: msg.Modifications,
		})
	case "TransitionChunk":
		if msg.Chunk == "" {
			return nil, fmt.Errorf("transition chunk message missing chunk")
		}
		if msg.TransitionID == "" {
			return nil, fmt.Errorf("transition chunk message missing transitionId")
		}
		if msg.TotalParts == 0 {
			return nil, fmt.Errorf("transition chunk message missing totalParts")
		}
		if msg.PartNumber >= msg.TotalParts {
			return nil, fmt.Errorf("transition chunk partNumber %d out of range for totalParts %d", msg.PartNumber, msg.TotalParts)
		}
		partNumber := msg.PartNumber
		totalParts := msg.TotalParts
		chunk := msg.Chunk
		transitionID := msg.TransitionID
		return json.Marshal(serverMessageJSON{
			Type:         "TransitionChunk",
			Chunk:        &chunk,
			PartNumber:   &partNumber,
			TotalParts:   &totalParts,
			TransitionID: &transitionID,
		})
	case "MutationResponse":
		if msg.Success == nil {
			return nil, fmt.Errorf("mutation response missing success")
		}
		requestID := msg.RequestID
		payload := serverMessageJSON{
			Type:      "MutationResponse",
			RequestID: &requestID,
			Success:   msg.Success,
			Result:    msg.Result,
			ErrorData: msg.ErrorData,
			Error:     msg.Error,
		}
		if msg.TS != "" {
			payload.TS = &msg.TS
		}
		return json.Marshal(payload)
	case "ActionResponse":
		if msg.Success == nil {
			return nil, fmt.Errorf("action response missing success")
		}
		requestID := msg.RequestID
		return json.Marshal(serverMessageJSON{
			Type:      "ActionResponse",
			RequestID: &requestID,
			Success:   msg.Success,
			Result:    msg.Result,
			ErrorData: msg.ErrorData,
			Error:     msg.Error,
		})
	case "AuthError":
		if msg.Error == "" {
			return nil, fmt.Errorf("auth error message missing error")
		}
		return json.Marshal(serverMessageJSON{
			Type:                "AuthError",
			Error:               msg.Error,
			BaseVersion:         msg.BaseVersion,
			AuthUpdateAttempted: msg.AuthUpdateAttempted,
		})
	case "FatalError":
		if msg.Error == "" {
			return nil, fmt.Errorf("fatal error message missing error")
		}
		return json.Marshal(serverMessageJSON{Type: "FatalError", Error: msg.Error})
	case "Ping":
		return json.Marshal(serverMessageJSON{Type: "Ping"})
	default:
		return nil, fmt.Errorf("unknown server message type %q", msg.Type)
	}
}

func (msg *ServerMessage) UnmarshalJSON(data []byte) error {
	var base struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &base); err != nil {
		return err
	}

	switch base.Type {
	case "Transition":
		var payload serverMessageJSON
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.StartVersion == nil {
			return fmt.Errorf("transition message missing startVersion")
		}
		if payload.EndVersion == nil {
			return fmt.Errorf("transition message missing endVersion")
		}
		if payload.Modifications == nil {
			return fmt.Errorf("transition message missing modifications")
		}
		msg.Type = "Transition"
		msg.StartVersion = payload.StartVersion
		msg.EndVersion = payload.EndVersion
		msg.Modifications = payload.Modifications
		return nil
	case "TransitionChunk":
		var payload serverMessageJSON
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.Chunk == nil || *payload.Chunk == "" {
			return fmt.Errorf("transition chunk message missing chunk")
		}
		if payload.PartNumber == nil {
			return fmt.Errorf("transition chunk message missing partNumber")
		}
		if payload.TotalParts == nil || *payload.TotalParts == 0 {
			return fmt.Errorf("transition chunk message missing totalParts")
		}
		if payload.TransitionID == nil || *payload.TransitionID == "" {
			return fmt.Errorf("transition chunk message missing transitionId")
		}
		if *payload.PartNumber >= *payload.TotalParts {
			return fmt.Errorf("transition chunk partNumber %d out of range for totalParts %d", *payload.PartNumber, *payload.TotalParts)
		}
		msg.Type = "TransitionChunk"
		msg.Chunk = *payload.Chunk
		msg.PartNumber = *payload.PartNumber
		msg.TotalParts = *payload.TotalParts
		msg.TransitionID = *payload.TransitionID
		return nil
	case "MutationResponse", "ActionResponse":
		var payload serverMessageJSON
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.RequestID == nil {
			return fmt.Errorf("%s missing requestId", strings.ToLower(base.Type))
		}
		if payload.Success == nil {
			return fmt.Errorf("%s missing success", strings.ToLower(base.Type))
		}
		if !*payload.Success && len(payload.Result) == 0 && payload.Error == "" {
			return fmt.Errorf("%s error response missing result/error", strings.ToLower(base.Type))
		}
		if base.Type == "ActionResponse" && payload.TS != nil {
			return fmt.Errorf("action response must not include ts")
		}
		if base.Type == "MutationResponse" && payload.TS != nil {
			if _, err := DecodeTimestamp(*payload.TS); err != nil {
				return fmt.Errorf("mutation response has invalid ts: %w", err)
			}
		}
		msg.Type = base.Type
		msg.RequestID = *payload.RequestID
		msg.Success = payload.Success
		msg.Result = payload.Result
		msg.ErrorData = payload.ErrorData
		msg.Error = payload.Error
		if payload.TS != nil {
			msg.TS = *payload.TS
		}
		return nil
	case "AuthError":
		var payload serverMessageJSON
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.Error == "" {
			return fmt.Errorf("auth error message missing error")
		}
		msg.Type = "AuthError"
		msg.Error = payload.Error
		msg.BaseVersion = payload.BaseVersion
		msg.AuthUpdateAttempted = payload.AuthUpdateAttempted
		return nil
	case "FatalError":
		var payload serverMessageJSON
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		if payload.Error == "" {
			return fmt.Errorf("fatal error message missing error")
		}
		msg.Type = "FatalError"
		msg.Error = payload.Error
		return nil
	case "Ping":
		msg.Type = "Ping"
		return nil
	default:
		return fmt.Errorf("unknown server message type %q", base.Type)
	}
}
