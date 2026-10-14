package protocol

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestClientMessageCodecRoundTrip(t *testing.T) {
	in := ClientMessage{Type: "Connect", SessionID: MustSessionID("f47ac10b-58cc-4372-a567-0e02b2c3d479"), ConnectionCount: 1}
	raw, err := EncodeClientMessage(in)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	out, err := DecodeClientMessage(raw)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out.Type != in.Type {
		t.Fatalf("type mismatch: %s != %s", out.Type, in.Type)
	}
	if out.LastCloseReason != "unknown" {
		t.Fatalf("expected connect default lastCloseReason, got %q", out.LastCloseReason)
	}
}

func TestTimestampBase64RoundTrip(t *testing.T) {
	values := []uint64{0, 1, 42, 1<<53 + 7, ^uint64(0)}
	for _, value := range values {
		encoded := EncodeTimestamp(value)
		decoded, err := DecodeTimestamp(encoded)
		if err != nil {
			t.Fatalf("decode failed for %d: %v", value, err)
		}
		if decoded != value {
			t.Fatalf("roundtrip mismatch: got %d want %d", decoded, value)
		}
	}
}

func TestStateVersionTimestampWireShape(t *testing.T) {
	version := StateVersion{
		QuerySet: NewQuerySetVersion(3),
		Identity: NewIdentityVersion(5),
		TS:       NewTimestamp(11),
	}
	raw, err := json.Marshal(version)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal wire map failed: %v", err)
	}
	if wire["ts"] != EncodeTimestamp(11) {
		t.Fatalf("unexpected ts wire value: %v", wire["ts"])
	}

	var decoded StateVersion
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("state version decode failed: %v", err)
	}
	if decoded.QuerySet != version.QuerySet || decoded.Identity != version.Identity || decoded.TS != version.TS {
		t.Fatalf("decoded state version mismatch: got %+v want %+v", decoded, version)
	}
}

func TestStateVersionRejectsInvalidTimestamp(t *testing.T) {
	raw := []byte(`{"querySet":1,"identity":2,"ts":"bad"}`)
	var version StateVersion
	err := json.Unmarshal(raw, &version)
	if err == nil {
		t.Fatalf("expected invalid timestamp error")
	}
}

func TestQuerySetModificationAddRoundTrip(t *testing.T) {
	input := NewQuerySetAdd(Query{
		QueryID: NewQueryID(9),
		UDFPath: "messages:list",
		Args:    json.RawMessage(`[{"limit":10}]`),
	})

	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var output QuerySetModification
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	query, ok := output.Query()
	if !ok {
		t.Fatalf("expected add variant after roundtrip")
	}
	if query.QueryID != NewQueryID(9) || query.UDFPath != "messages:list" {
		t.Fatalf("unexpected query payload: %+v", query)
	}
}

func TestQuerySetModificationRemoveRoundTrip(t *testing.T) {
	input := NewQuerySetRemove(NewQueryID(3))
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var output QuerySetModification
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if !output.IsRemove() {
		t.Fatalf("expected remove variant after roundtrip")
	}
	if output.QueryID() != NewQueryID(3) {
		t.Fatalf("unexpected remove query id: %d", output.QueryID())
	}
}

func TestQuerySetModificationRejectsMalformedAdd(t *testing.T) {
	raw := []byte(`{"type":"Add","queryId":1}`)
	var output QuerySetModification
	if err := json.Unmarshal(raw, &output); err == nil {
		t.Fatalf("expected malformed add decode error")
	}
}

func TestStateModificationVariantsRoundTrip(t *testing.T) {
	updated := NewStateModificationQueryUpdated(NewQueryID(1), json.RawMessage(`{"ok":true}`), nil)
	rawUpdated, err := json.Marshal(updated)
	if err != nil {
		t.Fatalf("marshal query updated failed: %v", err)
	}
	var decodedUpdated StateModification
	if err := json.Unmarshal(rawUpdated, &decodedUpdated); err != nil {
		t.Fatalf("unmarshal query updated failed: %v", err)
	}
	if _, ok := decodedUpdated.QueryUpdated(); !ok {
		t.Fatalf("expected query updated variant")
	}

	failed := NewStateModificationQueryFailed(NewQueryID(2), "boom", json.RawMessage(`{"code":"X"}`), nil)
	rawFailed, err := json.Marshal(failed)
	if err != nil {
		t.Fatalf("marshal query failed failed: %v", err)
	}
	var decodedFailed StateModification
	if err := json.Unmarshal(rawFailed, &decodedFailed); err != nil {
		t.Fatalf("unmarshal query failed failed: %v", err)
	}
	if data, ok := decodedFailed.QueryFailed(); !ok || data.ErrorMessage != "boom" {
		t.Fatalf("expected query failed variant with error message")
	}

	removed := NewStateModificationQueryRemoved(NewQueryID(3))
	rawRemoved, err := json.Marshal(removed)
	if err != nil {
		t.Fatalf("marshal query removed failed: %v", err)
	}
	var decodedRemoved StateModification
	if err := json.Unmarshal(rawRemoved, &decodedRemoved); err != nil {
		t.Fatalf("unmarshal query removed failed: %v", err)
	}
	if queryID, ok := decodedRemoved.QueryRemoved(); !ok || queryID != NewQueryID(3) {
		t.Fatalf("expected query removed variant")
	}
}

func TestStateModificationQueryUpdatedNullValue(t *testing.T) {
	// Convex legitimately sends QueryUpdated with value=null when a query
	// returns no results. The SDK must accept this instead of erroring,
	// which previously caused an infinite WebSocket reconnect loop.
	raw := []byte(`{"type":"QueryUpdated","queryId":1,"value":null}`)
	var mod StateModification
	if err := json.Unmarshal(raw, &mod); err != nil {
		t.Fatalf("query updated with null value should not error: %v", err)
	}
	data, ok := mod.QueryUpdated()
	if !ok {
		t.Fatalf("expected query updated variant")
	}
	if string(data.Value) != "null" {
		t.Fatalf("expected value to be literal 'null', got %q", string(data.Value))
	}
	if data.QueryID != NewQueryID(1) {
		t.Fatalf("unexpected query id: %d", data.QueryID)
	}
}

func TestStateModificationRejectsMalformedVariant(t *testing.T) {
	raw := []byte(`{"type":"QueryFailed","queryId":1}`)
	var output StateModification
	if err := json.Unmarshal(raw, &output); err == nil {
		t.Fatalf("expected malformed state modification decode error")
	}
}

func TestAuthenticationTokenVariantsRoundTrip(t *testing.T) {
	admin := NewAdminAuthenticationToken("adm", json.RawMessage(`{"sub":"user_1"}`))
	rawAdmin, err := json.Marshal(admin)
	if err != nil {
		t.Fatalf("marshal admin token failed: %v", err)
	}
	var decodedAdmin AuthenticationToken
	if err := json.Unmarshal(rawAdmin, &decodedAdmin); err != nil {
		t.Fatalf("unmarshal admin token failed: %v", err)
	}
	if adminPayload, ok := decodedAdmin.Admin(); !ok || adminPayload.Value != "adm" {
		t.Fatalf("expected admin token variant")
	}

	user := NewUserAuthenticationToken("user-jwt")
	rawUser, err := json.Marshal(user)
	if err != nil {
		t.Fatalf("marshal user token failed: %v", err)
	}
	var decodedUser AuthenticationToken
	if err := json.Unmarshal(rawUser, &decodedUser); err != nil {
		t.Fatalf("unmarshal user token failed: %v", err)
	}
	if userValue, ok := decodedUser.User(); !ok || userValue != "user-jwt" {
		t.Fatalf("expected user token variant")
	}

	none := NewNoAuthenticationToken()
	rawNone, err := json.Marshal(none)
	if err != nil {
		t.Fatalf("marshal none token failed: %v", err)
	}
	var decodedNone AuthenticationToken
	if err := json.Unmarshal(rawNone, &decodedNone); err != nil {
		t.Fatalf("unmarshal none token failed: %v", err)
	}
	if decodedNone.Kind() != "None" {
		t.Fatalf("expected none token variant")
	}
}

func TestAuthenticationTokenDecodeCompatibilityImpersonating(t *testing.T) {
	raw := []byte(`{"tokenType":"Admin","value":"adm","impersonating":{"sub":"legacy"}}`)
	var decoded AuthenticationToken
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal compatibility token failed: %v", err)
	}
	adminPayload, ok := decoded.Admin()
	if !ok {
		t.Fatalf("expected admin token variant")
	}
	if string(adminPayload.ActingAs) != `{"sub":"legacy"}` {
		t.Fatalf("unexpected actingAs payload: %s", string(adminPayload.ActingAs))
	}
}

func TestConnectDecodeRejectsMissingRequiredFields(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"type":"Connect","connectionCount":1}`),
		[]byte(`{"type":"Connect","sessionId":"f47ac10b-58cc-4372-a567-0e02b2c3d479"}`),
	}
	for _, raw := range tests {
		if _, err := DecodeClientMessage(raw); err == nil {
			t.Fatalf("expected connect decode failure for payload %s", string(raw))
		}
	}
}

func TestConnectDecodeDefaultsAndTimestampValidation(t *testing.T) {
	valid := []byte(`{"type":"Connect","sessionId":"f47ac10b-58cc-4372-a567-0e02b2c3d479","connectionCount":0}`)
	decoded, err := DecodeClientMessage(valid)
	if err != nil {
		t.Fatalf("connect decode failed: %v", err)
	}
	if decoded.LastCloseReason != "unknown" {
		t.Fatalf("expected default lastCloseReason=unknown, got %q", decoded.LastCloseReason)
	}

	invalidTS := []byte(`{"type":"Connect","sessionId":"f47ac10b-58cc-4372-a567-0e02b2c3d479","connectionCount":0,"maxObservedTimestamp":"bad"}`)
	if _, err := DecodeClientMessage(invalidTS); err == nil {
		t.Fatalf("expected invalid maxObservedTimestamp decode error")
	}
}

func TestClientMessageRemainingVariantsRoundTrip(t *testing.T) {
	modify := ClientMessage{
		Type:        "ModifyQuerySet",
		BaseVersion: 1,
		NewVersion:  2,
		Modifications: []QuerySetModification{
			NewQuerySetRemove(NewQueryID(9)),
		},
	}
	encodedModify, err := EncodeClientMessage(modify)
	if err != nil {
		t.Fatalf("encode modify failed: %v", err)
	}
	if _, err := DecodeClientMessage(encodedModify); err != nil {
		t.Fatalf("decode modify failed: %v", err)
	}

	mutation := ClientMessage{
		Type:      "Mutation",
		RequestID: NewRequestSequenceNumber(3),
		UDFPath:   "messages:send",
		Args:      json.RawMessage(`[{}]`),
	}
	encodedMutation, err := EncodeClientMessage(mutation)
	if err != nil {
		t.Fatalf("encode mutation failed: %v", err)
	}
	if _, err := DecodeClientMessage(encodedMutation); err != nil {
		t.Fatalf("decode mutation failed: %v", err)
	}

	auth := ClientMessage{
		Type:        "Authenticate",
		BaseVersion: 4,
		Token:       NewUserAuthenticationToken("jwt"),
	}
	encodedAuth, err := EncodeClientMessage(auth)
	if err != nil {
		t.Fatalf("encode authenticate failed: %v", err)
	}
	decodedAuth, err := DecodeClientMessage(encodedAuth)
	if err != nil {
		t.Fatalf("decode authenticate failed: %v", err)
	}
	if value, ok := decodedAuth.Token.User(); !ok || value != "jwt" {
		t.Fatalf("expected decoded user token")
	}

	event := ClientMessage{Type: "Event", EventType: "presence", Event: json.RawMessage(`{"id":1}`)}
	encodedEvent, err := EncodeClientMessage(event)
	if err != nil {
		t.Fatalf("encode event failed: %v", err)
	}
	if _, err := DecodeClientMessage(encodedEvent); err != nil {
		t.Fatalf("decode event failed: %v", err)
	}
}

func TestClientMessageVariantValidation(t *testing.T) {
	tests := []ClientMessage{
		{Type: "ModifyQuerySet", BaseVersion: 1, NewVersion: 2},
		{Type: "Mutation", RequestID: NewRequestSequenceNumber(1), Args: json.RawMessage(`[{}]`)},
		{Type: "Action", RequestID: NewRequestSequenceNumber(1), UDFPath: "x", Args: nil},
		{Type: "Authenticate", BaseVersion: 1},
		{Type: "Event", Event: json.RawMessage(`{}`)},
	}
	for _, message := range tests {
		if _, err := EncodeClientMessage(message); err == nil {
			t.Fatalf("expected encode validation failure for type %s", message.Type)
		}
	}
}

func TestServerTransitionAndChunkVariants(t *testing.T) {
	transition := ServerMessage{
		Type:         "Transition",
		StartVersion: &StateVersion{QuerySet: NewQuerySetVersion(0), Identity: NewIdentityVersion(0), TS: NewTimestamp(1)},
		EndVersion:   &StateVersion{QuerySet: NewQuerySetVersion(1), Identity: NewIdentityVersion(0), TS: NewTimestamp(2)},
		Modifications: []StateModification{
			NewStateModificationQueryRemoved(NewQueryID(7)),
		},
	}
	encodedTransition, err := EncodeServerMessage(transition)
	if err != nil {
		t.Fatalf("encode transition failed: %v", err)
	}
	decodedTransition, err := DecodeServerMessage(encodedTransition)
	if err != nil {
		t.Fatalf("decode transition failed: %v", err)
	}
	if decodedTransition.Type != "Transition" || decodedTransition.StartVersion == nil || decodedTransition.EndVersion == nil {
		t.Fatalf("decoded transition missing required fields")
	}

	chunk := ServerMessage{Type: "TransitionChunk", Chunk: "abc", PartNumber: 0, TotalParts: 2, TransitionID: "tr-1"}
	encodedChunk, err := EncodeServerMessage(chunk)
	if err != nil {
		t.Fatalf("encode transition chunk failed: %v", err)
	}
	decodedChunk, err := DecodeServerMessage(encodedChunk)
	if err != nil {
		t.Fatalf("decode transition chunk failed: %v", err)
	}
	if decodedChunk.TransitionID != "tr-1" {
		t.Fatalf("unexpected decoded transition id: %s", decodedChunk.TransitionID)
	}
}

func TestServerTransitionChunkRejectsMalformedPayload(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"type":"TransitionChunk","partNumber":0,"totalParts":2,"transitionId":"x"}`),
		[]byte(`{"type":"TransitionChunk","chunk":"x","partNumber":2,"totalParts":2,"transitionId":"x"}`),
		[]byte(`{"type":"Transition","endVersion":{"querySet":0,"identity":0,"ts":"AAAAAAAAAAA="},"modifications":[]}`),
	}
	for _, raw := range tests {
		if _, err := DecodeServerMessage(raw); err == nil {
			t.Fatalf("expected decode failure for malformed server payload %s", string(raw))
		}
	}
}

func TestServerRemainingVariantsRoundTrip(t *testing.T) {
	success := true
	mutation := ServerMessage{
		Type:      "MutationResponse",
		RequestID: NewRequestSequenceNumber(1),
		Success:   &success,
		Result:    json.RawMessage(`{"ok":true}`),
		TS:        EncodeTimestamp(9),
	}
	encodedMutation, err := EncodeServerMessage(mutation)
	if err != nil {
		t.Fatalf("encode mutation response failed: %v", err)
	}
	if _, err := DecodeServerMessage(encodedMutation); err != nil {
		t.Fatalf("decode mutation response failed: %v", err)
	}

	action := ServerMessage{
		Type:      "ActionResponse",
		RequestID: NewRequestSequenceNumber(2),
		Success:   &success,
		Result:    json.RawMessage(`{"ok":true}`),
	}
	encodedAction, err := EncodeServerMessage(action)
	if err != nil {
		t.Fatalf("encode action response failed: %v", err)
	}
	if _, err := DecodeServerMessage(encodedAction); err != nil {
		t.Fatalf("decode action response failed: %v", err)
	}

	authError := ServerMessage{Type: "AuthError", Error: "expired"}
	encodedAuthError, err := EncodeServerMessage(authError)
	if err != nil {
		t.Fatalf("encode auth error failed: %v", err)
	}
	if _, err := DecodeServerMessage(encodedAuthError); err != nil {
		t.Fatalf("decode auth error failed: %v", err)
	}

	fatalError := ServerMessage{Type: "FatalError", Error: "fatal"}
	encodedFatal, err := EncodeServerMessage(fatalError)
	if err != nil {
		t.Fatalf("encode fatal error failed: %v", err)
	}
	if _, err := DecodeServerMessage(encodedFatal); err != nil {
		t.Fatalf("decode fatal error failed: %v", err)
	}

	ping := ServerMessage{Type: "Ping"}
	encodedPing, err := EncodeServerMessage(ping)
	if err != nil {
		t.Fatalf("encode ping failed: %v", err)
	}
	if _, err := DecodeServerMessage(encodedPing); err != nil {
		t.Fatalf("decode ping failed: %v", err)
	}
}

func TestServerRemainingVariantsRejectMalformedPayload(t *testing.T) {
	tests := [][]byte{
		[]byte(`{"type":"MutationResponse","requestId":1}`),
		[]byte(`{"type":"ActionResponse","requestId":1,"success":false}`),
		[]byte(`{"type":"AuthError"}`),
		[]byte(`{"type":"FatalError"}`),
		[]byte(`{"type":"UnknownType"}`),
	}
	for _, raw := range tests {
		if _, err := DecodeServerMessage(raw); err == nil {
			t.Fatalf("expected malformed decode failure for payload %s", string(raw))
		}
	}
}

func TestClientEncodeWireKeys(t *testing.T) {
	message := ClientMessage{
		Type:        "Authenticate",
		BaseVersion: 3,
		Token:       NewUserAuthenticationToken("jwt"),
	}
	encoded, err := EncodeClientMessage(message)
	if err != nil {
		t.Fatalf("encode authenticate failed: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatalf("decode encoded json failed: %v", err)
	}
	required := []string{"type", "baseVersion", "tokenType", "value"}
	for _, key := range required {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected key %q in authenticate payload", key)
		}
	}
	if _, ok := payload["actingAs"]; ok {
		t.Fatalf("unexpected actingAs key for user token")
	}
}

func TestDecodeClientMessageStrictEnvelopeErrors(t *testing.T) {
	if _, err := DecodeClientMessage([]byte(`{"sessionId":"x"}`)); err == nil {
		t.Fatalf("expected missing type decode error")
	}
	if _, err := DecodeClientMessage([]byte(`not-json`)); err == nil {
		t.Fatalf("expected invalid json decode error")
	}
}

func TestDecodeClientMessageAuthenticateLegacyCompatibility(t *testing.T) {
	raw := []byte(`{"type":"Authenticate","baseVersion":1,"token":"legacy-token","admin":true,"actingAs":{"sub":"u1"}}`)
	decoded, err := DecodeClientMessage(raw)
	if err != nil {
		t.Fatalf("legacy authenticate decode failed: %v", err)
	}
	admin, ok := decoded.Token.Admin()
	if !ok || admin.Value != "legacy-token" {
		t.Fatalf("expected decoded legacy admin token")
	}
}

func TestServerEncodeValidationFailures(t *testing.T) {
	success := false
	tests := []ServerMessage{
		{Type: "Transition"},
		{Type: "TransitionChunk", Chunk: "x", PartNumber: 2, TotalParts: 2, TransitionID: "t"},
		{Type: "MutationResponse", RequestID: NewRequestSequenceNumber(1), Success: &success},
		{Type: "MutationResponse", RequestID: NewRequestSequenceNumber(1), Success: &success, Result: nil, Error: ""},
		{Type: "FatalError"},
	}
	for _, message := range tests {
		if _, err := EncodeServerMessage(message); err == nil {
			t.Fatalf("expected encode validation error for server message type %s", message.Type)
		}
	}
}

func TestDecodeServerMessageEnvelopeErrors(t *testing.T) {
	if _, err := DecodeServerMessage([]byte(`{"requestId":1}`)); err == nil {
		t.Fatalf("expected missing type decode error")
	}
	if _, err := DecodeServerMessage([]byte(`nope`)); err == nil {
		t.Fatalf("expected invalid json decode error")
	}
}

func TestServerDecodeNullOptionalFields(t *testing.T) {
	raw := []byte(`{"type":"AuthError","error":"expired","baseVersion":null,"authUpdateAttempted":null}`)
	decoded, err := DecodeServerMessage(raw)
	if err != nil {
		t.Fatalf("auth error decode failed: %v", err)
	}
	if decoded.BaseVersion != nil || decoded.AuthUpdateAttempted != nil {
		t.Fatalf("expected null optional fields to decode as nil")
	}
}

func TestServerDecodeRejectsActionResponseTS(t *testing.T) {
	raw := []byte(`{"type":"ActionResponse","requestId":1,"success":true,"result":{},"ts":"AAAAAAAAAAA="}`)
	if _, err := DecodeServerMessage(raw); err == nil {
		t.Fatalf("expected action response ts decode error")
	}
}

func TestMalformedProtocolCorpus(t *testing.T) {
	type malformedCase struct {
		Name     string `json:"name"`
		Kind     string `json:"kind"`
		Payload  string `json:"payload"`
		Contains string `json:"contains"`
	}

	raw, err := os.ReadFile("testdata/malformed_corpus.json")
	if err != nil {
		t.Fatalf("read malformed corpus failed: %v", err)
	}
	var cases []malformedCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("decode malformed corpus failed: %v", err)
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			var decodeErr error
			switch tc.Kind {
			case "client":
				_, decodeErr = DecodeClientMessage([]byte(tc.Payload))
			case "server":
				_, decodeErr = DecodeServerMessage([]byte(tc.Payload))
			default:
				t.Fatalf("unknown corpus kind %q", tc.Kind)
			}
			if decodeErr == nil {
				t.Fatalf("expected malformed decode error")
			}
			if tc.Contains != "" && !strings.Contains(strings.ToLower(decodeErr.Error()), strings.ToLower(tc.Contains)) {
				t.Fatalf("expected error to contain %q, got %q", tc.Contains, decodeErr.Error())
			}
		})
	}
}

func TestRustFixtureConformance(t *testing.T) {
	type fixtureVector struct {
		Name    string          `json:"name"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
		Value   uint64          `json:"value"`
		Encoded string          `json:"encoded"`
	}
	type fixtureFile struct {
		SourceCommit string          `json:"source_commit"`
		Vectors      []fixtureVector `json:"vectors"`
	}

	raw, err := os.ReadFile("testdata/rust_fixture_vectors.json")
	if err != nil {
		t.Fatalf("read rust fixture vectors failed: %v", err)
	}
	var fixtures fixtureFile
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatalf("decode rust fixture vectors failed: %v", err)
	}
	if fixtures.SourceCommit == "" {
		t.Fatalf("rust fixture vectors missing source_commit")
	}

	for _, vector := range fixtures.Vectors {
		vector := vector
		t.Run(vector.Name, func(t *testing.T) {
			switch vector.Kind {
			case "timestamp":
				encoded := EncodeTimestamp(vector.Value)
				if encoded != vector.Encoded {
					t.Fatalf("timestamp encode drift: got %q want %q", encoded, vector.Encoded)
				}
				decoded, err := DecodeTimestamp(vector.Encoded)
				if err != nil {
					t.Fatalf("timestamp decode failed: %v", err)
				}
				if decoded != vector.Value {
					t.Fatalf("timestamp decode drift: got %d want %d", decoded, vector.Value)
				}
			case "client_decode":
				message, err := DecodeClientMessage(vector.Payload)
				if err != nil {
					t.Fatalf("client fixture decode failed: %v", err)
				}
				if _, err := EncodeClientMessage(message); err != nil {
					t.Fatalf("client fixture re-encode failed: %v", err)
				}
			default:
				t.Fatalf("unknown fixture kind %q", vector.Kind)
			}
		})
	}
}
