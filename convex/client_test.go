package convex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/get-convex/convex-go/internal/protocol"
	syncproto "github.com/get-convex/convex-go/internal/sync"
)

func TestOutboundFunctionArgsAreArrayWrapped(t *testing.T) {
	client := newClient()
	sent := make(chan protocol.ClientMessage, 16)

	client.mu.Lock()
	client.connected = true
	client.workerStarted = true
	client.sendFn = func(_ context.Context, message protocol.ClientMessage) error {
		sent <- message
		return nil
	}
	client.mu.Unlock()

	go client.workerLoop()
	t.Cleanup(client.Close)

	sub, err := client.Subscribe(context.Background(), "messages:list", map[string]any{})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Close()

	modify := awaitSentMessageType(t, sent, "ModifyQuerySet")
	if len(modify.Modifications) != 1 {
		t.Fatalf("expected one query modification, got %d", len(modify.Modifications))
	}
	query, ok := modify.Modifications[0].Query()
	if !ok {
		t.Fatalf("expected query add modification")
	}

	var subscribeArgs []any
	if err := json.Unmarshal(query.Args, &subscribeArgs); err != nil {
		t.Fatalf("failed decoding subscribe args: %v", err)
	}
	if len(subscribeArgs) != 1 {
		t.Fatalf("expected subscribe args array length 1, got %d (%s)", len(subscribeArgs), string(query.Args))
	}
	firstSubscribeArg, ok := subscribeArgs[0].(map[string]any)
	if !ok {
		t.Fatalf("expected subscribe first arg object, got %T", subscribeArgs[0])
	}
	if len(firstSubscribeArg) != 0 {
		t.Fatalf("expected empty subscribe arg object, got %#v", firstSubscribeArg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = client.Mutation(ctx, "messages:create", map[string]any{"count": int64(1)})

	mutation := awaitSentMessageType(t, sent, "Mutation")
	var mutationArgs []any
	if err := json.Unmarshal(mutation.Args, &mutationArgs); err != nil {
		t.Fatalf("failed decoding mutation args: %v", err)
	}
	if len(mutationArgs) != 1 {
		t.Fatalf("expected mutation args array length 1, got %d (%s)", len(mutationArgs), string(mutation.Args))
	}
	firstMutationArg, ok := mutationArgs[0].(map[string]any)
	if !ok {
		t.Fatalf("expected mutation first arg object, got %T", mutationArgs[0])
	}
	count, ok := firstMutationArg["count"].(map[string]any)
	if !ok {
		t.Fatalf("expected mutation count encoded as object, got %#v", firstMutationArg["count"])
	}
	if got := count["$integer"]; got != "AQAAAAAAAAA=" {
		t.Fatalf("expected integer wire encoding %q, got %#v", "AQAAAAAAAAA=", got)
	}
}

func TestQueryMatchesSubscribeFirstValue(t *testing.T) {
	server := newSyncTestServer(t)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	result, err := client.Query(context.Background(), "test:query", map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	value, err := result.Unwrap()
	if err != nil {
		t.Fatalf("unwrap failed: %v", err)
	}

	raw, ok := value.Raw().(map[string]any)
	if !ok {
		t.Fatalf("expected object result, got %T", value.Raw())
	}
	if raw["source"] != "server" {
		t.Fatalf("expected server-sourced value, got %#v", raw)
	}
}

func TestWatchAllSnapshot(t *testing.T) {
	server := newSyncTestServer(t)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	sub, err := client.Subscribe(context.Background(), "test:query", map[string]any{})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Close()

	watch := client.WatchAll()
	defer watch.Close()

	select {
	case snapshot := <-watch.Updates():
		if len(snapshot) == 0 {
			t.Fatalf("expected non-empty snapshot")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for watch_all snapshot")
	}
}

func TestCloneSharesInstance(t *testing.T) {
	c := NewClient()
	clone := c.Clone()
	if c != clone {
		t.Fatalf("expected clone to share connection instance")
	}
}

func TestWebSocketStateCallbackOrderingInitialConnect(t *testing.T) {
	server := newSyncTestServer(t)
	defer server.Close()

	states := make(chan WebSocketState, 8)
	client := NewClientBuilder().
		WithDeploymentURL(server.URL).
		WithWebSocketStateCallback(func(state WebSocketState) {
			states <- state
		}).
		Build()
	defer client.Close()

	_, err := client.Query(context.Background(), "test:query", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	first := awaitState(t, states)
	second := awaitState(t, states)
	if first != WebSocketStateConnecting || second != WebSocketStateConnected {
		t.Fatalf("unexpected initial state ordering: %s -> %s", first, second)
	}
}

func TestWebSocketStateCallbackOrderingReconnect(t *testing.T) {
	server := newReconnectingSyncTestServer(t)
	defer server.Close()

	var (
		mu     sync.Mutex
		states []WebSocketState
	)
	client := NewClientBuilder().
		WithDeploymentURL(server.URL).
		WithWebSocketStateCallback(func(state WebSocketState) {
			mu.Lock()
			states = append(states, state)
			mu.Unlock()
		}).
		Build()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Query(ctx, "test:query", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("query with reconnect failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(states) < 4 {
		t.Fatalf("expected reconnect state sequence, got %v", states)
	}
	expectedPrefix := []WebSocketState{WebSocketStateConnecting, WebSocketStateConnected, WebSocketStateReconnecting, WebSocketStateConnected}
	for i, expected := range expectedPrefix {
		if states[i] != expected {
			t.Fatalf("unexpected state at %d: got %s want %s (full: %v)", i, states[i], expected, states)
		}
	}
}

func TestTransitionVersionMismatchTriggersReconnect(t *testing.T) {
	server := newMismatchedTransitionServer(t)
	defer server.Close()

	states := make(chan WebSocketState, 16)
	client := NewClientBuilder().
		WithDeploymentURL(server.URL).
		WithWebSocketStateCallback(func(state WebSocketState) {
			states <- state
		}).
		Build()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Query(ctx, "test:query", map[string]any{"x": 1}); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case state := <-states:
			if state == WebSocketStateReconnecting {
				return
			}
		case <-deadline:
			t.Fatalf("expected reconnecting state after transition version mismatch")
		}
	}
}

func TestTransitionChunkAssemblyAppliesTransition(t *testing.T) {
	server := newChunkedTransitionServer(t, false)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	result, err := client.Query(context.Background(), "test:query", map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	value, err := result.Unwrap()
	if err != nil {
		t.Fatalf("unwrap failed: %v", err)
	}

	raw, ok := value.Raw().(map[string]any)
	if !ok {
		t.Fatalf("expected object result, got %T", value.Raw())
	}
	if raw["source"] != "server" {
		t.Fatalf("expected server-sourced value, got %#v", raw)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.transitionChunks) != 0 {
		t.Fatalf("expected transition chunk buffers to be cleared, got %d", len(client.transitionChunks))
	}
}

func TestTransitionChunkOutOfOrderTriggersReconnect(t *testing.T) {
	server := newChunkedTransitionServer(t, true)
	defer server.Close()

	states := make(chan WebSocketState, 16)
	client := NewClientBuilder().
		WithDeploymentURL(server.URL).
		WithWebSocketStateCallback(func(state WebSocketState) {
			states <- state
		}).
		Build()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Query(ctx, "test:query", map[string]any{"x": 1}); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		select {
		case state := <-states:
			if state == WebSocketStateReconnecting {
				return
			}
		case <-deadline:
			t.Fatalf("expected reconnecting state after out-of-order transition chunk")
		}
	}
}

func TestSetAuthCallbackRequiresFetcher(t *testing.T) {
	client := NewClient()
	if err := client.SetAuthCallback(nil); err == nil {
		t.Fatalf("expected error when auth callback is nil")
	}
}

func TestSetAuthCallbackReconnectForceRefreshAndRetry(t *testing.T) {
	server := newReconnectingSyncTestServer(t)
	defer server.Close()

	var (
		mu       sync.Mutex
		calls    []bool
		refreshN int
	)

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	err := client.SetAuthCallback(func(forceRefresh bool) (*string, error) {
		mu.Lock()
		calls = append(calls, forceRefresh)
		if forceRefresh {
			refreshN++
			if refreshN == 1 {
				mu.Unlock()
				return nil, fmt.Errorf("temporary auth refresh failure")
			}
		}
		mu.Unlock()

		token := "token"
		return &token, nil
	})
	if err != nil {
		t.Fatalf("set auth callback failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Query(ctx, "test:query", map[string]any{"x": 1}); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) < 3 {
		t.Fatalf("expected initial + reconnect auth callback calls, got %v", calls)
	}
	if calls[0] {
		t.Fatalf("expected first auth callback call with forceRefresh=false")
	}
	trueCalls := 0
	for _, call := range calls {
		if call {
			trueCalls++
		}
	}
	if trueCalls < 2 {
		t.Fatalf("expected reconnect refresh retries with forceRefresh=true, got %v", calls)
	}

	client.mu.Lock()
	identityVersion := client.state.IdentityVersion()
	client.mu.Unlock()
	if identityVersion < 2 {
		t.Fatalf("expected identity version to advance across auth callback + reconnect refresh, got %d", identityVersion)
	}
}

func TestReconnectReplayOrderAuthQueriesThenPendingRequests(t *testing.T) {
	server := newReplayOrderServer(t)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	token := "auth-token"
	client.SetAuth(&token)

	sub, err := client.Subscribe(context.Background(), "test:query", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Close()
	select {
	case <-sub.Updates():
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for initial subscription value")
	}

	mutationCtx, cancelMutation := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMutation()
	if _, err := client.Mutation(mutationCtx, "test:mutation", map[string]any{"x": 1}); err != nil {
		t.Fatalf("mutation failed: %v (second connection replay: %v)", err, server.secondConnectionReplay())
	}

	replayed := server.secondConnectionReplay()
	if len(replayed) < 3 {
		t.Fatalf("expected replayed messages on second connection, got %v", replayed)
	}
	expected := []string{"Authenticate", "ModifyQuerySet", "Mutation"}
	for i, want := range expected {
		if replayed[i] != want {
			t.Fatalf("unexpected replay order at %d: got %s want %s (full: %v)", i, replayed[i], want, replayed)
		}
	}
}

func TestProtocolFailureReconnectPayloadIncludesReasonAndMaxObservedTimestamp(t *testing.T) {
	client := newClient()
	client.mu.Lock()
	client.connected = true
	client.state.UpdateObservedTimestamp(5)
	client.pending[1] = &pendingRequest{kind: "mutation", waitingOnTS: true, visibleTS: 9}
	requests := make(chan syncproto.ReconnectRequest, 1)
	client.reconnectFn = func(_ context.Context, request syncproto.ReconnectRequest) error {
		requests <- request
		return nil
	}
	client.mu.Unlock()

	client.onProtocolFailure(errors.New("protocol decode failure: malformed server frame"))

	select {
	case request := <-requests:
		if !strings.Contains(request.Reason, "protocol decode failure") {
			t.Fatalf("unexpected reconnect reason %q", request.Reason)
		}
		if request.MaxObservedTimestamp != 9 {
			t.Fatalf("expected max observed timestamp 9, got %d", request.MaxObservedTimestamp)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for reconnect request")
	}
}

func TestFailureClassesPropagateReconnectReason(t *testing.T) {
	tests := []struct {
		name           string
		expectedReason string
		trigger        func(*Client)
	}{
		{
			name:           "auth error",
			expectedReason: "auth error",
			trigger: func(c *Client) {
				c.handleServerMessage(protocol.ServerMessage{Type: "AuthError", Error: "bad token"})
			},
		},
		{
			name:           "fatal error",
			expectedReason: "fatal error",
			trigger: func(c *Client) {
				c.handleServerMessage(protocol.ServerMessage{Type: "FatalError", Error: "boom"})
			},
		},
		{
			name:           "unknown server message",
			expectedReason: "unknown server message type",
			trigger: func(c *Client) {
				c.handleServerMessage(protocol.ServerMessage{Type: "NotARealMessage"})
			},
		},
		{
			name:           "transport protocol failure",
			expectedReason: "protocol decode failure",
			trigger: func(c *Client) {
				c.handleWorkerEvent(workerEvent{kind: workerEventTransportErr, err: errors.New("protocol decode failure: bad frame")})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newClient()
			client.mu.Lock()
			client.connected = true
			requests := make(chan syncproto.ReconnectRequest, 1)
			client.reconnectFn = func(_ context.Context, request syncproto.ReconnectRequest) error {
				requests <- request
				return nil
			}
			client.mu.Unlock()

			tc.trigger(client)

			select {
			case request := <-requests:
				if !strings.Contains(request.Reason, tc.expectedReason) {
					t.Fatalf("expected reconnect reason containing %q, got %q", tc.expectedReason, request.Reason)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("timed out waiting for reconnect request")
			}
		})
	}
}

func TestMutationContextCancellationRemovesPendingRequest(t *testing.T) {
	server := newBlackholeSyncTestServer(t)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := client.Mutation(ctx, "test:mutation", map[string]any{"x": 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.pending) != 0 {
		t.Fatalf("expected pending requests to be cleared after cancellation, got %d", len(client.pending))
	}
}

func TestWatchAllSnapshotCoherentAcrossSubscriptions(t *testing.T) {
	server := newSyncTestServer(t)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	defer client.Close()

	subA, err := client.Subscribe(context.Background(), "test:query", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("subscribe A failed: %v", err)
	}
	defer subA.Close()
	subB, err := client.Subscribe(context.Background(), "test:query", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("subscribe B failed: %v", err)
	}
	defer subB.Close()

	select {
	case <-subA.Updates():
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for subscription A update")
	}
	select {
	case <-subB.Updates():
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for subscription B update")
	}

	watch := client.WatchAll()
	defer watch.Close()

	select {
	case snapshot := <-watch.Updates():
		if len(snapshot) != 2 {
			t.Fatalf("expected coherent snapshot with 2 subscribers, got %d (%v)", len(snapshot), snapshot)
		}
		for subID, value := range snapshot {
			raw, ok := value.Raw().(map[string]any)
			if !ok {
				t.Fatalf("subscriber %d expected object value, got %T", subID, value.Raw())
			}
			if raw["source"] != "server" {
				t.Fatalf("subscriber %d expected server source, got %#v", subID, raw)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for coherent watch_all snapshot")
	}
}

func TestCloneCloseLifecycleNoPanic(t *testing.T) {
	server := newSyncTestServer(t)
	defer server.Close()

	client := NewClientBuilder().WithDeploymentURL(server.URL).Build()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Query(ctx, "test:query", map[string]any{}); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	clone := client.Clone()
	clone.Close()
	client.Close()
}

func TestSetAuthThroughWorkerEnqueuesAuthenticate(t *testing.T) {
	client := newClient()
	sent := make(chan protocol.ClientMessage, 2)

	client.mu.Lock()
	client.connected = true
	client.workerStarted = true
	client.sendFn = func(_ context.Context, message protocol.ClientMessage) error {
		sent <- message
		return nil
	}
	client.mu.Unlock()

	go client.workerLoop()

	token := "worker-token"
	client.SetAuth(&token)

	select {
	case message := <-sent:
		if message.Type != "Authenticate" {
			t.Fatalf("expected authenticate message, got %s", message.Type)
		}
		if userToken, ok := message.Token.User(); !ok || userToken != token {
			t.Fatalf("expected user auth token %q, got %#v", token, message.Token)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for authenticate message")
	}

	client.Close()
}

func TestSetAuthCallbackThroughWorkerEnqueuesAuthenticate(t *testing.T) {
	client := newClient()
	sent := make(chan protocol.ClientMessage, 2)
	var calls []bool

	client.mu.Lock()
	client.connected = true
	client.workerStarted = true
	client.sendFn = func(_ context.Context, message protocol.ClientMessage) error {
		sent <- message
		return nil
	}
	client.mu.Unlock()

	go client.workerLoop()

	err := client.SetAuthCallback(func(forceRefresh bool) (*string, error) {
		calls = append(calls, forceRefresh)
		token := "callback-token"
		return &token, nil
	})
	if err != nil {
		t.Fatalf("set auth callback failed: %v", err)
	}
	if len(calls) != 1 || calls[0] {
		t.Fatalf("expected immediate callback invocation with forceRefresh=false, got %v", calls)
	}

	select {
	case message := <-sent:
		if message.Type != "Authenticate" {
			t.Fatalf("expected authenticate message, got %s", message.Type)
		}
		if userToken, ok := message.Token.User(); !ok || userToken != "callback-token" {
			t.Fatalf("expected callback user token, got %#v", message.Token)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for authenticate message")
	}

	client.Close()
}

func awaitState(t *testing.T, states <-chan WebSocketState) WebSocketState {
	t.Helper()
	select {
	case state := <-states:
		return state
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for websocket state callback")
		return ""
	}
}

func awaitSentMessageType(t *testing.T, sent <-chan protocol.ClientMessage, messageType string) protocol.ClientMessage {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case message := <-sent:
			if message.Type == messageType {
				return message
			}
		case <-deadline:
			t.Fatalf("timed out waiting for client message type %q", messageType)
			return protocol.ClientMessage{}
		}
	}
}

func newReconnectingSyncTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	var mu sync.Mutex
	connections := 0

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sync" {
			http.NotFound(w, r)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connections++
		connectionIndex := connections
		mu.Unlock()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			message, err := protocol.DecodeClientMessage(data)
			if err != nil {
				continue
			}

			if message.Type == "Connect" && connectionIndex == 1 {
				_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "force reconnect"), time.Now().Add(time.Second))
				return
			}

			if message.Type != "ModifyQuerySet" {
				continue
			}
			for _, mod := range message.Modifications {
				query, ok := mod.Query()
				if !ok {
					continue
				}
				payload, err := json.Marshal(NewValue(map[string]any{"source": "server", "query": query.UDFPath}))
				if err != nil {
					t.Fatalf("marshal failed: %v", err)
				}
				transition := protocol.ServerMessage{
					Type: "Transition",
					StartVersion: &protocol.StateVersion{
						QuerySet: 0,
						Identity: 0,
						TS:       protocol.NewTimestamp(0),
					},
					EndVersion: &protocol.StateVersion{
						QuerySet: 1,
						Identity: 0,
						TS:       protocol.NewTimestamp(1),
					},
					Modifications: []protocol.StateModification{
						protocol.NewStateModificationQueryUpdated(query.QueryID, payload, nil),
					},
				}
				bytes, err := protocol.EncodeServerMessage(transition)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
					return
				}
			}
		}
	}))
}

func newMismatchedTransitionServer(t *testing.T) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sync" {
			http.NotFound(w, r)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			message, err := protocol.DecodeClientMessage(data)
			if err != nil {
				continue
			}
			if message.Type != "ModifyQuerySet" {
				continue
			}
			for _, mod := range message.Modifications {
				query, ok := mod.Query()
				if !ok {
					continue
				}
				payload, err := json.Marshal(NewValue(map[string]any{"source": "server", "query": query.UDFPath}))
				if err != nil {
					t.Fatalf("marshal failed: %v", err)
				}
				good := protocol.ServerMessage{
					Type:         "Transition",
					StartVersion: &protocol.StateVersion{QuerySet: 0, Identity: 0, TS: protocol.NewTimestamp(0)},
					EndVersion:   &protocol.StateVersion{QuerySet: 1, Identity: 0, TS: protocol.NewTimestamp(1)},
					Modifications: []protocol.StateModification{
						protocol.NewStateModificationQueryUpdated(query.QueryID, payload, nil),
					},
				}
				goodBytes, err := protocol.EncodeServerMessage(good)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, goodBytes); err != nil {
					return
				}

				bad := protocol.ServerMessage{
					Type:         "Transition",
					StartVersion: &protocol.StateVersion{QuerySet: 9, Identity: 0, TS: protocol.NewTimestamp(9)},
					EndVersion:   &protocol.StateVersion{QuerySet: 10, Identity: 0, TS: protocol.NewTimestamp(10)},
					Modifications: []protocol.StateModification{
						protocol.NewStateModificationQueryRemoved(query.QueryID),
					},
				}
				badBytes, err := protocol.EncodeServerMessage(bad)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, badBytes); err != nil {
					return
				}
			}
		}
	}))
}

func newChunkedTransitionServer(t *testing.T, outOfOrderFirstConnection bool) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	var mu sync.Mutex
	connections := 0

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sync" {
			http.NotFound(w, r)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connections++
		connectionIndex := connections
		mu.Unlock()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			message, err := protocol.DecodeClientMessage(data)
			if err != nil || message.Type != "ModifyQuerySet" {
				continue
			}

			for _, mod := range message.Modifications {
				query, ok := mod.Query()
				if !ok {
					continue
				}

				encodedTransition, err := encodeTransitionForQuery(query.QueryID, query.UDFPath)
				if err != nil {
					t.Fatalf("encode transition failed: %v", err)
				}
				chunks := splitChunks(encodedTransition, 3)
				if len(chunks) != 3 {
					t.Fatalf("expected 3 chunks, got %d", len(chunks))
				}

				order := []uint32{0, 1, 2}
				if outOfOrderFirstConnection && connectionIndex == 1 {
					order = []uint32{1, 0, 2}
				}

				for _, part := range order {
					chunkMessage := protocol.ServerMessage{
						Type:         "TransitionChunk",
						Chunk:        chunks[part],
						PartNumber:   part,
						TotalParts:   uint32(len(chunks)),
						TransitionID: "transition-1",
					}
					payload, err := protocol.EncodeServerMessage(chunkMessage)
					if err != nil {
						t.Fatalf("encode chunk failed: %v", err)
					}
					if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
						return
					}
				}
			}
		}
	}))
}

type replayOrderServer struct {
	*httptest.Server
	mu                 sync.Mutex
	secondConnMessages []string
}

func newReplayOrderServer(t *testing.T) *replayOrderServer {
	t.Helper()

	upgrader := websocket.Upgrader{}
	srv := &replayOrderServer{}
	var mu sync.Mutex
	connections := 0

	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sync" {
			http.NotFound(w, r)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		mu.Lock()
		connections++
		connectionIndex := connections
		mu.Unlock()

		var (
			lastQueryID protocol.QueryID
			hasQuery    bool
		)

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			message, err := protocol.DecodeClientMessage(data)
			if err != nil {
				continue
			}

			if connectionIndex == 2 && message.Type != "Connect" {
				srv.mu.Lock()
				srv.secondConnMessages = append(srv.secondConnMessages, message.Type)
				srv.mu.Unlock()
			}

			switch message.Type {
			case "ModifyQuerySet":
				for _, mod := range message.Modifications {
					query, ok := mod.Query()
					if !ok {
						continue
					}
					lastQueryID = query.QueryID
					hasQuery = true
					payload, err := json.Marshal(NewValue(map[string]any{"source": "server", "query": query.UDFPath}))
					if err != nil {
						t.Fatalf("marshal failed: %v", err)
					}
					transition := protocol.ServerMessage{
						Type:         "Transition",
						StartVersion: &protocol.StateVersion{QuerySet: 0, Identity: 0, TS: protocol.NewTimestamp(0)},
						EndVersion:   &protocol.StateVersion{QuerySet: 1, Identity: 0, TS: protocol.NewTimestamp(1)},
						Modifications: []protocol.StateModification{
							protocol.NewStateModificationQueryUpdated(query.QueryID, payload, nil),
						},
					}
					encoded, err := protocol.EncodeServerMessage(transition)
					if err != nil {
						t.Fatalf("encode failed: %v", err)
					}
					if err := conn.WriteMessage(websocket.TextMessage, encoded); err != nil {
						return
					}
				}
			case "Mutation":
				if connectionIndex == 1 {
					_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "force reconnect"), time.Now().Add(time.Second))
					return
				}

				success := true
				result, err := json.Marshal(NewValue(map[string]any{"ok": true}))
				if err != nil {
					t.Fatalf("marshal failed: %v", err)
				}
				response := protocol.ServerMessage{
					Type:      "MutationResponse",
					RequestID: message.RequestID,
					Success:   &success,
					Result:    result,
					TS:        protocol.EncodeTimestamp(2),
				}
				responseBytes, err := protocol.EncodeServerMessage(response)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, responseBytes); err != nil {
					return
				}
				transition := protocol.ServerMessage{
					Type:          "Transition",
					StartVersion:  &protocol.StateVersion{QuerySet: 1, Identity: 0, TS: protocol.NewTimestamp(1)},
					EndVersion:    &protocol.StateVersion{QuerySet: 1, Identity: 0, TS: protocol.NewTimestamp(2)},
					Modifications: []protocol.StateModification{},
				}
				if hasQuery {
					payload, err := json.Marshal(NewValue(map[string]any{"source": "server", "query": "test:query"}))
					if err != nil {
						t.Fatalf("marshal failed: %v", err)
					}
					transition.Modifications = append(transition.Modifications, protocol.NewStateModificationQueryUpdated(lastQueryID, payload, nil))
				}
				transitionBytes, err := protocol.EncodeServerMessage(transition)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, transitionBytes); err != nil {
					return
				}
			}
		}
	}))

	return srv
}

func newBlackholeSyncTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sync" {
			http.NotFound(w, r)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if _, err := protocol.DecodeClientMessage(data); err != nil {
				continue
			}
		}
	}))
}

func (s *replayOrderServer) secondConnectionReplay() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.secondConnMessages))
	copy(out, s.secondConnMessages)
	return out
}

func encodeTransitionForQuery(queryID protocol.QueryID, path string) (string, error) {
	payload, err := json.Marshal(NewValue(map[string]any{"source": "server", "query": path}))
	if err != nil {
		return "", err
	}
	transition := protocol.ServerMessage{
		Type: "Transition",
		StartVersion: &protocol.StateVersion{
			QuerySet: 0,
			Identity: 0,
			TS:       protocol.NewTimestamp(0),
		},
		EndVersion: &protocol.StateVersion{
			QuerySet: 1,
			Identity: 0,
			TS:       protocol.NewTimestamp(1),
		},
		Modifications: []protocol.StateModification{
			protocol.NewStateModificationQueryUpdated(queryID, payload, nil),
		},
	}
	encoded, err := protocol.EncodeServerMessage(transition)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func splitChunks(payload string, parts int) []string {
	if parts <= 1 || len(payload) == 0 {
		return []string{payload}
	}

	step := len(payload) / parts
	if step == 0 {
		return []string{payload}
	}

	chunks := make([]string, 0, parts)
	start := 0
	for i := 0; i < parts-1; i++ {
		end := start + step
		chunks = append(chunks, payload[start:end])
		start = end
	}
	chunks = append(chunks, payload[start:])
	return chunks
}

func newSyncTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/sync" {
			http.NotFound(w, r)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			message, err := protocol.DecodeClientMessage(data)
			if err != nil {
				continue
			}

			switch message.Type {
			case "Connect":
			case "Authenticate":
			case "ModifyQuerySet":
				for _, mod := range message.Modifications {
					query, ok := mod.Query()
					if !ok {
						continue
					}

					payload, err := json.Marshal(NewValue(map[string]any{
						"source": "server",
						"query":  query.UDFPath,
					}))
					if err != nil {
						t.Fatalf("marshal failed: %v", err)
					}
					transition := protocol.ServerMessage{
						Type: "Transition",
						StartVersion: &protocol.StateVersion{
							QuerySet: 0,
							Identity: 0,
							TS:       protocol.NewTimestamp(0),
						},
						EndVersion: &protocol.StateVersion{
							QuerySet: 1,
							Identity: 0,
							TS:       protocol.NewTimestamp(1),
						},
						Modifications: []protocol.StateModification{
							protocol.NewStateModificationQueryUpdated(query.QueryID, payload, nil),
						},
					}
					bytes, err := protocol.EncodeServerMessage(transition)
					if err != nil {
						t.Fatalf("encode failed: %v", err)
					}
					if err := conn.WriteMessage(websocket.TextMessage, bytes); err != nil {
						return
					}
				}
			case "Mutation":
				success := true
				result, err := json.Marshal(NewValue(map[string]any{"ok": true}))
				if err != nil {
					t.Fatalf("marshal failed: %v", err)
				}

				response := protocol.ServerMessage{
					Type:      "MutationResponse",
					RequestID: message.RequestID,
					Success:   &success,
					Result:    result,
					TS:        protocol.EncodeTimestamp(2),
				}
				respBytes, err := protocol.EncodeServerMessage(response)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, respBytes); err != nil {
					return
				}

				transition := protocol.ServerMessage{
					Type:         "Transition",
					StartVersion: &protocol.StateVersion{TS: protocol.NewTimestamp(1)},
					EndVersion:   &protocol.StateVersion{TS: protocol.NewTimestamp(2)},
				}
				transitionBytes, err := protocol.EncodeServerMessage(transition)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, transitionBytes); err != nil {
					return
				}
			case "Action":
				success := true
				result, err := json.Marshal(NewValue(map[string]any{"ok": true}))
				if err != nil {
					t.Fatalf("marshal failed: %v", err)
				}

				response := protocol.ServerMessage{
					Type:      "ActionResponse",
					RequestID: message.RequestID,
					Success:   &success,
					Result:    result,
				}
				respBytes, err := protocol.EncodeServerMessage(response)
				if err != nil {
					t.Fatalf("encode failed: %v", err)
				}
				if err := conn.WriteMessage(websocket.TextMessage, respBytes); err != nil {
					return
				}
			}
		}
	}))
}
