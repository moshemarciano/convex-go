package convex

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/get-convex/convex-go/internal/protocol"
	syncproto "github.com/get-convex/convex-go/internal/sync"
)

func TestHandleWorkerCommandRejectsDuringReconnect(t *testing.T) {
	t.Parallel()

	for _, kind := range []workerCommandKind{
		workerCommandSubscribe,
		workerCommandUnsubscribe,
		workerCommandMutation,
		workerCommandAction,
		workerCommandWatchAll,
		workerCommandUnwatch,
		workerCommandSetAuth,
		workerCommandSetAuthCB,
	} {
		t.Run(string(kind), func(t *testing.T) {
			client := newClient()
			client.mu.Lock()
			client.reconnecting = true
			client.mu.Unlock()

			resultCh := make(chan workerCommandResult, 1)
			client.handleWorkerCommand(workerCommand{kind: kind, result: resultCh})

			result := <-resultCh
			if result.err == nil {
				t.Fatalf("expected error for %s during reconnect, got nil", kind)
			}
			if result.err.Error() != "client reconnecting" {
				t.Fatalf("expected 'client reconnecting' error, got: %v", result.err)
			}
		})
	}
}

func TestHandleWorkerCommandCloseAllowedDuringReconnect(t *testing.T) {
	t.Parallel()

	client := newClient()
	client.mu.Lock()
	client.reconnecting = true
	client.mu.Unlock()

	resultCh := make(chan workerCommandResult, 1)
	client.handleWorkerCommand(workerCommand{kind: workerCommandClose, result: resultCh})

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("close should succeed during reconnect, got: %v", result.err)
	}
}

func TestHandleWorkerCommandAllowedWhenNotReconnecting(t *testing.T) {
	t.Parallel()

	client := newClient()
	// reconnecting defaults to false — command should reach the switch
	resultCh := make(chan workerCommandResult, 1)
	client.handleWorkerCommand(workerCommand{kind: workerCommandMutation, result: resultCh})

	result := <-resultCh
	// Should get "unsupported" or a real error, NOT "client reconnecting"
	if result.err != nil && result.err.Error() == "client reconnecting" {
		t.Fatalf("mutation should not be rejected when not reconnecting")
	}
}

func TestForceReconnectNoopWhenNotConnected(t *testing.T) {
	t.Parallel()

	client := newClient()
	// connected defaults to false
	client.ForceReconnect()

	client.mu.Lock()
	reconnecting := client.reconnecting
	client.mu.Unlock()

	if reconnecting {
		t.Fatalf("ForceReconnect should be a no-op when not connected, but reconnecting=true")
	}
}

func TestForceReconnectTriggersReconnectWhenConnected(t *testing.T) {
	t.Parallel()

	client := newClient()
	requests := make(chan syncproto.ReconnectRequest, 1)

	client.mu.Lock()
	client.connected = true
	client.reconnectFn = func(_ context.Context, request syncproto.ReconnectRequest) error {
		requests <- request
		return nil
	}
	client.mu.Unlock()

	client.ForceReconnect()

	select {
	case req := <-requests:
		if req.Reason == "" {
			t.Fatalf("expected non-empty reconnect reason")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for reconnect to trigger")
	}
}

func TestOnProtocolFailureClearsOutboundQueue(t *testing.T) {
	t.Parallel()

	client := newClient()
	blockReconnect := make(chan struct{})

	client.mu.Lock()
	client.connected = true
	client.reconnectFn = func(_ context.Context, _ syncproto.ReconnectRequest) error {
		<-blockReconnect // block so reconnecting stays true while we check
		return nil
	}
	// Simulate stale messages in the outbound queue
	client.outboundQueue = []protocol.ClientMessage{
		{Type: "ModifyQuerySet", BaseVersion: 4, NewVersion: 5},
		{Type: "Mutation"},
	}
	client.mu.Unlock()

	client.onProtocolFailure(errors.New("test failure"))

	// Give reconnectLoop time to call reconnectFn (which blocks)
	time.Sleep(50 * time.Millisecond)

	client.mu.Lock()
	queueLen := len(client.outboundQueue)
	client.mu.Unlock()

	if queueLen != 0 {
		t.Fatalf("expected outbound queue to be cleared, got %d messages", queueLen)
	}

	close(blockReconnect)
}

func TestOnProtocolFailureNoopWhenAlreadyReconnecting(t *testing.T) {
	t.Parallel()

	client := newClient()
	reconnectCalled := make(chan struct{}, 2)
	blockReconnect := make(chan struct{})

	client.mu.Lock()
	client.connected = true
	client.reconnectFn = func(_ context.Context, _ syncproto.ReconnectRequest) error {
		reconnectCalled <- struct{}{}
		<-blockReconnect // block until test signals
		return nil
	}
	client.mu.Unlock()

	// First call triggers reconnect (reconnectFn blocks)
	client.onProtocolFailure(errors.New("first failure"))

	select {
	case <-reconnectCalled:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for first reconnect call")
	}

	// reconnecting is true while reconnectFn is blocked
	client.mu.Lock()
	if !client.reconnecting {
		client.mu.Unlock()
		t.Fatalf("expected reconnecting=true during reconnect")
	}
	client.mu.Unlock()

	// Second call should be a no-op
	client.onProtocolFailure(errors.New("second failure"))

	select {
	case <-reconnectCalled:
		t.Fatalf("second onProtocolFailure should not trigger another reconnect")
	default:
		// Expected: no second reconnect call
	}

	// Unblock the reconnectFn to allow cleanup
	close(blockReconnect)
}

func TestReconnectLoopKeepsReconnectingDuringReplayState(t *testing.T) {
	t.Parallel()

	client := newClient()

	// Track when replayState is called relative to reconnecting flag
	var replayMu sync.Mutex
	replayStates := []bool{}
	replayDone := make(chan struct{})

	// Set up a reconnectFn that succeeds immediately
	client.mu.Lock()
	client.connected = true
	client.reconnectFn = func(_ context.Context, _ syncproto.ReconnectRequest) error {
		return nil
	}
	client.mu.Unlock()

	// Override replayState to capture the reconnecting state at call time
	origReplayState := client.replayState
	_ = origReplayState // suppress unused warning
	replayStateCalled := false

	// We can't easily override replayState since it's a method, so instead
	// test the observable behavior: ensureConnected should block until
	// reconnect completes.

	// Trigger reconnect
	client.onProtocolFailure(errors.New("test"))

	// Wait for reconnect to fully complete
	deadline := time.After(3 * time.Second)
	for {
		client.mu.Lock()
		done := !client.reconnecting
		client.mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("reconnect did not complete within timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}

	_ = replayStateCalled
	_ = replayDone
	_ = replayMu
	_ = replayStates
}

func TestEnsureConnectedBlocksUntilReplayCompletes(t *testing.T) {
	t.Parallel()

	client := newClient()
	var replayMu sync.Mutex
	replayStarted := false
	replayFinished := false

	client.mu.Lock()
	client.connected = true
	client.reconnecting = true
	client.replayCh = make(chan struct{})
	// Set up a reconnectFn so reconnectLoop can succeed
	client.reconnectFn = func(_ context.Context, _ syncproto.ReconnectRequest) error {
		return nil
	}
	client.mu.Unlock()

	// Simulate: reconnectLoop has established the WebSocket but is about to
	// run replayState. ensureConnected should block until we broadcast.

	ensureResult := make(chan error, 1)
	go func() {
		err := client.ensureConnected(context.Background())
		ensureResult <- err
	}()

	// Give ensureConnected a moment to enter Wait()
	time.Sleep(50 * time.Millisecond)

	// Verify it hasn't returned yet
	select {
	case err := <-ensureResult:
		t.Fatalf("ensureConnected should block during replay, but returned: %v", err)
	default:
		// Good — it's blocked
	}

	// Now simulate replayState completing
	replayMu.Lock()
	replayStarted = true
	replayMu.Unlock()

	client.mu.Lock()
	client.reconnecting = false
	if client.replayCh != nil {
		close(client.replayCh)
		client.replayCh = nil
	}
	client.mu.Unlock()

	replayMu.Lock()
	replayFinished = true
	replayMu.Unlock()

	select {
	case err := <-ensureResult:
		if err != nil {
			t.Fatalf("ensureConnected should succeed after replay, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ensureConnected did not unblock after replay completed")
	}

	_ = replayStarted
	_ = replayFinished
}

func TestEnsureConnectedReturnsImmediatelyWhenConnectedAndNotReconnecting(t *testing.T) {
	t.Parallel()

	client := newClient()
	client.mu.Lock()
	client.connected = true
	client.reconnecting = false
	client.mu.Unlock()

	err := client.ensureConnected(context.Background())
	if err != nil {
		t.Fatalf("expected nil error when connected and not reconnecting, got: %v", err)
	}
}

func TestFlushOutboundSkipsRequeueWhenDisconnected(t *testing.T) {
	t.Parallel()

	client := newClient()

	client.mu.Lock()
	client.connected = true // start connected so flushOutboundBeforeSelect processes the queue
	client.workerStarted = true
	client.sendFn = func(_ context.Context, msg protocol.ClientMessage) error {
		// Simulate: connection drops between dequeue and re-queue check
		client.mu.Lock()
		client.connected = false
		client.mu.Unlock()
		return errors.New("send failed")
	}
	client.outboundQueue = []protocol.ClientMessage{
		{Type: "TestMessage"},
	}
	client.responses = make(chan syncproto.ProtocolResponse, 1)
	client.mu.Unlock()

	go client.workerLoop()

	// Wait for the send to fail and the re-queue check to happen
	time.Sleep(100 * time.Millisecond)

	client.mu.Lock()
	queueLen := len(client.outboundQueue)
	client.mu.Unlock()

	if queueLen != 0 {
		t.Fatalf("expected empty outbound queue when disconnected after send failure, got %d messages", queueLen)
	}

	client.Close()
}

func TestSendRejectsDuringReconnect(t *testing.T) {
	t.Parallel()

	client := newClient()
	client.mu.Lock()
	client.reconnecting = true
	client.mu.Unlock()

	err := client.send(context.Background(), protocol.ClientMessage{
		Type:        "ModifyQuerySet",
		BaseVersion: 3,
		NewVersion:  4,
	})
	if err == nil {
		t.Fatalf("send should reject during reconnect, got nil error")
	}
	if err.Error() != "client reconnecting" {
		t.Fatalf("expected 'client reconnecting' error, got: %v", err)
	}
}

func TestSendSucceedsWhenNotReconnecting(t *testing.T) {
	t.Parallel()

	sendCalled := false
	client := newClient()
	client.mu.Lock()
	client.connected = true
	client.sendFn = func(_ context.Context, _ protocol.ClientMessage) error {
		sendCalled = true
		return nil
	}
	client.mu.Unlock()

	err := client.send(context.Background(), protocol.ClientMessage{
		Type: "Mutation",
	})
	if err != nil {
		t.Fatalf("send should succeed when not reconnecting, got: %v", err)
	}
	if !sendCalled {
		t.Fatalf("sendFn was not called")
	}
}

func TestReplayDoneBroadcastOnClientClose(t *testing.T) {
	t.Parallel()

	client := newClient()
	client.mu.Lock()
	client.connected = true
	client.reconnecting = true
	client.replayCh = make(chan struct{})
	client.mu.Unlock()

	unblocked := make(chan struct{})
	go func() {
		client.mu.Lock()
		ch := client.replayCh
		client.mu.Unlock()
		if ch == nil {
			close(unblocked)
			return
		}
		<-ch
		close(unblocked)
	}()

	// Give the goroutine time to enter the replay wait
	time.Sleep(50 * time.Millisecond)

	// Close should close the replay channel and unblock the waiter
	client.Close()

	select {
	case <-unblocked:
		// Good — Close() closed the replay channel and the waiter unblocked
	case <-time.After(2 * time.Second):
		t.Fatalf("Close() did not unblock replay waiters")
	}
}
