package convex

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	syncproto "github.com/get-convex/convex-go/internal/sync"
)

// TestWaitersHonourCtxWhileDialBlocked pins the ctx contract of the
// connect single-flight: while ONE dial is in flight, waiters must select on
// their own ctx.Done() — not queue unboundedly behind the leader. Before the
// fix, waiters blocked on a plain mutex and a Convex outage with N
// deadline-bounded callers serialized them dial-by-dial (up to N * deadline).
func TestWaitersHonourCtxWhileDialBlocked(t *testing.T) {
	t.Parallel()

	client := newClient()
	blockDial := make(chan struct{})

	client.mu.Lock()
	client.deploymentURL = "https://ctx-waiter-test.convex.cloud"
	client.openFn = func(_ context.Context, _ *syncproto.WebSocketManager, _ syncproto.ReconnectRequest) (<-chan syncproto.ProtocolResponse, error) {
		<-blockDial
		ch := make(chan syncproto.ProtocolResponse, 1)
		return ch, nil
	}
	client.mu.Unlock()

	// Leader enters the dial and stays blocked there.
	leaderErr := make(chan error, 1)
	go func() { leaderErr <- client.ensureConnected(context.Background()) }()
	time.Sleep(50 * time.Millisecond)

	// Waiters carry a short deadline; none of them may dial.
	const n = 15
	errs := make(chan error, n)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			errs <- client.ensureConnected(ctx)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed > 900*time.Millisecond {
		t.Fatalf("waiters took %v to fail, want ~100ms ctx deadline", elapsed)
	}
	for i := 0; i < n; i++ {
		err := <-errs
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter %d error = %v, want context.DeadlineExceeded", i, err)
		}
	}

	// Unblock the leader; it must succeed with exactly one dial having been
	// attempted for the whole batch.
	close(blockDial)
	select {
	case err := <-leaderErr:
		if err != nil {
			t.Fatalf("leader ensureConnected: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("leader did not return after dial unblocked")
	}
	client.Close()
}

// TestDialErrorSharedWithWaiters pins failure sharing: when the single dial
// fails, EVERY concurrent waiter gets the error — no N serial re-dials during
// an outage.
func TestDialErrorSharedWithWaiters(t *testing.T) {
	t.Parallel()

	client := newClient()
	dialErr := errors.New("dial refused: deployment unreachable")

	var mu sync.Mutex
	opens := 0
	client.mu.Lock()
	client.deploymentURL = "https://shared-error-test.convex.cloud"
	client.openFn = func(_ context.Context, _ *syncproto.WebSocketManager, _ syncproto.ReconnectRequest) (<-chan syncproto.ProtocolResponse, error) {
		mu.Lock()
		opens++
		mu.Unlock()
		// Hold the dial open briefly so the whole batch piles up behind it.
		time.Sleep(100 * time.Millisecond)
		return nil, dialErr
	}
	client.mu.Unlock()

	const n = 8
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- client.ensureConnected(context.Background())
		}()
	}
	wg.Wait()

	mu.Lock()
	got := opens
	mu.Unlock()
	if got != 1 {
		t.Fatalf("failing dial attempts = %d, want exactly 1 (no serial re-dials)", got)
	}
	for i := 0; i < n; i++ {
		err := <-errs
		if !errors.Is(err, dialErr) {
			t.Fatalf("caller %d error = %v, want the shared dial error", i, err)
		}
	}
	client.Close()
}

// TestEnsureConnectedHonoursCtxWhileReplaying pins MED 3: a caller arriving
// while the reconnectLoop is mid-replay (connected, reconnecting) must honour
// its ctx instead of blocking on the replayDone cond until the loop finishes
// (which may never, while Convex stays down).
func TestEnsureConnectedHonoursCtxWhileReplaying(t *testing.T) {
	t.Parallel()

	client := newClient()

	client.mu.Lock()
	client.connected = true
	client.reconnecting = true // replay in progress, no broadcast coming
	client.mu.Unlock()

	errs := make(chan error, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		errs <- client.ensureConnected(ctx)
	}()

	select {
	case err := <-errs:
		elapsed := time.Since(start)
		if elapsed > 900*time.Millisecond {
			t.Fatalf("ensureConnected took %v, want ~100ms ctx deadline", elapsed)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ensureConnected error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("ensureConnected blocked past its ctx while replaying")
	}

	// Release the (pre-fix stuck / post-fix lingering helper) goroutine.
	client.Close()
}
