package convex

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	syncproto "github.com/get-convex/convex-go/internal/sync"
)

// TestCloseDuringDialDoesNotResurrectClient pins: a dial that ENDS after
// Close() must not install connected state — the attempt result is the
// closed error and the client stays disconnected.
func TestCloseDuringDialDoesNotResurrectClient(t *testing.T) {
	t.Parallel()

	client := newClient()
	blockDial := make(chan struct{})

	client.mu.Lock()
	client.deploymentURL = "https://close-during-dial-test.convex.cloud"
	client.openFn = func(_ context.Context, _ *syncproto.WebSocketManager, _ syncproto.ReconnectRequest) (<-chan syncproto.ProtocolResponse, error) {
		<-blockDial
		ch := make(chan syncproto.ProtocolResponse, 1)
		return ch, nil
	}
	client.mu.Unlock()

	errs := make(chan error, 1)
	go func() { errs <- client.ensureConnected(context.Background()) }()
	time.Sleep(50 * time.Millisecond) // let the dial start and block

	client.Close()
	close(blockDial)

	select {
	case err := <-errs:
		if err == nil {
			t.Fatalf("dial completed after Close returned nil — client resurrected")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ensureConnected did not return after Close + dial end")
	}
	client.mu.Lock()
	connected := client.connected
	client.mu.Unlock()
	if connected {
		t.Fatalf("connected=true after Close-during-dial")
	}
}

// TestPanickingDialPublishesAttempt pins the deferred cleanup: a dial that
// panics must still publish the attempt (clear it, set err, close done) so
// waiters fail instead of parking forever.
func TestPanickingDialPublishesAttempt(t *testing.T) {
	t.Parallel()

	client := newClient()
	client.mu.Lock()
	client.deploymentURL = "https://panic-dial-test.convex.cloud"
	client.openFn = func(_ context.Context, _ *syncproto.WebSocketManager, _ syncproto.ReconnectRequest) (<-chan syncproto.ProtocolResponse, error) {
		panic("open exploded")
	}
	client.mu.Unlock()

	const n = 5
	errs := make(chan error, n+1)
	var wg sync.WaitGroup
	for i := 0; i < n+1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- client.ensureConnected(context.Background())
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		for i := 0; i < n+1; i++ {
			if err := <-errs; err == nil {
				t.Fatalf("caller %d got nil from a panicked dial", i)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("callers parked after a panicking dial — attempt never published")
	}
	client.Close()
}

// TestCancelledWaitersDuringReplayLeakNoGoroutines pins MED 2: cancelled
// callers arriving during a blocked replay must NOT leave goroutines parked
// on a replay Cond — with the channel-based replay signal there is no helper
// goroutine at all. No Close in the test: nothing unblocks a replay wait.
func TestCancelledWaitersDuringReplayLeakNoGoroutines(t *testing.T) {
	t.Parallel()

	client := newClient()
	client.mu.Lock()
	client.connected = true
	client.reconnecting = true            // replay in progress, never completes
	client.replayCh = make(chan struct{}) // as onProtocolFailure sets it
	client.mu.Unlock()

	// Steady the goroutine count first.
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	const n = 10
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		go func() { errs <- client.ensureConnected(ctx) }()
	}
	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("waiter %d error = %v, want context.Canceled", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d did not return on a cancelled ctx", i)
		}
	}

	// Leaked replay-wait goroutines keep the count above base. Retry briefly
	// so normal short-lived runtime goroutines can settle.
	flat := false
	for i := 0; i < 20; i++ {
		if runtime.NumGoroutine() <= base {
			flat = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !flat {
		t.Fatalf("goroutines: base=%d now=%d — cancelled replay waiters leaked goroutines", base, runtime.NumGoroutine())
	}
}

// TestDetachedDialSurvivesLeaderCancel pins MED 3: the dial is detached from
// the leader's ctx, so when the leader's ctx expires mid-dial a waiter with a
// LIVE ctx still gets SUCCESS from the same attempt (not the leader's error,
// and not a second dial).
func TestDetachedDialSurvivesLeaderCancel(t *testing.T) {
	t.Parallel()

	client := newClient()
	blockDial := make(chan struct{})
	var mu sync.Mutex
	opens := 0

	client.mu.Lock()
	client.deploymentURL = "https://detached-dial-test.convex.cloud"
	client.openFn = func(ctx context.Context, _ *syncproto.WebSocketManager, _ syncproto.ReconnectRequest) (<-chan syncproto.ProtocolResponse, error) {
		mu.Lock()
		opens++
		mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err() // the dial honours whatever ctx drives it
		case <-blockDial:
			ch := make(chan syncproto.ProtocolResponse, 1)
			return ch, nil
		}
	}
	client.mu.Unlock()

	// Leader with a short ctx.
	leaderErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		leaderErr <- client.ensureConnected(ctx)
	}()
	time.Sleep(50 * time.Millisecond) // leader is inside the dial

	// Waiter with a live ctx, joining the in-flight attempt.
	waiterErr := make(chan error, 1)
	go func() { waiterErr <- client.ensureConnected(context.Background()) }()
	time.Sleep(100 * time.Millisecond) // leader ctx expires here

	select {
	case err := <-leaderErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("leader error = %v, want context.DeadlineExceeded", err)
		}
	default:
		t.Fatalf("leader still blocked after its ctx expired")
	}

	// The attempt must survive the leader: unblock the dial, the waiter
	// succeeds on the SAME connection.
	close(blockDial)
	select {
	case err := <-waiterErr:
		if err != nil {
			t.Fatalf("waiter error = %v, want success from the surviving attempt", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waiter did not return after the dial unblocked")
	}
	mu.Lock()
	got := opens
	mu.Unlock()
	if got != 1 {
		t.Fatalf("dials = %d, want exactly 1 (leader cancel must not kill or restart the attempt)", got)
	}
	client.Close()
}
