package convex

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	syncproto "github.com/get-convex/convex-go/internal/sync"
)

// TestConcurrentEnsureConnectedDialsOnce pins the single-flight contract of
// ensureConnected: N concurrent callers on a disconnected client must produce
// EXACTLY ONE dial. Before the connectMu single-flight, every racer passed the
// connected=false check and opened its own WebSocket (openFn counted >1), the
// second open silently replacing the first conn while its readLoop leaked.
func TestConcurrentEnsureConnectedDialsOnce(t *testing.T) {
	t.Parallel()

	client := newClient()
	var mu sync.Mutex
	opens := 0
	blockDial := make(chan struct{})

	client.mu.Lock()
	client.deploymentURL = "https://singleflight-test.convex.cloud"
	client.openFn = func(_ context.Context, _ *syncproto.WebSocketManager, _ syncproto.ReconnectRequest) (<-chan syncproto.ProtocolResponse, error) {
		mu.Lock()
		opens++
		mu.Unlock()
		<-blockDial // hold the winner inside the dial so the racers pile up
		ch := make(chan syncproto.ProtocolResponse, 1)
		return ch, nil
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

	// Let the racers reach the open path before the dial completes.
	time.Sleep(100 * time.Millisecond)
	close(blockDial)
	wg.Wait()

	mu.Lock()
	got := opens
	mu.Unlock()
	if got != 1 {
		t.Fatalf("concurrent ensureConnected dials = %d, want exactly 1", got)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("ensureConnected racer %d: %v", i, err)
		}
	}
	client.Close()
}

// TestEnsureConnectedAfterReconnectLoopCompletes: a caller arriving while the
// protocol-failure reconnectLoop is mid-replay (reconnecting=true, connected
// may be false) must NOT dial a second WebSocket — it waits for the loop's
// replayDone broadcast and returns.
func TestEnsureConnectedAfterReconnectLoopCompletes(t *testing.T) {
	t.Parallel()

	client := newClient()
	blockReconnect := make(chan struct{})

	client.mu.Lock()
	client.deploymentURL = "https://singleflight-test.convex.cloud"
	client.mu.Unlock()

	client.mu.Lock()
	client.connected = true
	client.reconnectFn = func(_ context.Context, _ syncproto.ReconnectRequest) error {
		<-blockReconnect
		return nil
	}
	client.mu.Unlock()

	client.onProtocolFailure(errors.New("test failure"))

	// reconnecting is now true with connected=false; a racing ensureConnected
	// must wait, not dial.
	errs := make(chan error, 1)
	go func() { errs <- client.ensureConnected(context.Background()) }()

	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errs:
		t.Fatalf("ensureConnected should wait for the reconnectLoop, got early return: %v", err)
	default:
	}

	close(blockReconnect)
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("ensureConnected after replay: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ensureConnected did not return after reconnect completed")
	}
	client.Close()
}

