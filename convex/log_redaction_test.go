package convex

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/get-convex/convex-go/internal/protocol"
	syncproto "github.com/get-convex/convex-go/internal/sync"
)

const serverSecretText = `args {"serviceToken":"SERVER-ECHOED-SECRET"}`

func captureStdLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	oldW, oldF := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(oldW); log.SetFlags(oldF) })
	return &buf
}

func TestQueryFailedLogHoldsNoServerText(t *testing.T) {
	buf := captureStdLog(t)
	client := newClient()
	failed := protocol.NewStateModificationQueryFailed(protocol.NewQueryID(1), serverSecretText, json.RawMessage(`{}`), nil)
	client.handleServerMessage(protocol.ServerMessage{
		Type:          "Transition",
		StartVersion:  &protocol.StateVersion{TS: protocol.NewTimestamp(0)},
		EndVersion:    &protocol.StateVersion{TS: protocol.NewTimestamp(1)},
		Modifications: []protocol.StateModification{failed},
	})
	out := buf.String()
	if !strings.Contains(out, "query failed") {
		t.Fatalf("QueryFailed not logged: %q", out)
	}
	if strings.Contains(out, "SERVER-ECHOED-SECRET") || strings.Contains(out, "serviceToken") {
		t.Fatalf("log holds server text: %q", out)
	}
}

func TestAuthAndFatalErrorsHoldNoServerText(t *testing.T) {
	for _, typ := range []string{"AuthError", "FatalError"} {
		t.Run(typ, func(t *testing.T) {
			buf := captureStdLog(t)
			client := newClient()
			requests := make(chan syncproto.ReconnectRequest, 1)
			failures := make(chan error, 1)
			client.mu.Lock()
			client.connected = true
			client.failureCallback = func(err error) { failures <- err }
			client.reconnectFn = func(_ context.Context, r syncproto.ReconnectRequest) error {
				requests <- r
				return nil
			}
			client.mu.Unlock()

			client.handleServerMessage(protocol.ServerMessage{Type: typ, Error: serverSecretText})

			select {
			case r := <-requests:
				if strings.Contains(r.Reason, "SERVER-ECHOED-SECRET") {
					t.Fatalf("reconnect reason holds server text: %q", r.Reason)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no reconnect")
			}
			if err := <-failures; strings.Contains(err.Error(), "SERVER-ECHOED-SECRET") {
				t.Fatalf("failure callback error holds server text: %v", err)
			}
			if out := buf.String(); !strings.Contains(out, "onProtocolFailure") || strings.Contains(out, "SERVER-ECHOED-SECRET") {
				t.Fatalf("log wrong: %q", out)
			}
		})
	}
}
