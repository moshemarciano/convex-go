package convex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/get-convex/convex-go/internal/baseclient"
	"github.com/get-convex/convex-go/internal/protocol"
	syncproto "github.com/get-convex/convex-go/internal/sync"
)

type AuthTokenFetcher func(forceRefresh bool) (*string, error)

type queryRegistration struct {
	path string
	args map[string]any
}

type pendingRequest struct {
	kind          string
	message       protocol.ClientMessage
	response      chan FunctionResult
	visibleTS     uint64
	waitingOnTS   bool
	completed     bool
	lastError     error
	resolvedValue Value
}

type transitionChunkBuffer struct {
	totalParts uint32
	nextPart   uint32
	parts      map[uint32]string
}

type Client struct {
	mu         sync.Mutex
	replayDone *sync.Cond // signaled when reconnecting transitions to false

	closed        bool
	reconnecting  bool
	deploymentURL string
	wsURL         string
	clientID      string
	stateCallback  StateCallback
	failureCallback FailureCallback
	lastState     WebSocketState
	hasLastState  bool

	manager         *syncproto.WebSocketManager
	sendFn          func(context.Context, protocol.ClientMessage) error
	reconnectFn     func(context.Context, syncproto.ReconnectRequest) error
	responses       <-chan syncproto.ProtocolResponse
	workerStarted   bool
	workerCommands  chan workerCommand
	workerDone      chan struct{}
	flushWake       chan struct{}
	workerEventHook func(workerEvent)
	connected       bool

	state            *baseclient.LocalSyncState
	querySubs        map[int64]chan Value
	querySubscribers map[uint64]map[int64]struct{}
	queries          map[uint64]queryRegistration
	watchers         map[int64]chan map[int64]Value
	nextWatcherID    int64

	nextRequestID    protocol.RequestSequenceNumber
	pending          map[protocol.RequestSequenceNumber]*pendingRequest
	lastTransition   *protocol.StateVersion
	transitionChunks map[string]*transitionChunkBuffer

	authToken   *string
	authFetcher AuthTokenFetcher

	outboundQueue []protocol.ClientMessage
}

func newClient() *Client {
	c := &Client{
		state:            NewLocalState(),
		querySubs:        map[int64]chan Value{},
		querySubscribers: map[uint64]map[int64]struct{}{},
		queries:          map[uint64]queryRegistration{},
		watchers:         map[int64]chan map[int64]Value{},
		pending:          map[protocol.RequestSequenceNumber]*pendingRequest{},
		transitionChunks: map[string]*transitionChunkBuffer{},
		workerCommands:   make(chan workerCommand, 64),
		workerDone:       make(chan struct{}),
		flushWake:        make(chan struct{}, 1),
	}
	c.replayDone = sync.NewCond(&c.mu)
	return c
}

func NewClient() *Client {
	return NewClientBuilder().Build()
}

func NewLocalState() *baseclient.LocalSyncState {
	return baseclient.NewLocalSyncState()
}

func (c *Client) Clone() *Client {
	return c
}

func (c *Client) Subscribe(ctx context.Context, name string, args map[string]any) (*QuerySubscription, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	result, err := c.executeWorkerCommand(ctx, workerCommandSubscribe, workerSubscribePayload{name: name, args: copyMap(args)})
	if err != nil {
		return nil, err
	}
	subscribeResult, ok := result.(workerSubscribeResult)
	if !ok {
		return nil, fmt.Errorf("invalid subscribe worker result")
	}

	return &QuerySubscription{
		UpdatesCh: subscribeResult.updates,
		closeFn: func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = c.executeWorkerCommand(ctx, workerCommandUnsubscribe, workerUnsubscribePayload{subID: subscribeResult.subID})
		},
	}, nil
}

func (c *Client) Query(ctx context.Context, name string, args map[string]any) (FunctionResult, error) {
	sub, err := c.Subscribe(ctx, name, args)
	if err != nil {
		return Failure(err), err
	}
	defer sub.Close()

	select {
	case value := <-sub.Updates():
		return Success(value), nil
	case <-ctx.Done():
		return Failure(ctx.Err()), ctx.Err()
	}
}

func (c *Client) Mutation(ctx context.Context, name string, args map[string]any) (FunctionResult, error) {
	return c.runRequest(ctx, "mutation", name, args)
}

func (c *Client) Action(ctx context.Context, name string, args map[string]any) (FunctionResult, error) {
	return c.runRequest(ctx, "action", name, args)
}

func (c *Client) WatchAll() *QuerySetSubscription {
	c.mu.Lock()
	workerReady := c.workerStarted && !c.closed
	c.mu.Unlock()
	if !workerReady {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.nextWatcherID++
		watcherID := c.nextWatcherID
		updates := make(chan map[int64]Value, 8)
		updates <- c.snapshotLocked()
		c.watchers[watcherID] = updates

		return &QuerySetSubscription{
			UpdatesCh: updates,
			closeFn: func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				if ch, ok := c.watchers[watcherID]; ok {
					close(ch)
					delete(c.watchers, watcherID)
				}
			},
		}
	}

	resultAny, err := c.executeWorkerCommand(context.Background(), workerCommandWatchAll, nil)
	if err != nil {
		updates := make(chan map[int64]Value)
		close(updates)
		return &QuerySetSubscription{UpdatesCh: updates, closeFn: func() {}}
	}
	watchResult, ok := resultAny.(workerWatchAllResult)
	if !ok {
		updates := make(chan map[int64]Value)
		close(updates)
		return &QuerySetSubscription{UpdatesCh: updates, closeFn: func() {}}
	}

	return &QuerySetSubscription{
		UpdatesCh: watchResult.updates,
		closeFn: func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = c.executeWorkerCommand(ctx, workerCommandUnwatch, workerUnwatchPayload{watcherID: watchResult.watcherID})
		},
	}
}

func (c *Client) SetAuth(token *string) {
	c.mu.Lock()
	workerReady := c.workerStarted && !c.closed
	if !workerReady {
		c.setAuthTokenLocked(token)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = c.executeWorkerCommand(ctx, workerCommandSetAuth, workerSetAuthPayload{token: token})
}

func (c *Client) SetAuthCallback(fetcher AuthTokenFetcher) error {
	if fetcher == nil {
		return fmt.Errorf("auth callback is required")
	}

	token, err := fetcher(false)
	if err != nil {
		return err
	}

	c.mu.Lock()
	workerReady := c.workerStarted && !c.closed
	if !workerReady {
		c.authFetcher = fetcher
		c.setAuthTokenLocked(token)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.executeWorkerCommand(ctx, workerCommandSetAuthCB, workerSetAuthCallbackPayload{fetcher: fetcher, token: token})
	return err
}

// ForceReconnect forces the client to close its current WebSocket connection
// and reconnect immediately. This is useful when the process wakes from
// suspension (e.g., Fly.io Machine resume, VM unpaused) and the existing
// connection is known to be stale.
//
// Uses the SDK's built-in reconnect loop with exponential backoff (starting
// at 100ms), so reconnection typically completes in ~200ms.
//
// No-op if the client is not currently connected (connected=false), which
// prevents racing with ensureConnected during initial connection establishment.
func (c *Client) ForceReconnect() {
	c.mu.Lock()
	if !c.connected {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.onProtocolFailure(fmt.Errorf("ForceReconnect: stale connection after process wake"))
}

// IsConnected returns whether the client currently has an active WebSocket connection.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.replayDone.Broadcast()
	manager := c.manager
	workerStarted := c.workerStarted
	workerDone := c.workerDone
	for id, ch := range c.querySubs {
		close(ch)
		delete(c.querySubs, id)
	}
	for id, ch := range c.watchers {
		close(ch)
		delete(c.watchers, id)
	}
	for id, pending := range c.pending {
		pending.response <- Failure(errors.New("client closed"))
		close(pending.response)
		delete(c.pending, id)
	}
	c.mu.Unlock()

	if workerStarted {
		select {
		case c.workerCommands <- workerCommand{kind: workerCommandClose}:
		default:
		}
	}

	if manager != nil {
		_ = manager.Close()
	}
	if workerStarted {
		select {
		case <-workerDone:
		case <-time.After(time.Second):
		}
	}
	c.emitState(WebSocketStateDisconnected)
}

func (c *Client) unsubscribe(subID int64) {
	c.mu.Lock()
	queryID, removed, err := c.state.Unsubscribe(subID)
	if err != nil {
		c.mu.Unlock()
		return
	}
	if ch, ok := c.querySubs[subID]; ok {
		close(ch)
		delete(c.querySubs, subID)
	}
	if subs, ok := c.querySubscribers[queryID]; ok {
		delete(subs, subID)
		if len(subs) == 0 {
			delete(c.querySubscribers, queryID)
		}
	}
	c.broadcastWatchersLocked()
	c.mu.Unlock()

	if removed {
		msg, buildErr := c.buildModifyRemoveMessage(queryID)
		if buildErr == nil {
			_ = c.send(context.Background(), msg)
		}
	}
}

func (c *Client) ensureConnected(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("client closed")
	}
	if c.connected {
		if c.reconnecting {
			// WebSocket is up but replayState is still running.
			// Block until reconnectLoop broadcasts completion.
			c.replayDone.Wait()
			if c.reconnecting {
				return fmt.Errorf("client still reconnecting")
			}
			return nil
		}
		c.mu.Unlock()
		return nil
	}
	deploymentURL := c.deploymentURL
	clientID := c.clientID
	c.mu.Unlock()

	if deploymentURL == "" {
		return fmt.Errorf("deployment URL is required; use WithDeploymentURL")
	}
	c.emitState(WebSocketStateConnecting)
	wsURL, err := syncproto.DeploymentURLToWebSocketURL(deploymentURL)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.wsURL = wsURL
	if c.manager == nil {
		c.manager = syncproto.NewWebSocketManager(wsURL, clientID)
	}
	manager := c.manager
	c.mu.Unlock()

	responses, err := manager.Open(ctx, syncproto.ReconnectRequest{})
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.responses = responses
	c.connected = true
	shouldStart := !c.workerStarted
	if shouldStart {
		c.workerStarted = true
	}
	c.mu.Unlock()

	if shouldStart {
		go c.workerLoop()
	}

	c.emitState(WebSocketStateConnected)
	if c.authToken != nil {
		_ = c.sendAuthenticate(ctx, c.authToken)
	}
	return nil
}

func (c *Client) send(ctx context.Context, message protocol.ClientMessage) error {
	c.mu.Lock()
	if c.reconnecting {
		c.mu.Unlock()
		return fmt.Errorf("client reconnecting")
	}
	sendFn := c.sendFn
	manager := c.manager
	c.mu.Unlock()
	if sendFn != nil {
		return sendFn(ctx, message)
	}
	if manager == nil {
		return fmt.Errorf("client not connected")
	}
	return manager.Send(ctx, message)
}

func (c *Client) executeWorkerCommand(ctx context.Context, kind workerCommandKind, value any) (any, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	resultCh := make(chan workerCommandResult, 1)
	command := workerCommand{kind: kind, ctx: ctx, value: value, result: resultCh}

	select {
	case c.workerCommands <- command:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-resultCh:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) runRequest(ctx context.Context, kind string, name string, args map[string]any) (FunctionResult, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return Failure(err), err
	}
	commandKind := workerCommandMutation
	if kind == "action" {
		commandKind = workerCommandAction
	}

	resultAny, err := c.executeWorkerCommand(ctx, commandKind, workerRunRequestPayload{name: name, args: copyMap(args)})
	if err != nil {
		return Failure(err), err
	}
	requestResult, ok := resultAny.(workerRunRequestResult)
	if !ok {
		return Failure(fmt.Errorf("invalid %s worker result", kind)), fmt.Errorf("invalid %s worker result", kind)
	}

	select {
	case result := <-requestResult.response:
		if result.Err != nil {
			return result, result.Err
		}
		return result, nil
	case <-ctx.Done():
		cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _ = c.executeWorkerCommand(cancelCtx, workerCommandCancelReq, workerCancelRequestPayload{requestID: requestResult.requestID, err: ctx.Err()})
		cancel()
		return Failure(ctx.Err()), ctx.Err()
	}
}

func (c *Client) workerLoop() {
	defer close(c.workerDone)

	for {
		if err := c.flushOutboundBeforeSelect(); err != nil {
			c.onProtocolFailure(fmt.Errorf("flush outbound failed: %w", err))
			continue
		}

		c.mu.Lock()
		if c.closed {
			c.replayDone.Broadcast()
			c.mu.Unlock()
			return
		}
		responses := c.responses
		c.mu.Unlock()
		if responses == nil {
			select {
			case <-c.flushWake:
			case cmd := <-c.workerCommands:
				c.handleWorkerCommand(cmd)
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		select {
		case <-c.flushWake:
			continue
		case cmd := <-c.workerCommands:
			c.handleWorkerCommand(cmd)
		case response, ok := <-responses:
			if !ok {
				c.onProtocolFailure(errors.New("websocket response stream closed"))
				return
			}
			event := workerEventFromProtocolResponse(response)
			c.handleWorkerEvent(event)
		}
	}
}

func (c *Client) handleWorkerCommand(cmd workerCommand) {
	if cmd.cancelled() {
		cmd.resolve(nil, cmd.cancelErr())
		return
	}

	// During reconnect, replayState() rebuilds the query set from scratch.
	// Allowing Subscribe/Mutation commands to run here would enqueue stale
	// ModifyQuerySet messages with wrong BaseVersion, causing FatalError.
	if cmd.kind != workerCommandClose {
		c.mu.Lock()
		recon := c.reconnecting
		c.mu.Unlock()
		if recon {
			cmd.resolve(nil, fmt.Errorf("client reconnecting"))
			return
		}
	}

	switch cmd.kind {
	case workerCommandSubscribe:
		c.handleWorkerSubscribe(cmd)
	case workerCommandUnsubscribe:
		c.handleWorkerUnsubscribe(cmd)
	case workerCommandMutation:
		c.handleWorkerRunRequest(cmd, "mutation")
	case workerCommandAction:
		c.handleWorkerRunRequest(cmd, "action")
	case workerCommandCancelReq:
		c.handleWorkerCancelRequest(cmd)
	case workerCommandWatchAll:
		c.handleWorkerWatchAll(cmd)
	case workerCommandUnwatch:
		c.handleWorkerUnwatch(cmd)
	case workerCommandSetAuth:
		c.handleWorkerSetAuth(cmd)
	case workerCommandSetAuthCB:
		c.handleWorkerSetAuthCallback(cmd)
	case workerCommandClose:
		cmd.resolve(nil, nil)
	default:
		cmd.resolve(nil, fmt.Errorf("unsupported worker command %q", cmd.kind))
	}
}

func (c *Client) handleWorkerSubscribe(cmd workerCommand) {
	payload, ok := cmd.value.(workerSubscribePayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid subscribe payload"))
		return
	}

	c.mu.Lock()
	queryID, subID, added, err := c.state.Subscribe(payload.name, payload.args)
	if err != nil {
		c.mu.Unlock()
		cmd.resolve(nil, err)
		return
	}

	updates := make(chan Value, 16)
	c.querySubs[subID] = updates
	if c.querySubscribers[queryID] == nil {
		c.querySubscribers[queryID] = map[int64]struct{}{}
	}
	c.querySubscribers[queryID][subID] = struct{}{}
	c.queries[queryID] = queryRegistration{path: payload.name, args: copyMap(payload.args)}

	if value, ok := c.snapshotForSubscriberLocked(subID); ok {
		updates <- value
	}
	c.mu.Unlock()

	if added {
		msg, err := c.buildModifyAddMessage(queryID, payload.name, payload.args)
		if err != nil {
			cmd.resolve(nil, err)
			return
		}
		c.enqueueOutbound(msg)
	}

	cmd.resolve(workerSubscribeResult{subID: subID, updates: updates}, nil)
}

func (c *Client) handleWorkerUnsubscribe(cmd workerCommand) {
	payload, ok := cmd.value.(workerUnsubscribePayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid unsubscribe payload"))
		return
	}
	c.unsubscribe(payload.subID)
	cmd.resolve(nil, nil)
}

func (c *Client) handleWorkerRunRequest(cmd workerCommand, kind string) {
	payload, ok := cmd.value.(workerRunRequestPayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid %s payload", kind))
		return
	}

	argsRaw, err := marshalFunctionArgs(payload.args)
	if err != nil {
		cmd.resolve(nil, err)
		return
	}

	c.mu.Lock()
	c.nextRequestID++
	requestID := c.nextRequestID
	message := protocol.ClientMessage{
		Type:      map[bool]string{true: "Mutation", false: "Action"}[kind == "mutation"],
		RequestID: requestID,
		UDFPath:   payload.name,
		Args:      argsRaw,
	}
	response := make(chan FunctionResult, 1)
	c.pending[requestID] = &pendingRequest{kind: kind, message: message, response: response}
	c.mu.Unlock()

	c.enqueueOutbound(message)
	cmd.resolve(workerRunRequestResult{requestID: requestID, response: response}, nil)
}

func (c *Client) handleWorkerCancelRequest(cmd workerCommand) {
	payload, ok := cmd.value.(workerCancelRequestPayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid cancel request payload"))
		return
	}

	c.mu.Lock()
	pending, ok := c.pending[payload.requestID]
	if ok {
		pending.response <- Failure(payload.err)
		close(pending.response)
		delete(c.pending, payload.requestID)
	}
	c.mu.Unlock()

	cmd.resolve(nil, nil)
}

func (c *Client) handleWorkerWatchAll(cmd workerCommand) {
	c.mu.Lock()
	c.nextWatcherID++
	watcherID := c.nextWatcherID
	updates := make(chan map[int64]Value, 8)
	updates <- c.snapshotLocked()
	c.watchers[watcherID] = updates
	c.mu.Unlock()

	cmd.resolve(workerWatchAllResult{watcherID: watcherID, updates: updates}, nil)
}

func (c *Client) handleWorkerUnwatch(cmd workerCommand) {
	payload, ok := cmd.value.(workerUnwatchPayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid unwatch payload"))
		return
	}

	c.mu.Lock()
	if ch, ok := c.watchers[payload.watcherID]; ok {
		close(ch)
		delete(c.watchers, payload.watcherID)
	}
	c.mu.Unlock()

	cmd.resolve(nil, nil)
}

func (c *Client) handleWorkerSetAuth(cmd workerCommand) {
	payload, ok := cmd.value.(workerSetAuthPayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid set auth payload"))
		return
	}

	c.mu.Lock()
	c.setAuthTokenLocked(payload.token)
	connected := c.connected
	c.mu.Unlock()

	if connected {
		message, err := c.buildAuthenticateMessage(payload.token)
		if err != nil {
			cmd.resolve(nil, err)
			return
		}
		c.enqueueOutbound(message)
	}

	cmd.resolve(nil, nil)
}

func (c *Client) handleWorkerSetAuthCallback(cmd workerCommand) {
	payload, ok := cmd.value.(workerSetAuthCallbackPayload)
	if !ok {
		cmd.resolve(nil, fmt.Errorf("invalid set auth callback payload"))
		return
	}

	c.mu.Lock()
	c.authFetcher = payload.fetcher
	c.setAuthTokenLocked(payload.token)
	connected := c.connected
	c.mu.Unlock()

	if connected {
		message, err := c.buildAuthenticateMessage(payload.token)
		if err != nil {
			cmd.resolve(nil, err)
			return
		}
		c.enqueueOutbound(message)
	}

	cmd.resolve(nil, nil)
}

func (c *Client) handleWorkerEvent(event workerEvent) {
	c.mu.Lock()
	hook := c.workerEventHook
	c.mu.Unlock()
	if hook != nil {
		hook(event)
	}

	switch event.kind {
	case workerEventTransportErr:
		c.onProtocolFailure(event.err)
	case workerEventTransportMsg:
		if event.message != nil {
			c.handleServerMessage(*event.message)
		}
	case workerEventTransportDone:
		c.onProtocolFailure(errors.New("websocket response stream closed"))
	}
}

func (c *Client) enqueueOutbound(message protocol.ClientMessage) {
	c.mu.Lock()
	c.outboundQueue = append(c.outboundQueue, message)
	c.mu.Unlock()
	select {
	case c.flushWake <- struct{}{}:
	default:
	}
}

func (c *Client) flushOutboundBeforeSelect() error {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil
		}
		if len(c.outboundQueue) == 0 {
			c.mu.Unlock()
			return nil
		}
		if !c.connected {
			c.mu.Unlock()
			return nil
		}
		message := c.outboundQueue[0]
		c.outboundQueue = c.outboundQueue[1:]
		c.mu.Unlock()

		if err := c.sendWhileCommunicating(message); err != nil {
			c.mu.Lock()
			if c.connected {
				// Still connected — re-queue for retry
				c.outboundQueue = append([]protocol.ClientMessage{message}, c.outboundQueue...)
			}
			// If !connected, onProtocolFailure already cleared the queue.
			// replayState() will re-enqueue what's needed after reconnect.
			c.mu.Unlock()
			return err
		}
	}
}

func (c *Client) sendWhileCommunicating(message protocol.ClientMessage) error {
	done := make(chan error, 1)
	go func() {
		done <- c.send(context.Background(), message)
	}()

	for {
		c.mu.Lock()
		responses := c.responses
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return fmt.Errorf("client closed")
		}

		select {
		case err := <-done:
			return err
		case <-c.flushWake:
		case cmd := <-c.workerCommands:
			c.handleWorkerCommand(cmd)
		case response, ok := <-responses:
			if !ok {
				closeErr := errors.New("websocket response stream closed")
				c.handleWorkerEvent(workerEvent{kind: workerEventTransportDone, err: closeErr})
				return closeErr
			}
			c.handleWorkerEvent(workerEventFromProtocolResponse(response))
		}
	}
}

func (c *Client) handleServerMessage(message protocol.ServerMessage) {
	switch message.Type {
	case "Transition":
		c.handleTransition(message)
	case "MutationResponse":
		c.handleMutationResponse(message)
	case "ActionResponse":
		c.handleActionResponse(message)
	case "Ping":
		return
	case "AuthError":
		c.onProtocolFailure(fmt.Errorf("auth error: %s", message.Error))
	case "FatalError":
		c.onProtocolFailure(fmt.Errorf("fatal error: %s", message.Error))
	case "TransitionChunk":
		c.handleTransitionChunk(message)
	default:
		c.onProtocolFailure(fmt.Errorf("unknown server message type: %s", message.Type))
	}
}

func (c *Client) handleTransitionChunk(message protocol.ServerMessage) {
	c.mu.Lock()
	buffer, ok := c.transitionChunks[message.TransitionID]
	if !ok {
		buffer = &transitionChunkBuffer{
			totalParts: message.TotalParts,
			nextPart:   0,
			parts:      make(map[uint32]string, message.TotalParts),
		}
		c.transitionChunks[message.TransitionID] = buffer
	}
	if buffer.totalParts != message.TotalParts {
		c.mu.Unlock()
		c.onProtocolFailure(fmt.Errorf("transition chunk totalParts mismatch for %q: got %d want %d", message.TransitionID, message.TotalParts, buffer.totalParts))
		return
	}
	if message.PartNumber != buffer.nextPart {
		c.mu.Unlock()
		c.onProtocolFailure(fmt.Errorf("transition chunk out of order for %q: got part %d want %d", message.TransitionID, message.PartNumber, buffer.nextPart))
		return
	}
	if _, exists := buffer.parts[message.PartNumber]; exists {
		c.mu.Unlock()
		c.onProtocolFailure(fmt.Errorf("transition chunk duplicate part %d for %q", message.PartNumber, message.TransitionID))
		return
	}

	buffer.parts[message.PartNumber] = message.Chunk
	buffer.nextPart++

	if uint32(len(buffer.parts)) != buffer.totalParts {
		c.mu.Unlock()
		return
	}

	assembled := assembleTransitionChunks(buffer)
	delete(c.transitionChunks, message.TransitionID)
	c.mu.Unlock()

	decoded, err := protocol.DecodeServerMessage([]byte(assembled))
	if err != nil {
		c.onProtocolFailure(fmt.Errorf("failed to decode assembled transition chunk %q: %w", message.TransitionID, err))
		return
	}
	if decoded.Type != "Transition" {
		c.onProtocolFailure(fmt.Errorf("assembled transition chunk %q decoded as %s", message.TransitionID, decoded.Type))
		return
	}
	c.handleTransition(decoded)
}

func assembleTransitionChunks(buffer *transitionChunkBuffer) string {
	var out strings.Builder
	for i := uint32(0); i < buffer.totalParts; i++ {
		out.WriteString(buffer.parts[i])
	}
	return out.String()
}

func (c *Client) handleTransition(message protocol.ServerMessage) {
	c.mu.Lock()
	if message.StartVersion == nil || message.EndVersion == nil {
		c.mu.Unlock()
		c.onProtocolFailure(fmt.Errorf("transition missing start/end version"))
		return
	}
	if c.lastTransition != nil && !stateVersionEqual(*c.lastTransition, *message.StartVersion) {
		expected := *c.lastTransition
		actual := *message.StartVersion
		c.mu.Unlock()
		c.onProtocolFailure(fmt.Errorf("transition start version mismatch: got (%d,%d,%d) want (%d,%d,%d)", actual.QuerySet, actual.Identity, actual.TS.Uint64(), expected.QuerySet, expected.Identity, expected.TS.Uint64()))
		return
	}
	c.state.UpdateObservedTimestamp(message.EndVersion.TS.Uint64())

	for _, modification := range message.Modifications {
		switch modification.Kind() {
		case "QueryUpdated":
			updated, ok := modification.QueryUpdated()
			if !ok {
				continue
			}
			var value Value
			if err := json.Unmarshal(updated.Value, &value); err != nil {
				continue
			}
			queryID := updated.QueryID.Uint64()
			c.state.SetQueryValue(queryID, value)
			for subID := range c.querySubscribers[queryID] {
				if ch, ok := c.querySubs[subID]; ok {
					nonBlockingSendValue(ch, value)
				}
			}
		case "QueryFailed":
			failed, ok := modification.QueryFailed()
			if !ok {
				continue
			}
			log.Printf("[CONVEX-SDK] query failed server-side: %s", failed.ErrorMessage)
			queryID := failed.QueryID.Uint64()
			for subID := range c.querySubscribers[queryID] {
				if ch, ok := c.querySubs[subID]; ok {
					nonBlockingSendValue(ch, NewNullValue())
				}
			}
		case "QueryRemoved":
			queryID, ok := modification.QueryRemoved()
			if !ok {
				continue
			}
			c.state.SetQueryValue(queryID.Uint64(), NewNullValue())
		}
	}
	endVersion := *message.EndVersion
	c.lastTransition = &endVersion

	c.resolveMutationVisibilityLocked()
	c.broadcastWatchersLocked()
	c.mu.Unlock()
}

func (c *Client) handleMutationResponse(message protocol.ServerMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending, ok := c.pending[message.RequestID]
	if !ok {
		return
	}
	if message.Success == nil {
		pending.response <- Failure(fmt.Errorf("mutation response missing success field"))
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	if !*message.Success {
		err := decodeFunctionError(message)
		pending.response <- Failure(err)
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	value, err := decodeResultValue(message.Result)
	if err != nil {
		pending.response <- Failure(err)
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	pending.resolvedValue = value
	if message.TS == "" {
		pending.response <- Success(value)
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	ts, err := protocol.DecodeTimestamp(message.TS)
	if err != nil {
		pending.response <- Failure(err)
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}
	// Observe the mutation's own timestamp so resolveMutationVisibilityLocked
	// can complete immediately without waiting for an external Transition.
	// This matches the official Rust SDK behavior (observe_timestamp in
	// MutationResponse handler).
	c.state.UpdateObservedTimestamp(ts)
	pending.visibleTS = ts
	pending.waitingOnTS = true
	c.resolveMutationVisibilityLocked()
}

func (c *Client) handleActionResponse(message protocol.ServerMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending, ok := c.pending[message.RequestID]
	if !ok {
		return
	}
	if message.Success == nil {
		pending.response <- Failure(fmt.Errorf("action response missing success field"))
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	if !*message.Success {
		err := decodeFunctionError(message)
		pending.response <- Failure(err)
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	value, err := decodeResultValue(message.Result)
	if err != nil {
		pending.response <- Failure(err)
		close(pending.response)
		delete(c.pending, message.RequestID)
		return
	}

	pending.response <- Success(value)
	close(pending.response)
	delete(c.pending, message.RequestID)
}

func (c *Client) onProtocolFailure(err error) {
	if err == nil {
		err = errors.New("unknown protocol failure")
	}

	c.mu.Lock()
	if c.closed || c.reconnecting {
		c.mu.Unlock()
		return
	}

	// Always log the failure reason — critical for debugging reconnect storms.
	log.Printf("[CONVEX-SDK] onProtocolFailure: %s (outbound=%d)", err.Error(), len(c.outboundQueue))

	c.connected = false
	c.reconnecting = true
	c.lastTransition = nil
	c.transitionChunks = map[string]*transitionChunkBuffer{}
	c.outboundQueue = nil
	observed := c.maxObservedTimestampLocked()
	callback := c.failureCallback
	c.mu.Unlock()

	if callback != nil {
		callback(err)
	}

	c.emitState(WebSocketStateReconnecting)

	go c.reconnectLoop(err.Error(), observed)
}

func (c *Client) reconnectLoop(reason string, observed uint64) {
	backoff := baseclient.NewBackoff(100*time.Millisecond, 15*time.Second, nil)

	for {
		c.mu.Lock()
		if c.closed {
			c.replayDone.Broadcast()
			c.mu.Unlock()
			return
		}
		manager := c.manager
		reconnectFn := c.reconnectFn
		fetcher := c.authFetcher
		c.mu.Unlock()

		if fetcher != nil {
			token, err := fetcher(true)
			if err != nil {
				time.Sleep(backoff.Next())
				continue
			}
			c.mu.Lock()
			c.setAuthTokenLocked(token)
			c.mu.Unlock()
		}

		if reconnectFn == nil {
			if manager == nil {
				time.Sleep(backoff.Next())
				continue
			}
			reconnectFn = manager.Reconnect
		}

		err := reconnectFn(context.Background(), syncproto.ReconnectRequest{
			Reason:               reason,
			MaxObservedTimestamp: observed,
		})
		if err != nil {
			time.Sleep(backoff.Next())
			continue
		}

		c.mu.Lock()
		c.connected = true
		// Keep reconnecting=true during replayState to prevent workerLoop from
		// enqueuing stale ModifyQuerySet messages with wrong BaseVersion.
		c.mu.Unlock()

		c.emitState(WebSocketStateConnected)

		_ = c.replayState()

		c.mu.Lock()
		c.reconnecting = false
		c.replayDone.Broadcast()
		c.mu.Unlock()
		return
	}
}

func (c *Client) replayState() error {
	c.mu.Lock()
	authToken := c.authToken
	queries := make(map[uint64]queryRegistration, len(c.queries))
	for id, query := range c.queries {
		queries[id] = query
	}
	pendingIDs := make([]protocol.RequestSequenceNumber, 0, len(c.pending))
	for requestID := range c.pending {
		pendingIDs = append(pendingIDs, requestID)
	}
	c.mu.Unlock()

	sort.Slice(pendingIDs, func(i, j int) bool {
		return pendingIDs[i] < pendingIDs[j]
	})
	pending := make([]protocol.ClientMessage, 0, len(pendingIDs))
	c.mu.Lock()
	for _, requestID := range pendingIDs {
		request, ok := c.pending[requestID]
		if !ok {
			continue
		}
		pending = append(pending, request.message)
	}
	c.mu.Unlock()

	authMessage, err := c.buildAuthenticateMessage(authToken)
	if err != nil {
		return err
	}
	c.enqueueOutbound(authMessage)

	if len(queries) > 0 {
		newVersion, err := protocol.QuerySetVersionFromUint64(c.state.QuerySetVersion())
		if err != nil {
			return err
		}
		msg := protocol.ClientMessage{
			Type:        "ModifyQuerySet",
			BaseVersion: 0,
			NewVersion:  newVersion.Uint32(),
		}
		queryIDs := make([]uint64, 0, len(queries))
		for queryID := range queries {
			queryIDs = append(queryIDs, queryID)
		}
		sort.Slice(queryIDs, func(i, j int) bool { return queryIDs[i] < queryIDs[j] })

		msg.Modifications = make([]protocol.QuerySetModification, 0, len(queries))
		for _, queryID := range queryIDs {
			query := queries[queryID]
			args, err := marshalFunctionArgs(query.args)
			if err != nil {
				return err
			}
			wireQueryID, err := protocol.QueryIDFromUint64(queryID)
			if err != nil {
				return err
			}
			msg.Modifications = append(msg.Modifications, protocol.NewQuerySetAdd(protocol.Query{
				QueryID: wireQueryID,
				UDFPath: query.path,
				Args:    args,
			}))
		}
		c.enqueueOutbound(msg)
	}

	for _, message := range pending {
		c.enqueueOutbound(message)
	}

	return nil
}

func (c *Client) buildModifyAddMessage(queryID uint64, path string, args map[string]any) (protocol.ClientMessage, error) {
	argsRaw, err := marshalFunctionArgs(args)
	if err != nil {
		return protocol.ClientMessage{}, err
	}
	version, err := protocol.QuerySetVersionFromUint64(c.state.QuerySetVersion())
	if err != nil {
		return protocol.ClientMessage{}, err
	}
	wireQueryID, err := protocol.QueryIDFromUint64(queryID)
	if err != nil {
		return protocol.ClientMessage{}, err
	}
	baseVersion := uint32(0)
	if version.Uint32() > 0 {
		baseVersion = version.Uint32() - 1
	}

	return protocol.ClientMessage{
		Type:        "ModifyQuerySet",
		BaseVersion: baseVersion,
		NewVersion:  version.Uint32(),
		Modifications: []protocol.QuerySetModification{protocol.NewQuerySetAdd(protocol.Query{
			QueryID: wireQueryID,
			UDFPath: path,
			Args:    argsRaw,
		})},
	}, nil
}

func (c *Client) buildModifyRemoveMessage(queryID uint64) (protocol.ClientMessage, error) {
	version, err := protocol.QuerySetVersionFromUint64(c.state.QuerySetVersion())
	if err != nil {
		return protocol.ClientMessage{}, err
	}
	wireQueryID, err := protocol.QueryIDFromUint64(queryID)
	if err != nil {
		return protocol.ClientMessage{}, err
	}
	baseVersion := uint32(0)
	if version.Uint32() > 0 {
		baseVersion = version.Uint32() - 1
	}
	delete(c.queries, queryID)
	return protocol.ClientMessage{
		Type:          "ModifyQuerySet",
		BaseVersion:   baseVersion,
		NewVersion:    version.Uint32(),
		Modifications: []protocol.QuerySetModification{protocol.NewQuerySetRemove(wireQueryID)},
	}, nil
}

func (c *Client) sendAuthenticate(ctx context.Context, token *string) error {
	message, err := c.buildAuthenticateMessage(token)
	if err != nil {
		return err
	}
	return c.send(ctx, message)
}

func (c *Client) buildAuthenticateMessage(token *string) (protocol.ClientMessage, error) {
	c.mu.Lock()
	version, err := protocol.IdentityVersionFromUint64(c.state.IdentityVersion())
	c.mu.Unlock()
	if err != nil {
		return protocol.ClientMessage{}, err
	}

	message := protocol.ClientMessage{Type: "Authenticate", BaseVersion: version.Uint32()}
	if token == nil {
		message.Token = protocol.NewNoAuthenticationToken()
	} else {
		message.Token = protocol.NewUserAuthenticationToken(*token)
	}
	return message, nil
}

func (c *Client) snapshotForSubscriberLocked(subID int64) (Value, bool) {
	snapshot := c.state.ResultsBySubscriber()
	value, ok := snapshot[subID]
	if !ok || value == nil {
		return Value{}, false
	}
	switch v := value.(type) {
	case Value:
		return v, true
	default:
		return NewValue(v), true
	}
}

func (c *Client) snapshotLocked() map[int64]Value {
	results := c.state.ResultsBySubscriber()
	out := make(map[int64]Value, len(results))
	for subID, raw := range results {
		switch value := raw.(type) {
		case Value:
			out[subID] = value
		case nil:
			out[subID] = NewNullValue()
		default:
			out[subID] = NewValue(value)
		}
	}
	return out
}

func (c *Client) broadcastWatchersLocked() {
	snapshot := c.snapshotLocked()
	for _, watcher := range c.watchers {
		nonBlockingSendSnapshot(watcher, snapshot)
	}
}

func (c *Client) resolveMutationVisibilityLocked() {
	observed := c.state.ObservedTimestamp()
	for id, request := range c.pending {
		if request.kind != "mutation" || request.completed || !request.waitingOnTS {
			continue
		}
		if observed >= request.visibleTS {
			request.completed = true
			request.response <- Success(request.resolvedValue)
			close(request.response)
			delete(c.pending, id)
		}
	}
}

func (c *Client) maxObservedTimestampLocked() uint64 {
	maxObserved := c.state.ObservedTimestamp()
	for _, request := range c.pending {
		if request.waitingOnTS && request.visibleTS > maxObserved {
			maxObserved = request.visibleTS
		}
	}
	return maxObserved
}

func decodeResultValue(raw json.RawMessage) (Value, error) {
	var value Value
	if len(raw) == 0 {
		return NewNullValue(), nil
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return Value{}, err
	}
	return value, nil
}

func decodeFunctionError(message protocol.ServerMessage) error {
	text := message.Error
	if len(message.Result) > 0 {
		var resultText string
		if err := json.Unmarshal(message.Result, &resultText); err == nil {
			text = resultText
		}
	}
	if len(message.ErrorData) > 0 {
		var data map[string]any
		if err := json.Unmarshal(message.ErrorData, &data); err == nil {
			return ConvexError{Message: text, Data: data}
		}
	}
	if text == "" {
		text = "operation failed"
	}
	return errors.New(text)
}

func marshalWireValue(value any) (json.RawMessage, error) {
	wrapped := NewValue(value)
	payload, err := json.Marshal(wrapped)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func marshalFunctionArgs(args map[string]any) (json.RawMessage, error) {
	if args == nil {
		args = map[string]any{}
	}
	return marshalWireValue([]any{args})
}

func copyMap(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func (c *Client) setAuthTokenLocked(token *string) {
	c.authToken = token
	c.state.SetAuthToken(token)
}

func (c *Client) emitState(state WebSocketState) {
	c.mu.Lock()
	if c.stateCallback == nil {
		c.mu.Unlock()
		return
	}
	if c.hasLastState && c.lastState == state {
		c.mu.Unlock()
		return
	}
	callback := c.stateCallback
	c.lastState = state
	c.hasLastState = true
	c.mu.Unlock()
	callback(state)
}

func nonBlockingSendValue(ch chan Value, value Value) {
	select {
	case ch <- value:
	default:
	}
}

func nonBlockingSendSnapshot(ch chan map[int64]Value, snapshot map[int64]Value) {
	copied := make(map[int64]Value, len(snapshot))
	for k, v := range snapshot {
		copied[k] = v
	}
	select {
	case ch <- copied:
	default:
	}
}

func stateVersionEqual(a, b protocol.StateVersion) bool {
	return a.QuerySet == b.QuerySet && a.Identity == b.Identity && a.TS == b.TS
}
