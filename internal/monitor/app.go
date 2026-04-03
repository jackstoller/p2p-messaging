package monitor

import (
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultShutdownTimeout = 5 * time.Second
	DefaultRetryDelay      = 2 * time.Second
	defaultTailLines       = 2000
	defaultNodeHTTPPort    = 8081
	maxFeedEvents          = 15000
	maxNodeEvents          = 250
	activeWindow           = 15 * time.Second

	statusOnline  = "online"
	statusSuspect = "suspect"
	statusOffline = "offline"

	transferStatusPlanned   = "planned"
	transferStatusAccepted  = "accepted"
	transferStatusStreaming = "streaming"
	transferStatusCutover   = "cutover"
	transferStatusActive    = "active"
	transferStatusComplete  = "complete"
	transferStatusFailed    = "failed"
	transferStatusRejected  = "rejected"
)

type Config struct {
	ProjectDir   string
	ComposeFile  string
	TailLines    int
	NodeHTTPPort int
}

type App struct {
	cfg Config

	mu sync.RWMutex

	source       SourceStatus
	nodes        map[string]*nodeState
	transfers    map[string]*transferState
	events       []FeedEvent
	subscribers  map[chan FeedEvent]struct{}
	nextEventId  int64
	replicaCount int

	execCommandContext func(context.Context, string, ...string) *exec.Cmd
}

type SourceStatus struct {
	Connected    bool      `json:"connected"`
	LastError    string    `json:"lastError,omitempty"`
	LastActivity time.Time `json:"lastActivity"`
}

type DashboardState struct {
	GeneratedAt time.Time       `json:"generatedAt"`
	Source      SourceStatus    `json:"source"`
	Summary     SummaryView     `json:"summary"`
	Ring        RingView        `json:"ring"`
	Nodes       []NodeView      `json:"nodes"`
	Transfers   []transferState `json:"transfers"`
	Feed        []FeedEvent     `json:"feed"`
}

type SummaryView struct {
	NodeCount       int `json:"nodeCount"`
	OnlineNodes     int `json:"onlineNodes"`
	SuspectNodes    int `json:"suspectNodes"`
	OfflineNodes    int `json:"offlineNodes"`
	ActiveVnodes    int `json:"activeVnodes"`
	ObservedRecords int `json:"observedRecords"`
	ActiveTransfers int `json:"activeTransfers"`
}

type RingView struct {
	Nodes     []RingNodeView  `json:"nodes"`
	Transfers []transferState `json:"transfers"`
}

type RingNodeView struct {
	NodeId   string       `json:"nodeId"`
	NodeAddr string       `json:"nodeAddr"`
	Status   string       `json:"status"`
	Color    string       `json:"color"`
	Vnodes   []vnodeState `json:"vnodes"`
}

type NodeView struct {
	NodeId        string          `json:"nodeId"`
	NodeAddr      string          `json:"nodeAddr"`
	Source        string          `json:"source"`
	Status        string          `json:"status"`
	LastSeen      time.Time       `json:"lastSeen"`
	LastEvent     string          `json:"lastEvent"`
	LastOutcome   string          `json:"lastOutcome"`
	LastError     string          `json:"lastError"`
	CurrentAction string          `json:"currentAction"`
	Tags          []string        `json:"tags"`
	PingSent      int             `json:"pingSent"`
	PingSucceeded int             `json:"pingSucceeded"`
	PingFailed    int             `json:"pingFailed"`
	PingConfirmed int             `json:"pingConfirmed"`
	ActiveVnodes  int             `json:"activeVnodes"`
	Vnodes        []vnodeState    `json:"vnodes"`
	Records       []recordState   `json:"records"`
	Transfers     []transferState `json:"transfers"`
	RecentEvents  []FeedEvent     `json:"recentEvents"`
}

type FeedEvent struct {
	Id      int64             `json:"id"`
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Event   string            `json:"event"`
	Outcome string            `json:"outcome,omitempty"`
	NodeId  string            `json:"nodeId,omitempty"`
	Source  string            `json:"source,omitempty"`
	Message string            `json:"message"`
	Raw     string            `json:"raw"`
	Kind    string            `json:"kind"`
	Fields  map[string]string `json:"fields,omitempty"`
}

type parsedLine struct {
	Source string
	Raw    string
	Fields map[string]string
	Time   time.Time
}

type vnodeState struct {
	NodeId     string  `json:"nodeId"`
	VnodeId    string  `json:"vnodeId"`
	Position   string  `json:"position"`
	Angle      float64 `json:"angle"`
	Active     bool    `json:"active"`
	RangeStart string  `json:"rangeStart,omitempty"`
	RangeEnd   string  `json:"rangeEnd,omitempty"`
}

type recordState struct {
	Key           string    `json:"key"`
	VnodeId       string    `json:"vnodeId"`
	Timestamp     int64     `json:"timestamp"`
	ObservedAt    time.Time `json:"observedAt"`
	LastOperation string    `json:"lastOperation"`
	LastOutcome   string    `json:"lastOutcome"`
	Found         bool      `json:"found"`
	ReadCount     int       `json:"readCount"`
	WriteCount    int       `json:"writeCount"`
}

type transferState struct {
	TransferId      string    `json:"transferId"`
	StreamId        string    `json:"streamId,omitempty"`
	OwnerNodeId     string    `json:"ownerNodeId,omitempty"`
	OwnerNodeAddr   string    `json:"ownerNodeAddr,omitempty"`
	RequestorNodeId string    `json:"requestorNodeId,omitempty"`
	RequestorAddr   string    `json:"requestorAddr,omitempty"`
	SourceVnodeId   string    `json:"sourceVnodeId,omitempty"`
	TargetVnodeId   string    `json:"targetVnodeId,omitempty"`
	RangeStart      string    `json:"rangeStart,omitempty"`
	RangeEnd        string    `json:"rangeEnd,omitempty"`
	Status          string    `json:"status"`
	UpdatedAt       time.Time `json:"updatedAt"`
	LastEvent       string    `json:"lastEvent,omitempty"`
	LastOutcome     string    `json:"lastOutcome,omitempty"`
	Error           string    `json:"error,omitempty"`
}

type nodeState struct {
	NodeId        string
	NodeAddr      string
	Source        string
	Status        string
	LastSeen      time.Time
	LastEvent     string
	LastOutcome   string
	LastError     string
	CurrentAction string

	PingSent       int
	PingSucceeded  int
	PingFailed     int
	PingConfirmed  int
	LastPingAt     time.Time
	LastTransmitAt time.Time

	Vnodes  map[string]*vnodeState
	Records map[string]*recordState

	RecentEvents []FeedEvent
}

func New(cfg Config) *App {
	projectDir := strings.TrimSpace(cfg.ProjectDir)
	if projectDir == "" {
		projectDir = "."
	}
	composeFile := strings.TrimSpace(cfg.ComposeFile)
	if composeFile == "" {
		composeFile = "docker-compose.yml"
	}
	tailLines := cfg.TailLines
	if tailLines <= 0 {
		tailLines = defaultTailLines
	}
	nodeHTTPPort := cfg.NodeHTTPPort
	if nodeHTTPPort <= 0 {
		nodeHTTPPort = defaultNodeHTTPPort
	}

	return &App{
		cfg: Config{
			ProjectDir:   projectDir,
			ComposeFile:  composeFile,
			TailLines:    tailLines,
			NodeHTTPPort: nodeHTTPPort,
		},
		source:             SourceStatus{LastActivity: time.Now().UTC()},
		nodes:              map[string]*nodeState{},
		transfers:          map[string]*transferState{},
		events:             []FeedEvent{},
		subscribers:        map[chan FeedEvent]struct{}{},
		execCommandContext: exec.CommandContext,
	}
}

func (a *App) applyParsedLine(parsed parsedLine) {
	now := parsed.Time
	if now.IsZero() {
		now = time.Now().UTC()
	}

	fields := parsed.Fields
	eventName := fields["event"]
	if eventName == "" {
		eventName = "raw.line"
	}
	outcome := strings.ToLower(strings.TrimSpace(fields["outcome"]))
	level := strings.ToLower(strings.TrimSpace(fields["level"]))
	if level == "" {
		level = "info"
	}
	nodeId := a.resolveNodeId(fields, parsed.Source)

	feedEvent := FeedEvent{
		Time:    now,
		Level:   level,
		Event:   eventName,
		Outcome: outcome,
		NodeId:  nodeId,
		Source:  parsed.Source,
		Raw:     parsed.Raw,
		Fields:  fields,
		Kind:    eventKind(eventName),
		Message: buildMessage(fields),
	}

	a.mu.Lock()
	node := a.ensureNodeLocked(nodeId, parsed.Source)
	a.applyNodeEventLocked(node, feedEvent)
	a.applyVnodeEventLocked(node, fields)
	a.applyRecordEventLocked(node, fields, now)
	a.applyTransferEventLocked(feedEvent)
	a.applyPeerStateEventLocked(eventName, outcome, fields, now)

	if shouldSuppressEvent(fields) {
		a.mu.Unlock()
		return
	}
	a.publishEventLocked(feedEvent)
	a.mu.Unlock()
}

func (a *App) applyUnparsedLine(line string, isError bool) {
	now := time.Now().UTC()
	level := "info"
	if isError {
		level = "warn"
	}
	feedEvent := FeedEvent{
		Time:    now,
		Level:   level,
		Event:   "raw.line",
		Source:  "docker",
		Raw:     line,
		Kind:    "mesh",
		Message: buildRawMessage(strings.TrimSpace(line), isError),
	}

	a.mu.Lock()
	a.publishEventLocked(feedEvent)
	a.mu.Unlock()
}

func (a *App) ensureNodeLocked(nodeId, source string) *nodeState {
	if nodeId == "" {
		nodeId = source
	}
	if nodeId == "" {
		nodeId = "unknown"
	}
	node, ok := a.nodes[nodeId]
	if !ok {
		node = &nodeState{
			NodeId:  nodeId,
			Source:  source,
			Status:  statusOffline,
			Vnodes:  map[string]*vnodeState{},
			Records: map[string]*recordState{},
		}
		a.nodes[nodeId] = node
	}
	if source != "" {
		node.Source = source
	}
	return node
}

func (a *App) applyNodeEventLocked(node *nodeState, event FeedEvent) {
	node.LastSeen = eventTime(event)
	node.LastEvent = event.Event
	node.LastOutcome = event.Outcome
	node.CurrentAction = chooseAction(node.CurrentAction, humanizeEventName(event.Event))
	if errText := event.Fields["err"]; errText != "" {
		node.LastError = errText
	}

	if addr := chooseFirst(event.Fields["node_addr"], event.Fields["peer_addr"]); addr != "" {
		node.NodeAddr = addr
	}

	if isTransmitEvent(event.Event) {
		node.LastTransmitAt = event.Time
	}
	if strings.Contains(event.Event, "ping") {
		node.LastPingAt = event.Time
		node.PingSent++
		if event.Outcome == "succeeded" {
			node.PingSucceeded++
		}
		if event.Outcome == "failed" {
			node.PingFailed++
		}
		if event.Event == "rpc.membership.confirm_suspect" && strings.EqualFold(event.Fields["confirmed"], "true") {
			node.PingConfirmed++
		}
	}

	if marksNodeOnline(event.Event, event.Outcome, event.Fields) {
		node.Status = statusOnline
	}
	if marksNodeOffline(event.Event, event.Outcome, event.Fields) {
		node.Status = statusOffline
	}
	if event.Event == "node.start" {
		if replicaCount := chooseInt(event.Fields["replica_count"], 0); replicaCount > a.replicaCount {
			a.replicaCount = replicaCount
		}
	}

	node.RecentEvents = appendTrimmedEvent(node.RecentEvents, event, maxNodeEvents)
}

func (a *App) applyVnodeEventLocked(node *nodeState, fields map[string]string) {
	vnodeId := chooseFirst(fields["vnode_id"], fields["target_vnode_id"])
	if vnodeId == "" {
		return
	}
	vnode := node.Vnodes[vnodeId]
	if vnode == nil {
		vnode = &vnodeState{NodeId: node.NodeId, VnodeId: vnodeId}
		node.Vnodes[vnodeId] = vnode
	}

	if pos := chooseFirst(fields["position"], fields["range_end"]); pos != "" {
		vnode.Position = pos
		vnode.Angle = positionToAngle(pos)
	}

	state := strings.ToUpper(strings.TrimSpace(fields["state"]))
	switch {
	case strings.Contains(state, "ACTIVE"):
		vnode.Active = true
	case strings.Contains(state, "INACTIVE"):
		vnode.Active = false
	}

	if start := fields["range_start"]; start != "" {
		vnode.RangeStart = start
	}
	if end := fields["range_end"]; end != "" {
		vnode.RangeEnd = end
	}
}

func (a *App) applyRecordEventLocked(node *nodeState, fields map[string]string, observedAt time.Time) {
	key := fields["key"]
	if key == "" {
		return
	}

	eventName := fields["event"]
	outcome := strings.ToLower(strings.TrimSpace(fields["outcome"]))
	op, ok := classifyDataOperation(eventName, outcome)
	if !ok {
		return
	}

	record := node.Records[key]
	if record == nil {
		record = &recordState{Key: key}
		node.Records[key] = record
	}
	record.ObservedAt = observedAt
	record.LastOperation = op
	record.LastOutcome = outcome
	record.VnodeId = chooseFirst(fields["vnode_id"], record.VnodeId)
	record.Timestamp = chooseInt64(fields["timestamp"], record.Timestamp)
	if op == "read" {
		record.ReadCount++
		record.Found = strings.EqualFold(fields["found"], "true")
		return
	}
	record.WriteCount++
}

func (a *App) applyPeerStateEventLocked(eventName, outcome string, fields map[string]string, now time.Time) {
	peerId := fields["peer_id"]
	if peerId == "" {
		peerId = fields["node_id"]
	}
	if peerId == "" {
		return
	}
	node := a.ensureNodeLocked(peerId, "")
	node.LastSeen = now
	if addr := fields["peer_addr"]; addr != "" {
		node.NodeAddr = addr
	}

	state := strings.ToUpper(strings.TrimSpace(fields["state"]))
	if strings.Contains(eventName, "heartbeat.suspect") && (outcome == "started" || outcome == "failed") {
		node.Status = statusSuspect
	}
	if strings.Contains(eventName, "declare_down") || strings.Contains(state, "PEER_DOWN") {
		node.Status = statusOffline
	}
	if strings.Contains(state, "PEER_UP") {
		node.Status = statusOnline
	}
}

func (a *App) applyTransferEventLocked(event FeedEvent) {
	transferId := chooseFirst(event.Fields["transfer_id"], event.Fields["stream_id"])
	if transferId == "" {
		return
	}
	transfer := a.transfers[transferId]
	if transfer == nil {
		transfer = &transferState{TransferId: transferId}
		a.transfers[transferId] = transfer
	}

	transfer.StreamId = chooseFirst(event.Fields["stream_id"], transfer.StreamId)
	transfer.OwnerNodeId = chooseFirst(event.Fields["owner_node_id"], transfer.OwnerNodeId)
	transfer.OwnerNodeAddr = chooseFirst(event.Fields["owner_addr"], transfer.OwnerNodeAddr)
	transfer.RequestorNodeId = chooseFirst(event.Fields["requestor_id"], transfer.RequestorNodeId)
	transfer.RequestorAddr = chooseFirst(event.Fields["requestor_addr"], transfer.RequestorAddr)
	transfer.SourceVnodeId = chooseFirst(event.Fields["source_vnode_id"], transfer.SourceVnodeId)
	transfer.TargetVnodeId = chooseFirst(event.Fields["target_vnode_id"], transfer.TargetVnodeId)
	transfer.RangeStart = chooseFirst(event.Fields["range_start"], transfer.RangeStart)
	transfer.RangeEnd = chooseFirst(event.Fields["range_end"], transfer.RangeEnd)
	transfer.UpdatedAt = eventTime(event)
	transfer.LastEvent = event.Event
	transfer.LastOutcome = event.Outcome
	transfer.Error = chooseFirst(event.Fields["err"], transfer.Error)

	status := transfer.Status
	switch {
	case event.Event == "transfer.owner.clear_completed":
		status = transferStatusComplete
	case event.Outcome == "failed":
		status = transferStatusFailed
	case event.Outcome == "rejected":
		status = transferStatusRejected
	case event.Event == "transfer.claim.plan" || event.Event == "transfer.claim.plan.compute":
		status = transferStatusPlanned
	case event.Event == "transfer.request_range" && event.Outcome == "succeeded":
		status = transferStatusAccepted
	case event.Event == "transfer.claim.snapshot" && event.Outcome == "succeeded":
		status = transferStatusStreaming
	case event.Event == "transfer.complete" && event.Outcome == "succeeded":
		status = transferStatusCutover
	case event.Event == "transfer.activate_range" && event.Outcome == "succeeded":
		status = transferStatusActive
	}
	transfer.Status = status
}

func (a *App) publishEventLocked(event FeedEvent) {
	event.Id = a.nextIdLocked()
	a.events = appendTrimmedEvent(a.events, event, maxFeedEvents)

	node := a.nodes[event.NodeId]
	if node != nil {
		node.RecentEvents = appendTrimmedEvent(node.RecentEvents, event, maxNodeEvents)
	}

	for ch := range a.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

func (a *App) resolveNodeId(fields map[string]string, source string) string {
	nodeId := strings.TrimSpace(fields["node_id"])
	if nodeId != "" {
		return nodeId
	}
	if source != "" {
		left := strings.TrimSpace(strings.SplitN(source, " ", 2)[0])
		if left != "" {
			return left
		}
	}
	if peerId := strings.TrimSpace(fields["peer_id"]); peerId != "" {
		return peerId
	}
	return ""
}

func marksNodeOnline(event, outcome string, fields map[string]string) bool {
	if strings.Contains(event, "node.start") && outcome != "failed" {
		return true
	}
	if strings.Contains(event, "grpc.listen") && outcome != "failed" {
		return true
	}
	if strings.EqualFold(fields["state"], "PEER_UP") {
		return true
	}
	if event == "membership.peer.upsert" && outcome == "succeeded" {
		return true
	}
	return false
}

func marksNodeOffline(event, outcome string, fields map[string]string) bool {
	if strings.Contains(event, "node.shutdown") {
		return true
	}
	if strings.EqualFold(fields["state"], "PEER_DOWN") {
		return true
	}
	if event == "heartbeat.declare_down" && outcome == "succeeded" {
		return true
	}
	if event == "membership.peer.down" && outcome == "succeeded" {
		return true
	}
	return false
}

func classifyDataOperation(eventName, outcome string) (string, bool) {
	switch eventName {
	case "rpc.data.write":
		return "write", outcome == "succeeded"
	case "rpc.data.read.local":
		return "read", outcome == "succeeded" || outcome == "skipped"
	case "rpc.data.read.remote":
		return "read", outcome == "succeeded"
	case "rpc.data.read":
		return "read", outcome == "skipped"
	default:
		return "", false
	}
}

func isTransmitEvent(eventName string) bool {
	prefixes := []string{"rpc.data.", "storage.record.", "transfer.", "replica."}
	for _, prefix := range prefixes {
		if strings.HasPrefix(eventName, prefix) {
			return true
		}
	}
	return false
}

func chooseInt64(raw string, fallback int64) int64 {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func chooseInt(raw string, fallback int) int {
	if strings.TrimSpace(raw) == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func (a *App) NodeIds() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	ids := make([]string, 0, len(a.nodes))
	for id := range a.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (a *App) String() string {
	return fmt.Sprintf("monitor{nodes=%d, events=%d}", len(a.nodes), len(a.events))
}
