package monitor

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (a *App) nextIdLocked() int64 {
	a.nextEventId++
	return a.nextEventId
}

func (a *App) setSourceConnected(connected bool, lastError string) {
	a.mu.Lock()
	a.source.Connected = connected
	a.source.LastError = lastError
	a.source.LastActivity = time.Now().UTC()
	a.mu.Unlock()
}

func (a *App) snapshotSource() SourceStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.source
}

func (a *App) Snapshot() DashboardState {
	a.mu.RLock()
	defer a.mu.RUnlock()

	nodeIds := make([]string, 0, len(a.nodes))
	for nodeId := range a.nodes {
		nodeIds = append(nodeIds, nodeId)
	}
	sort.Strings(nodeIds)

	transfers := a.snapshotTransfersLocked()
	nodeViews, ringNodes, summary := a.snapshotNodes(nodeIds, transfers)

	return DashboardState{
		GeneratedAt: time.Now().UTC(),
		Source:      a.source,
		Summary:     summary,
		Ring:        RingView{Nodes: ringNodes, Transfers: transfers},
		Nodes:       nodeViews,
		Transfers:   transfers,
		Feed:        append([]FeedEvent(nil), a.events...),
	}
}

func (a *App) snapshotNodes(nodeIds []string, transfers []transferState) ([]NodeView, []RingNodeView, SummaryView) {
	nodeViews := make([]NodeView, 0, len(nodeIds))
	ringNodes := make([]RingNodeView, 0, len(nodeIds))
	now := time.Now().UTC()

	summary := SummaryView{
		NodeCount:       len(nodeIds),
		ActiveTransfers: countActiveTransfers(transfers),
	}

	for index, nodeId := range nodeIds {
		node := a.nodes[nodeId]
		nodeView, ringNode := buildNodeSnapshot(node, transfers, now, index)
		nodeViews = append(nodeViews, nodeView)
		ringNodes = append(ringNodes, ringNode)

		if node.Status == statusOnline {
			summary.OnlineNodes++
		}
		if node.Status == statusSuspect {
			summary.SuspectNodes++
		}
		if node.Status == statusOffline {
			summary.OfflineNodes++
		}
		summary.ActiveVnodes += nodeView.ActiveVnodes
		summary.ObservedRecords += len(nodeView.Records)
	}

	return nodeViews, ringNodes, summary
}

func buildNodeSnapshot(node *nodeState, transfers []transferState, now time.Time, colorIndex int) (NodeView, RingNodeView) {
	vnodeViews := snapshotVnodes(node.Vnodes)
	recordViews := snapshotRecords(node.Records)
	nodeTransfers := snapshotNodeTransfers(transfers, node.NodeId)
	tags, action := deriveNodeTags(node, now, nodeTransfers)

	nodeView := NodeView{
		NodeId:        node.NodeId,
		NodeAddr:      node.NodeAddr,
		Source:        node.Source,
		Status:        node.Status,
		LastSeen:      node.LastSeen,
		LastEvent:     node.LastEvent,
		LastOutcome:   node.LastOutcome,
		LastError:     node.LastError,
		CurrentAction: action,
		Tags:          tags,
		PingSent:      node.PingSent,
		PingSucceeded: node.PingSucceeded,
		PingFailed:    node.PingFailed,
		PingConfirmed: node.PingConfirmed,
		ActiveVnodes:  countActive(vnodeViews),
		Vnodes:        vnodeViews,
		Records:       recordViews,
		Transfers:     nodeTransfers,
		RecentEvents:  append([]FeedEvent(nil), node.RecentEvents...),
	}

	ringNode := RingNodeView{
		NodeId:   node.NodeId,
		NodeAddr: node.NodeAddr,
		Status:   node.Status,
		Color:    colorForIndex(colorIndex),
		Vnodes:   vnodeViews,
	}

	return nodeView, ringNode
}

func countActiveTransfers(transfers []transferState) int {
	count := 0
	for _, transfer := range transfers {
		if transfer.Status != transferStatusComplete && transfer.Status != transferStatusRejected && transfer.Status != transferStatusFailed {
			count++
		}
	}
	return count
}

func snapshotVnodes(vnodes map[string]*vnodeState) []vnodeState {
	list := make([]vnodeState, 0, len(vnodes))
	for _, vnode := range vnodes {
		list = append(list, *vnode)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Angle < list[j].Angle
	})
	active := make([]vnodeState, 0, len(list))
	for _, vnode := range list {
		if vnode.Active {
			active = append(active, vnode)
		}
	}
	if len(active) > 0 {
		for i := range active {
			prev := active[(i-1+len(active))%len(active)]
			active[i].RangeStart = prev.Position
			active[i].RangeEnd = active[i].Position
		}
	}
	activeMap := make(map[string]vnodeState, len(active))
	for _, vnode := range active {
		activeMap[vnode.VnodeId] = vnode
	}
	for i := range list {
		if active, ok := activeMap[list[i].VnodeId]; ok {
			list[i].RangeStart = active.RangeStart
			list[i].RangeEnd = active.RangeEnd
		}
	}
	return list
}

func snapshotRecords(records map[string]*recordState) []recordState {
	list := make([]recordState, 0, len(records))
	for _, record := range records {
		list = append(list, *record)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].ObservedAt.Equal(list[j].ObservedAt) {
			return list[i].Key < list[j].Key
		}
		return list[i].ObservedAt.After(list[j].ObservedAt)
	})
	return list
}

func (a *App) snapshotTransfersLocked() []transferState {
	list := make([]transferState, 0, len(a.transfers))
	for _, transfer := range a.transfers {
		list = append(list, *transfer)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].UpdatedAt.After(list[j].UpdatedAt) })
	return list
}

func snapshotNodeTransfers(transfers []transferState, nodeId string) []transferState {
	list := make([]transferState, 0)
	for _, transfer := range transfers {
		if transfer.OwnerNodeId == nodeId || transfer.RequestorNodeId == nodeId {
			list = append(list, transfer)
		}
	}
	return list
}

func deriveNodeTags(node *nodeState, now time.Time, transfers []transferState) ([]string, string) {
	tags := make([]string, 0, 5)
	action := node.CurrentAction
	if node.Status == statusOnline {
		tags = append(tags, "online")
	}
	if node.Status == statusSuspect {
		tags = append(tags, "suspect")
		if action == "" || action == "idle" {
			action = "suspected"
		}
	}
	if node.Status == statusOffline {
		tags = append(tags, "offline")
	}
	if !node.LastTransmitAt.IsZero() && now.Sub(node.LastTransmitAt) <= activeWindow {
		tags = append(tags, "transmitting")
		if action == "" || action == "mesh visible" || action == "holding data" {
			action = "transmitting"
		}
	}
	if !node.LastPingAt.IsZero() && now.Sub(node.LastPingAt) <= activeWindow {
		tags = append(tags, "pinging")
		if action == "" {
			action = "pinging"
		}
	}
	if len(transfers) > 0 {
		transferring := false
		claiming := false
		for _, transfer := range transfers {
			if transfer.Status == transferStatusComplete || transfer.Status == transferStatusRejected || transfer.Status == transferStatusFailed {
				continue
			}
			if transfer.OwnerNodeId == node.NodeId {
				transferring = true
			}
			if transfer.RequestorNodeId == node.NodeId {
				claiming = true
			}
		}
		if transferring {
			tags = append(tags, "transferring ranges")
			action = "transferring ranges"
		}
		if claiming {
			tags = append(tags, "claiming ranges")
			action = "claiming ranges"
		}
	}
	if action == "" {
		action = "idle"
	}
	return dedupe(tags), action
}

func countActive(vnodes []vnodeState) int {
	count := 0
	for _, vnode := range vnodes {
		if vnode.Active {
			count++
		}
	}
	return count
}

func chooseAction(current, next string) string {
	if current == "" || current == "idle" || current == "mesh visible" || current == "holding data" {
		return next
	}
	return current
}

func dedupe(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func chooseFirst(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func positionToAngle(position string) float64 {
	if position == "" {
		return 0
	}
	value, err := strconv.ParseUint(position, 10, 64)
	if err != nil {
		return 0
	}
	return (float64(value) / float64(math.MaxUint64)) * 360.0
}

func colorForIndex(index int) string {
	palette := []string{"#2b6cb0", "#dd6b20", "#2f855a", "#b83280", "#805ad5", "#c05621", "#3182ce", "#0f766e"}
	return palette[index%len(palette)]
}
