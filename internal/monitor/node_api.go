package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/ring"
)

type nodeRecordView struct {
	Key           string `json:"key"`
	VnodeId       string `json:"vnodeId"`
	Timestamp     int64  `json:"timestamp"`
	LastOperation string `json:"lastOperation"`
	LastOutcome   string `json:"lastOutcome"`
	Found         bool   `json:"found"`
	ReadCount     int    `json:"readCount"`
	WriteCount    int    `json:"writeCount"`
}

type nodeReadView struct {
	Key    string `json:"key"`
	Found  bool   `json:"found"`
	Value  string `json:"value,omitempty"`
	Base64 string `json:"base64,omitempty"`
}

type nodeWriteRequest struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Base64 string `json:"base64,omitempty"`
}

type nodeWriteView struct {
	Key       string `json:"key"`
	Ok        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

type nodeAPIError struct {
	Error string `json:"error"`
}

type keyNodeActivityView struct {
	NodeId        string `json:"nodeId"`
	VnodeId       string `json:"vnodeId"`
	LastOperation string `json:"lastOperation"`
	LastOutcome   string `json:"lastOutcome"`
	Timestamp     int64  `json:"timestamp"`
	Found         bool   `json:"found"`
	ReadCount     int    `json:"readCount"`
	WriteCount    int    `json:"writeCount"`
}

type keyPlacementNodeView struct {
	NodeId   string `json:"nodeId"`
	VnodeId  string `json:"vnodeId"`
	Position uint64 `json:"position"`
	Role     string `json:"role"`
}

type keyActivityView struct {
	Key             string                 `json:"key"`
	NodeCount       int                    `json:"nodeCount"`
	ReadCount       int                    `json:"readCount"`
	WriteCount      int                    `json:"writeCount"`
	LatestTimestamp int64                  `json:"latestTimestamp"`
	LastOperation   string                 `json:"lastOperation"`
	LastNodeId      string                 `json:"lastNodeId"`
	Nodes           []keyNodeActivityView  `json:"nodes"`
	Placement       []keyPlacementNodeView `json:"placement"`
}

func (a *App) nodeBaseURL(nodeId string) (string, error) {
	a.mu.RLock()
	node, ok := a.nodes[nodeId]
	a.mu.RUnlock()
	if !ok || strings.TrimSpace(node.NodeAddr) == "" {
		return "", fmt.Errorf("node %s has no known address", nodeId)
	}

	host, _, err := net.SplitHostPort(node.NodeAddr)
	if err != nil {
		return "", fmt.Errorf("invalid node address %q: %w", node.NodeAddr, err)
	}
	return fmt.Sprintf("http://%s:%d", host, a.cfg.NodeHTTPPort), nil
}

func (a *App) fetchNodeRecords(ctx context.Context, nodeId string) ([]nodeRecordView, error) {
	_ = ctx
	return a.nodeRecordsFromLogs(nodeId)
}

func (a *App) nodeRecordsFromLogs(nodeId string) ([]nodeRecordView, error) {
	a.mu.RLock()
	node, ok := a.nodes[nodeId]
	if !ok {
		a.mu.RUnlock()
		return nil, fmt.Errorf("node %s not found", nodeId)
	}

	records := make([]nodeRecordView, 0, len(node.Records))
	for _, record := range node.Records {
		records = append(records, nodeRecordView{
			Key:           record.Key,
			VnodeId:       record.VnodeId,
			Timestamp:     record.Timestamp,
			LastOperation: record.LastOperation,
			LastOutcome:   record.LastOutcome,
			Found:         record.Found,
			ReadCount:     record.ReadCount,
			WriteCount:    record.WriteCount,
		})
	}
	a.mu.RUnlock()

	sort.Slice(records, func(i, j int) bool {
		if records[i].Timestamp == records[j].Timestamp {
			return records[i].Key < records[j].Key
		}
		return records[i].Timestamp > records[j].Timestamp
	})
	return records, nil
}

func (a *App) readFromNode(ctx context.Context, nodeId, key string) (nodeReadView, error) {
	baseURL, err := a.nodeBaseURL(nodeId)
	if err != nil {
		return nodeReadView{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/records/"+url.PathEscape(key), nil)
	if err != nil {
		return nodeReadView{}, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nodeReadView{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return nodeReadView{}, decodeNodeAPIError(resp)
	}

	var result nodeReadView
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nodeReadView{}, err
	}
	return result, nil
}

func (a *App) writeToNode(ctx context.Context, nodeId string, payload nodeWriteRequest) (nodeWriteView, error) {
	baseURL, err := a.nodeBaseURL(nodeId)
	if err != nil {
		return nodeWriteView{}, err
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nodeWriteView{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+"/v1/records/"+url.PathEscape(payload.Key), bytes.NewReader(body))
	if err != nil {
		return nodeWriteView{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nodeWriteView{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nodeWriteView{}, decodeNodeAPIError(resp)
	}

	var result nodeWriteView
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nodeWriteView{}, err
	}
	return result, nil
}

func decodeNodeAPIError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var apiErr nodeAPIError
	if err := json.Unmarshal(body, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
		return fmt.Errorf("node API returned %d: %s", resp.StatusCode, apiErr.Error)
	}
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		trimmed = resp.Status
	}
	return fmt.Errorf("node API returned %d: %s", resp.StatusCode, trimmed)
}

func nodeRequestContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 5*time.Second)
}

func (a *App) aggregateClusterRecords(ctx context.Context) ([]keyActivityView, error) {
	_ = ctx
	return a.aggregateClusterRecordsFromLogs(), nil
}

func (a *App) aggregateClusterRecordsFromLogs() []keyActivityView {
	a.mu.RLock()
	nodeIds := make([]string, 0, len(a.nodes))
	for nodeId := range a.nodes {
		nodeIds = append(nodeIds, nodeId)
	}
	sort.Strings(nodeIds)
	activeRing, copies := a.activeRingLocked()

	grouped := make(map[string][]keyNodeActivityView)
	for _, nodeId := range nodeIds {
		node := a.nodes[nodeId]
		for _, record := range node.Records {
			grouped[record.Key] = append(grouped[record.Key], keyNodeActivityView{
				NodeId:        nodeId,
				VnodeId:       record.VnodeId,
				LastOperation: record.LastOperation,
				LastOutcome:   record.LastOutcome,
				Timestamp:     record.Timestamp,
				Found:         record.Found,
				ReadCount:     record.ReadCount,
				WriteCount:    record.WriteCount,
			})
		}
	}
	a.mu.RUnlock()

	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]keyActivityView, 0, len(keys))
	for _, key := range keys {
		nodes := grouped[key]
		sort.Slice(nodes, func(i, j int) bool {
			if nodes[i].Timestamp == nodes[j].Timestamp {
				return nodes[i].NodeId < nodes[j].NodeId
			}
			return nodes[i].Timestamp > nodes[j].Timestamp
		})

		view := keyActivityView{
			Key:       key,
			NodeCount: len(nodes),
			Nodes:     nodes,
		}
		for _, node := range nodes {
			view.ReadCount += node.ReadCount
			view.WriteCount += node.WriteCount
			if node.Timestamp >= view.LatestTimestamp {
				view.LatestTimestamp = node.Timestamp
				view.LastOperation = node.LastOperation
				view.LastNodeId = node.NodeId
			}
		}
		if activeRing != nil {
			for _, placement := range activeRing.ResponsibleNodes(key, copies) {
				role := "replica"
				if len(view.Placement) == 0 {
					role = "primary"
				}
				view.Placement = append(view.Placement, keyPlacementNodeView{
					NodeId:   placement.NodeId,
					VnodeId:  placement.Id,
					Position: placement.Position,
					Role:     role,
				})
			}
		}
		result = append(result, view)
	}

	return result
}

func (a *App) activeRingLocked() (*ring.Ring, int) {
	entries := make([]ring.VnodeEntry, 0)
	for _, node := range a.nodes {
		for _, vnode := range node.Vnodes {
			if !vnode.Active {
				continue
			}
			position, err := strconv.ParseUint(strings.TrimSpace(vnode.Position), 10, 64)
			if err != nil {
				continue
			}
			entries = append(entries, ring.VnodeEntry{
				Id:       vnode.VnodeId,
				Position: position,
				NodeId:   node.NodeId,
				Address:  node.NodeAddr,
			})
		}
	}
	if len(entries) == 0 {
		return nil, 0
	}
	r := ring.New()
	r.Rebuild(entries)
	copies := a.replicaCount + 1
	if copies < 1 {
		copies = 1
	}
	return r, copies
}
