package monitor

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"time"
)

var (
	//go:embed web/*
	webFS embed.FS
)

func (a *App) Start(ctx context.Context) {
	go a.runLogFollower(ctx)
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", a.handleState)
	mux.HandleFunc("/api/stream", a.handleStream)
	mux.HandleFunc("/api/debug/records", a.handleDebugRecords)
	mux.HandleFunc("/api/nodes/", a.handleNodeAPI)
	mux.HandleFunc("/api/ops/nodes/", a.handleNodeLifecycleAPI)
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.Handle("/", noCache(http.FileServer(http.FS(staticWebFS()))))
	return mux
}

func staticWebFS() fs.FS {
	staticFS, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}
	return staticFS
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (a *App) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "source": a.snapshotSource()})
}

func (a *App) handleState(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(a.Snapshot())
}

func (a *App) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan FeedEvent, 32)
	a.registerSubscriber(ch)
	defer a.unregisterSubscriber(ch)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	initial, _ := json.Marshal(map[string]any{"type": "snapshot", "state": a.Snapshot()})
	_, _ = fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", initial)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			payload, _ := json.Marshal(event)
			_, _ = fmt.Fprintf(w, "event: event\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-ticker.C:
			_, _ = io.WriteString(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func (a *App) handleNodeAPI(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/nodes/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	nodeId := parts[0]
	action := parts[1]

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")

	switch action {
	case "data":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		records, err := a.fetchNodeRecords(r.Context(), nodeId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(records)
	case "read":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := nodeRequestContext(r.Context())
		defer cancel()
		key := strings.TrimSpace(r.URL.Query().Get("key"))
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		result, err := a.readFromNode(ctx, nodeId, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	case "write":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := nodeRequestContext(r.Context())
		defer cancel()
		var payload nodeWriteRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(payload.Key) == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		result, err := a.writeToNode(ctx, nodeId, payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (a *App) handleDebugRecords(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	records, err := a.aggregateClusterRecords(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(records)
}

func (a *App) registerSubscriber(ch chan FeedEvent) {
	a.mu.Lock()
	a.subscribers[ch] = struct{}{}
	a.mu.Unlock()
}

func (a *App) unregisterSubscriber(ch chan FeedEvent) {
	a.mu.Lock()
	delete(a.subscribers, ch)
	close(ch)
	a.mu.Unlock()
}
