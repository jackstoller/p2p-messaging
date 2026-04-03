package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackstoller/p2p-messaging/internal/membership"
	meshserver "github.com/jackstoller/p2p-messaging/internal/server"
	"github.com/jackstoller/p2p-messaging/internal/storage"
	pb "github.com/jackstoller/p2p-messaging/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	authChallengeTTL     = 2 * time.Minute
	sessionTTL           = 30 * 24 * time.Hour
	presenceTTL          = 45 * time.Second
	maxRequestBytes      = 1 << 20
	maxMailboxSignals    = 128
	defaultPollTimeout   = 25 * time.Second
	maxPollTimeout       = 30 * time.Second
	nodeInfoRefreshTTL   = 60 * time.Second
	authTokenPrefix      = "Bearer "
	nodeHTTPRecordPrefix = "node-http-"
	userRecordPrefix     = "user-"
	sessionRecordPrefix  = "session-"
	mailboxRecordPrefix  = "mailbox-"
)

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{3,32}$`)

//go:embed web/*
var webFS embed.FS

type API struct {
	node             *meshserver.Server
	mgr              *membership.Manager
	store            *storage.Store
	nodeID           string
	nodeHTTPBaseURL  string
	pollersMu        sync.Mutex
	pollers          map[string]map[chan struct{}]struct{}
	challengesMu     sync.Mutex
	authChallenges   map[string]authChallenge
	httpClient       *http.Client
	nodeInfoMu       sync.Mutex
	lastNodeInfoPush time.Time
}

type authChallenge struct {
	Nonce     string `json:"nonce"`
	ExpiresAt int64  `json:"expiresAt"`
}

type writeRequest struct {
	Value  string `json:"value"`
	Base64 string `json:"base64"`
}

type writeResponse struct {
	Key       string `json:"key"`
	Ok        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

type readResponse struct {
	Key    string `json:"key"`
	Found  bool   `json:"found"`
	Value  string `json:"value,omitempty"`
	Base64 string `json:"base64,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type registerRequest struct {
	Username     string `json:"username"`
	AuthSalt     string `json:"authSalt"`
	AuthVerifier string `json:"authVerifier"`
}

type challengeResponse struct {
	Username   string `json:"username"`
	AuthSalt   string `json:"authSalt"`
	Nonce      string `json:"nonce"`
	ExpiresAt  int64  `json:"expiresAt"`
	SessionTTL int64  `json:"sessionTtlMs"`
}

type loginRequest struct {
	Username string `json:"username"`
	Proof    string `json:"proof"`
}

type authResponse struct {
	Username    string      `json:"username"`
	Session     string      `json:"session"`
	User        userView    `json:"user"`
	NodeBaseURL string      `json:"nodeBaseUrl"`
	Nodes       []nodeEntry `json:"nodes"`
}

type presenceRequest struct {
	NodeBaseURL string `json:"nodeBaseUrl"`
}

type startSessionRequest struct {
	Username string `json:"username"`
}

type signalRequest struct {
	To      string          `json:"to"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type internalSignalRequest struct {
	Signal signalEnvelope `json:"signal"`
}

type pollResponse struct {
	Signals []signalEnvelope `json:"signals"`
}

type sessionStateResponse struct {
	User        userView    `json:"user"`
	NodeBaseURL string      `json:"nodeBaseUrl"`
	Nodes       []nodeEntry `json:"nodes"`
}

type connectResponse struct {
	Target userView `json:"target"`
}

type userRecord struct {
	Username          string `json:"username"`
	AuthSalt          string `json:"authSalt"`
	AuthVerifier      string `json:"authVerifier"`
	PrimaryNodeID     string `json:"primaryNodeId"`
	CurrentNodeID     string `json:"currentNodeId"`
	CurrentNodeHTTP   string `json:"currentNodeHttp"`
	ActiveSessionID   string `json:"activeSessionId"`
	PresenceExpiresAt int64  `json:"presenceExpiresAt"`
	LastSeenAt        int64  `json:"lastSeenAt"`
	CreatedAt         int64  `json:"createdAt"`
	UpdatedAt         int64  `json:"updatedAt"`
	DeletedAt         int64  `json:"deletedAt"`
}

type sessionRecord struct {
	SessionID   string `json:"sessionId"`
	Username    string `json:"username"`
	SecretHash  string `json:"secretHash"`
	CreatedAt   int64  `json:"createdAt"`
	ExpiresAt   int64  `json:"expiresAt"`
	LastSeenAt  int64  `json:"lastSeenAt"`
	NodeID      string `json:"nodeId"`
	NodeHTTP    string `json:"nodeHttp"`
	RevokedAt   int64  `json:"revokedAt"`
	VerifierRef string `json:"verifierRef,omitempty"`
}

type signalEnvelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	From      string          `json:"from"`
	To        string          `json:"to"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt int64           `json:"createdAt"`
}

type mailboxRecord struct {
	Username string           `json:"username"`
	Signals  []signalEnvelope `json:"signals"`
}

type nodeHTTPRecord struct {
	NodeID      string `json:"nodeId"`
	BaseURL     string `json:"baseUrl"`
	LastUpdated int64  `json:"lastUpdated"`
}

type userView struct {
	Username        string `json:"username"`
	IsActive        bool   `json:"isActive"`
	LastSeenAt      int64  `json:"lastSeenAt"`
	CurrentNodeID   string `json:"currentNodeId,omitempty"`
	CurrentNodeHTTP string `json:"currentNodeHttp,omitempty"`
}

type nodeEntry struct {
	NodeID   string `json:"nodeId"`
	BaseURL  string `json:"baseUrl"`
	IsSelf   bool   `json:"isSelf"`
	IsActive bool   `json:"isActive"`
}

func New(node *meshserver.Server, mgr *membership.Manager, store *storage.Store, nodeID, httpAdvertise string) *API {
	return &API{
		node:            node,
		mgr:             mgr,
		store:           store,
		nodeID:          nodeID,
		nodeHTTPBaseURL: normalizeHTTPBaseURL(httpAdvertise),
		pollers:         make(map[string]map[chan struct{}]struct{}),
		authChallenges:  make(map[string]authChallenge),
		httpClient: &http.Client{
			Timeout: 4 * time.Second,
		},
	}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", a.handleIndex)
	mux.HandleFunc("/app.js", a.handleStatic("web/app.js", "application/javascript; charset=utf-8"))
	mux.HandleFunc("/styles.css", a.handleStatic("web/styles.css", "text/css; charset=utf-8"))
	mux.HandleFunc("/healthz", a.handleHealth)
	mux.HandleFunc("/v1/records/", a.handleRecord)
	mux.HandleFunc("/v1/auth/register", a.handleRegister)
	mux.HandleFunc("/v1/auth/challenge", a.handleChallenge)
	mux.HandleFunc("/v1/auth/login", a.handleLogin)
	mux.HandleFunc("/v1/me", a.handleSessionState)
	mux.HandleFunc("/v1/me/delete", a.handleDeleteAccount)
	mux.HandleFunc("/v1/presence/heartbeat", a.handlePresenceHeartbeat)
	mux.HandleFunc("/v1/presence/offline", a.handlePresenceOffline)
	mux.HandleFunc("/v1/peers/connect", a.handlePeerConnect)
	mux.HandleFunc("/v1/signals", a.handleSignal)
	mux.HandleFunc("/v1/events/poll", a.handlePoll)
	mux.HandleFunc("/v1/nodes", a.handleNodes)
	mux.HandleFunc("/v1/internal/signals/deliver", a.handleInternalSignal)
	return mux
}

func (a *API) PublishNodeInfo(ctx context.Context) error {
	a.nodeInfoMu.Lock()
	defer a.nodeInfoMu.Unlock()

	now := time.Now()
	if now.Sub(a.lastNodeInfoPush) < nodeInfoRefreshTTL {
		return nil
	}
	record := nodeHTTPRecord{
		NodeID:      a.nodeID,
		BaseURL:     a.nodeHTTPBaseURL,
		LastUpdated: now.UnixMilli(),
	}
	if err := a.writeJSONRecord(ctx, nodeHTTPRecordPrefix+a.nodeID, record); err != nil {
		return err
	}
	a.lastNodeInfoPush = now
	return nil
}

func (a *API) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	a.serveEmbeddedFile(w, "web/index.html", "text/html; charset=utf-8")
}

func (a *API) handleStatic(path, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		a.serveEmbeddedFile(w, path, contentType)
	}
}

func (a *API) serveEmbeddedFile(w http.ResponseWriter, path, contentType string) {
	data, err := webFS.ReadFile(path)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "asset not available")
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (a *API) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleRecord(w http.ResponseWriter, r *http.Request) {
	key, ok := recordKeyFromPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	switch r.Method {
	case http.MethodGet:
		a.handleRead(w, r, key)
	case http.MethodPut:
		a.handleWrite(w, r, key)
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (a *API) handleRead(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := a.node.Read(ctx, &pb.ReadRequest{Key: key})
	if err != nil {
		writeStatusError(w, err)
		return
	}
	if !resp.GetFound() {
		writeJSON(w, http.StatusNotFound, readResponse{Key: key, Found: false})
		return
	}

	value := resp.GetValue()
	writeJSON(w, http.StatusOK, readResponse{
		Key:    key,
		Found:  true,
		Value:  string(value),
		Base64: base64.StdEncoding.EncodeToString(value),
	})
}

func (a *API) handleWrite(w http.ResponseWriter, r *http.Request, key string) {
	value, err := decodeWriteValue(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := a.node.Write(ctx, &pb.WriteRequest{Key: key, Value: value})
	if err != nil {
		writeStatusError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, writeResponse{
		Key:       key,
		Ok:        resp.GetOk(),
		Timestamp: resp.GetTimestamp(),
	})
}

func (a *API) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := a.PublishNodeInfo(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "node metadata unavailable")
		return
	}

	var req registerRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	username := normalizeUsername(req.Username)
	if !usernamePattern.MatchString(username) {
		writeError(w, http.StatusBadRequest, "username must be 3-32 characters of letters, numbers, hyphen, or underscore")
		return
	}
	if req.AuthSalt == "" || req.AuthVerifier == "" {
		writeError(w, http.StatusBadRequest, "authSalt and authVerifier are required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	userKey := userRecordPrefix + username
	if _, found, err := a.readUserRecord(ctx, userKey); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to check username")
		return
	} else if found {
		writeError(w, http.StatusConflict, "username already exists")
		return
	}

	now := storage.NowMillis()
	user := userRecord{
		Username:          username,
		AuthSalt:          req.AuthSalt,
		AuthVerifier:      req.AuthVerifier,
		PrimaryNodeID:     a.nodeID,
		CurrentNodeID:     a.nodeID,
		CurrentNodeHTTP:   a.requestBaseURL(r),
		PresenceExpiresAt: now + int64(presenceTTL/time.Millisecond),
		LastSeenAt:        now,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	sessionToken, session, err := a.newSession(username, user.AuthVerifier, user.CurrentNodeHTTP)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create session")
		return
	}
	user.ActiveSessionID = session.SessionID

	if err := a.writeJSONRecord(ctx, userKey, user); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to save user")
		return
	}
	if err := a.writeJSONRecord(ctx, sessionRecordPrefix+session.SessionID, session); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to save session")
		return
	}

	writeJSON(w, http.StatusOK, authResponse{
		Username:    username,
		Session:     sessionToken,
		User:        a.userView(user),
		NodeBaseURL: user.CurrentNodeHTTP,
		Nodes:       a.collectNodeEntries(ctx),
	})
}

func (a *API) handleChallenge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	username := normalizeUsername(r.URL.Query().Get("username"))
	if !usernamePattern.MatchString(username) {
		writeError(w, http.StatusBadRequest, "invalid username")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	user, found, err := a.readUserRecord(ctx, userRecordPrefix+username)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to read user")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if user.DeletedAt > 0 {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	challenge := authChallenge{
		Nonce:     randomToken(24),
		ExpiresAt: time.Now().Add(authChallengeTTL).UnixMilli(),
	}
	a.challengesMu.Lock()
	a.authChallenges[username] = challenge
	a.challengesMu.Unlock()

	writeJSON(w, http.StatusOK, challengeResponse{
		Username:   username,
		AuthSalt:   user.AuthSalt,
		Nonce:      challenge.Nonce,
		ExpiresAt:  challenge.ExpiresAt,
		SessionTTL: int64(sessionTTL / time.Millisecond),
	})
}

func (a *API) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := a.PublishNodeInfo(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "node metadata unavailable")
		return
	}

	var req loginRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	username := normalizeUsername(req.Username)
	if req.Proof == "" || !usernamePattern.MatchString(username) {
		writeError(w, http.StatusBadRequest, "invalid login request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	user, found, err := a.readUserRecord(ctx, userRecordPrefix+username)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to read user")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if user.DeletedAt > 0 {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	a.challengesMu.Lock()
	challenge, ok := a.authChallenges[username]
	if ok && challenge.ExpiresAt < storage.NowMillis() {
		delete(a.authChallenges, username)
		ok = false
	}
	if ok {
		delete(a.authChallenges, username)
	}
	a.challengesMu.Unlock()
	if !ok {
		writeError(w, http.StatusUnauthorized, "login challenge missing or expired")
		return
	}

	expectedProof := computeChallengeProof(user.AuthVerifier, challenge.Nonce)
	if !secureEqual(expectedProof, req.Proof) {
		writeError(w, http.StatusUnauthorized, "invalid login proof")
		return
	}

	user.CurrentNodeID = a.nodeID
	user.CurrentNodeHTTP = a.requestBaseURL(r)
	user.LastSeenAt = storage.NowMillis()
	user.PresenceExpiresAt = user.LastSeenAt + int64(presenceTTL/time.Millisecond)
	user.UpdatedAt = user.LastSeenAt

	sessionToken, session, err := a.newSession(username, user.AuthVerifier, user.CurrentNodeHTTP)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create session")
		return
	}
	user.ActiveSessionID = session.SessionID

	if err := a.writeJSONRecord(ctx, userRecordPrefix+username, user); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to save user")
		return
	}
	if err := a.writeJSONRecord(ctx, sessionRecordPrefix+session.SessionID, session); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to save session")
		return
	}

	writeJSON(w, http.StatusOK, authResponse{
		Username:    username,
		Session:     sessionToken,
		User:        a.userView(user),
		NodeBaseURL: user.CurrentNodeHTTP,
		Nodes:       a.collectNodeEntries(ctx),
	})
}

func (a *API) handleSessionState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, session, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	a.touchSession(ctx, &user, &session, a.requestBaseURL(r))
	writeJSON(w, http.StatusOK, sessionStateResponse{
		User:        a.userView(user),
		NodeBaseURL: a.requestBaseURL(r),
		Nodes:       a.collectNodeEntries(ctx),
	})
}

func (a *API) handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, session, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}

	now := storage.NowMillis()
	user.DeletedAt = now
	user.PresenceExpiresAt = now
	user.ActiveSessionID = ""
	user.UpdatedAt = now
	session.RevokedAt = now
	session.LastSeenAt = now

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.writeJSONRecord(ctx, userRecordPrefix+user.Username, user); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to delete account")
		return
	}
	if err := a.writeJSONRecord(ctx, sessionRecordPrefix+session.SessionID, session); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to revoke session")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *API) handlePresenceHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, session, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}
	var req presenceRequest
	_ = decodeJSONBody(r, &req)
	baseURL := a.requestBaseURL(r)
	if req.NodeBaseURL != "" {
		baseURL = normalizeHTTPBaseURL(req.NodeBaseURL)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	a.touchSession(ctx, &user, &session, baseURL)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"user":        a.userView(user),
		"nodeBaseUrl": baseURL,
	})
}

func (a *API) handlePresenceOffline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, session, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}

	now := storage.NowMillis()
	user.PresenceExpiresAt = now
	user.UpdatedAt = now
	session.LastSeenAt = now
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_ = a.writeJSONRecord(ctx, userRecordPrefix+user.Username, user)
	_ = a.writeJSONRecord(ctx, sessionRecordPrefix+session.SessionID, session)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *API) handlePeerConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	_, _, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}
	var req startSessionRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetUsername := normalizeUsername(req.Username)
	if !usernamePattern.MatchString(targetUsername) {
		writeError(w, http.StatusBadRequest, "invalid target username")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	target, found, err := a.readUserRecord(ctx, userRecordPrefix+targetUsername)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to read target user")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "target user not found")
		return
	}
	if target.DeletedAt > 0 {
		writeError(w, http.StatusNotFound, "target user not found")
		return
	}
	writeJSON(w, http.StatusOK, connectResponse{Target: a.userView(target)})
}

func (a *API) handleSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, _, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}

	var req signalRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetUsername := normalizeUsername(req.To)
	if !usernamePattern.MatchString(targetUsername) {
		writeError(w, http.StatusBadRequest, "invalid target username")
		return
	}
	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "signal type is required")
		return
	}

	envelope := signalEnvelope{
		ID:        randomToken(18),
		Type:      req.Type,
		From:      user.Username,
		To:        targetUsername,
		Payload:   req.Payload,
		CreatedAt: storage.NowMillis(),
	}
	if err := a.routeSignal(r.Context(), envelope); err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *API) handleInternalSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req internalSignalRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Signal.To == "" || req.Signal.ID == "" {
		writeError(w, http.StatusBadRequest, "invalid signal")
		return
	}
	if err := a.appendMailboxSignal(r.Context(), req.Signal.To, req.Signal); err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to queue signal")
		return
	}
	a.notifyUser(req.Signal.To)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *API) handlePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	user, _, ok := a.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}

	timeout := defaultPollTimeout
	if raw := r.URL.Query().Get("timeoutMs"); raw != "" {
		if parsed, err := time.ParseDuration(raw + "ms"); err == nil && parsed > 0 && parsed <= maxPollTimeout {
			timeout = parsed
		}
	}

	if signals, err := a.consumeMailboxSignals(r.Context(), user.Username); err == nil && len(signals) > 0 {
		writeJSON(w, http.StatusOK, pollResponse{Signals: signals})
		return
	}

	waiter := a.registerPoller(user.Username)
	defer a.unregisterPoller(user.Username, waiter)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-waiter:
	case <-timer.C:
	case <-r.Context().Done():
		return
	}

	signals, err := a.consumeMailboxSignals(r.Context(), user.Username)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "failed to load signals")
		return
	}
	writeJSON(w, http.StatusOK, pollResponse{Signals: signals})
}

func (a *API) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if _, _, ok := a.authenticate(r); !ok {
		writeError(w, http.StatusUnauthorized, "invalid session")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, map[string]any{"nodes": a.collectNodeEntries(ctx)})
}

func (a *API) routeSignal(ctx context.Context, envelope signalEnvelope) error {
	target, found, err := a.readUserRecord(ctx, userRecordPrefix+envelope.To)
	if err != nil {
		return fmt.Errorf("failed to read target user")
	}
	if !found {
		return fmt.Errorf("target user not found")
	}
	if target.DeletedAt > 0 {
		return fmt.Errorf("target user not found")
	}

	if target.CurrentNodeID == a.nodeID || target.CurrentNodeHTTP == "" {
		if err := a.appendMailboxSignal(ctx, envelope.To, envelope); err != nil {
			return fmt.Errorf("failed to queue signal")
		}
		a.notifyUser(envelope.To)
		return nil
	}

	if err := a.forwardSignal(ctx, target.CurrentNodeHTTP, envelope); err == nil {
		return nil
	}

	if err := a.appendMailboxSignal(ctx, envelope.To, envelope); err != nil {
		return fmt.Errorf("failed to queue signal")
	}
	return nil
}

func (a *API) forwardSignal(ctx context.Context, baseURL string, envelope signalEnvelope) error {
	targetURL := strings.TrimRight(normalizeHTTPBaseURL(baseURL), "/") + "/v1/internal/signals/deliver"
	body, _ := json.Marshal(internalSignalRequest{Signal: envelope})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("signal forward failed with status %d", resp.StatusCode)
	}
	return nil
}

func (a *API) newSession(username, verifier, nodeHTTP string) (string, sessionRecord, error) {
	sessionID := randomToken(12)
	secret := randomToken(32)
	token := sessionID + "." + secret
	now := storage.NowMillis()
	record := sessionRecord{
		SessionID:   sessionID,
		Username:    username,
		SecretHash:  sha256Hex(secret),
		CreatedAt:   now,
		ExpiresAt:   now + int64(sessionTTL/time.Millisecond),
		LastSeenAt:  now,
		NodeID:      a.nodeID,
		NodeHTTP:    nodeHTTP,
		VerifierRef: sha256Hex(verifier),
	}
	return token, record, nil
}

func (a *API) authenticate(r *http.Request) (userRecord, sessionRecord, bool) {
	token := a.extractSessionToken(r)
	if token == "" {
		return userRecord{}, sessionRecord{}, false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return userRecord{}, sessionRecord{}, false
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var session sessionRecord
	found, err := a.readJSONRecord(ctx, sessionRecordPrefix+parts[0], &session)
	if err != nil || !found {
		return userRecord{}, sessionRecord{}, false
	}
	if session.RevokedAt > 0 || session.ExpiresAt < storage.NowMillis() || !secureEqual(session.SecretHash, sha256Hex(parts[1])) {
		return userRecord{}, sessionRecord{}, false
	}

	var user userRecord
	found, err = a.readJSONRecord(ctx, userRecordPrefix+session.Username, &user)
	if err != nil || !found {
		return userRecord{}, sessionRecord{}, false
	}
	if user.DeletedAt > 0 {
		return userRecord{}, sessionRecord{}, false
	}
	return user, session, true
}

func (a *API) extractSessionToken(r *http.Request) string {
	if token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), authTokenPrefix)); token != "" {
		return token
	}
	if token := strings.TrimSpace(r.URL.Query().Get("session")); token != "" {
		return token
	}
	return ""
}

func (a *API) touchSession(ctx context.Context, user *userRecord, session *sessionRecord, baseURL string) {
	now := storage.NowMillis()
	user.CurrentNodeID = a.nodeID
	user.CurrentNodeHTTP = normalizeHTTPBaseURL(baseURL)
	user.LastSeenAt = now
	user.PresenceExpiresAt = now + int64(presenceTTL/time.Millisecond)
	user.UpdatedAt = now
	user.ActiveSessionID = session.SessionID

	session.NodeID = a.nodeID
	session.NodeHTTP = user.CurrentNodeHTTP
	session.LastSeenAt = now

	_ = a.writeJSONRecord(ctx, userRecordPrefix+user.Username, *user)
	_ = a.writeJSONRecord(ctx, sessionRecordPrefix+session.SessionID, *session)
}

func (a *API) userView(user userRecord) userView {
	return userView{
		Username:        user.Username,
		IsActive:        user.PresenceExpiresAt > storage.NowMillis(),
		LastSeenAt:      user.LastSeenAt,
		CurrentNodeID:   user.CurrentNodeID,
		CurrentNodeHTTP: user.CurrentNodeHTTP,
	}
}

func (a *API) requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); forwarded != "" {
		scheme = forwarded
	}
	host := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = r.Host
	}
	if host == "" {
		return a.nodeHTTPBaseURL
	}
	return normalizeHTTPBaseURL(scheme + "://" + host)
}

func (a *API) registerPoller(username string) chan struct{} {
	ch := make(chan struct{}, 1)
	a.pollersMu.Lock()
	defer a.pollersMu.Unlock()
	if a.pollers[username] == nil {
		a.pollers[username] = make(map[chan struct{}]struct{})
	}
	a.pollers[username][ch] = struct{}{}
	return ch
}

func (a *API) unregisterPoller(username string, ch chan struct{}) {
	a.pollersMu.Lock()
	defer a.pollersMu.Unlock()
	waiters := a.pollers[username]
	delete(waiters, ch)
	if len(waiters) == 0 {
		delete(a.pollers, username)
	}
}

func (a *API) notifyUser(username string) {
	a.pollersMu.Lock()
	defer a.pollersMu.Unlock()
	for ch := range a.pollers[username] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (a *API) appendMailboxSignal(ctx context.Context, username string, signal signalEnvelope) error {
	key := mailboxRecordPrefix + username
	mailbox := mailboxRecord{Username: username}
	_, _ = a.readJSONRecord(ctx, key, &mailbox)
	if slices.ContainsFunc(mailbox.Signals, func(existing signalEnvelope) bool { return existing.ID == signal.ID }) {
		return nil
	}
	mailbox.Signals = append(mailbox.Signals, signal)
	if len(mailbox.Signals) > maxMailboxSignals {
		mailbox.Signals = mailbox.Signals[len(mailbox.Signals)-maxMailboxSignals:]
	}
	return a.writeJSONRecord(ctx, key, mailbox)
}

func (a *API) consumeMailboxSignals(ctx context.Context, username string) ([]signalEnvelope, error) {
	key := mailboxRecordPrefix + username
	var mailbox mailboxRecord
	found, err := a.readJSONRecord(ctx, key, &mailbox)
	if err != nil || !found || len(mailbox.Signals) == 0 {
		return nil, err
	}
	signals := mailbox.Signals
	mailbox.Signals = nil
	if err := a.writeJSONRecord(ctx, key, mailbox); err != nil {
		return nil, err
	}
	return signals, nil
}

func (a *API) readUserRecord(ctx context.Context, key string) (userRecord, bool, error) {
	var user userRecord
	found, err := a.readJSONRecord(ctx, key, &user)
	return user, found, err
}

func (a *API) collectNodeEntries(ctx context.Context) []nodeEntry {
	seen := make(map[string]struct{})
	var nodes []nodeEntry
	for _, member := range a.mgr.AllMembers() {
		if _, ok := seen[member.NodeId]; ok {
			continue
		}
		seen[member.NodeId] = struct{}{}

		record := nodeHTTPRecord{}
		_, _ = a.readJSONRecord(ctx, nodeHTTPRecordPrefix+member.NodeId, &record)
		baseURL := normalizeHTTPBaseURL(record.BaseURL)
		if member.NodeId == a.nodeID && baseURL == "" {
			baseURL = a.nodeHTTPBaseURL
		}
		nodes = append(nodes, nodeEntry{
			NodeID:   member.NodeId,
			BaseURL:  baseURL,
			IsSelf:   member.NodeId == a.nodeID,
			IsActive: member.State == membership.PeerUp,
		})
	}
	return nodes
}

func (a *API) readJSONRecord(ctx context.Context, key string, out any) (bool, error) {
	resp, err := a.node.Read(ctx, &pb.ReadRequest{Key: key})
	if err != nil {
		return false, err
	}
	if !resp.GetFound() {
		return false, nil
	}
	if err := json.Unmarshal(resp.GetValue(), out); err != nil {
		return false, err
	}
	return true, nil
}

func (a *API) writeJSONRecord(ctx context.Context, key string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = a.node.Write(ctx, &pb.WriteRequest{Key: key, Value: body})
	return err
}

func decodeWriteValue(r *http.Request) ([]byte, error) {
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "application/json") {
		var req writeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, fmt.Errorf("invalid JSON body")
		}
		switch {
		case req.Base64 != "":
			decoded, err := base64.StdEncoding.DecodeString(req.Base64)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 value")
			}
			return decoded, nil
		case req.Value != "":
			return []byte(req.Value), nil
		default:
			return nil, errors.New("request body must include value or base64")
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to read request body")
	}
	if len(body) == 0 {
		return nil, errors.New("request body is required")
	}
	return body, nil
}

func decodeJSONBody(r *http.Request, out any) error {
	if r.Body == nil {
		return errors.New("request body is required")
	}
	defer r.Body.Close()
	if err := json.NewDecoder(io.LimitReader(r.Body, maxRequestBytes)).Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid JSON body")
	}
	return nil
}

func recordKeyFromPath(path string) (string, bool) {
	const prefix = "/v1/records/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	key := strings.TrimPrefix(path, prefix)
	if key == "" || strings.Contains(key, "/") {
		return "", false
	}
	return key, true
}

func writeStatusError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	switch st.Code() {
	case codes.NotFound:
		writeError(w, http.StatusNotFound, st.Message())
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, st.Message())
	case codes.Unavailable, codes.DeadlineExceeded:
		writeError(w, http.StatusServiceUnavailable, st.Message())
	default:
		writeError(w, http.StatusInternalServerError, st.Message())
	}
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(body)
}

func normalizeUsername(username string) string {
	return strings.ToLower(strings.TrimSpace(username))
}

func normalizeHTTPBaseURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return strings.TrimRight(parsed.String(), "/")
}

func randomToken(byteLen int) string {
	buf := make([]byte, byteLen)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func computeChallengeProof(verifier, nonce string) string {
	mac := hmac.New(sha256.New, []byte(verifier))
	mac.Write([]byte(nonce))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func secureEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
}
