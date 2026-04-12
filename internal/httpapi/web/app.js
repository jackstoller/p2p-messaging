const DEFAULT_ICE_SERVERS = [
    { urls: ["stun:stun.l.google.com:19302"] },
    { urls: ["stun:stun1.l.google.com:19302"] },
    { urls: ["stun:stun2.l.google.com:19302"] },
];

const STORAGE_KEYS = {
    authToken: "meshline.authToken",
    username: "meshline.username",
    secret: "meshline.secret",
    nodeUrls: "meshline.nodeUrls",
    iceServers: "meshline.iceServers",
};

const MAX_MESSAGES = 200;
const MAX_PENDING_ICE = 32;
const RECONNECT_DELAYS_MS = [1200, 2500, 5000, 10000, 20000];
const FINGERPRINT_UNAVAILABLE = "Unavailable";
const FINGERPRINT_UNSUPPORTED = "Not supported on this browser";

const state = {
    authToken: localStorage.getItem(STORAGE_KEYS.authToken) || "",
    username: localStorage.getItem(STORAGE_KEYS.username) || "",
    savedSecret: localStorage.getItem(STORAGE_KEYS.secret) || "",
    nodeUrls: safeJSONParse(localStorage.getItem(STORAGE_KEYS.nodeUrls), []),
    iceServers: normalizeIceServers(safeJSONParse(localStorage.getItem(STORAGE_KEYS.iceServers), DEFAULT_ICE_SERVERS)),
    activeNodeUrl: location.origin,
    currentPeer: "",
    peerProfiles: {},
    sessions: {},
    heartbeatTimer: null,
    pollAbort: null,
    isCreatingConversation: false,
    conversationPendingTimer: null,
};

const els = {
    authScreen: document.getElementById("authScreen"),
    appShell: document.getElementById("appShell"),
    createUsername: document.getElementById("createUsername"),
    sidebarUser: document.getElementById("sidebarUser"),
    sidebarNode: document.getElementById("sidebarNode"),
    sidebarPresence: document.getElementById("sidebarPresence"),
    conversationList: document.getElementById("conversationList"),
    chatTitle: document.getElementById("chatTitle"),
    chatSubtitle: document.getElementById("chatSubtitle"),
    peerStatusDot: document.getElementById("peerStatusDot"),
    newConversationPanel: document.getElementById("newConversationPanel"),
    newConversationInput: document.getElementById("newConversationInput"),
    newConversationStatus: document.getElementById("newConversationStatus"),
    connectPeerBtn: document.getElementById("connectPeerBtn"),
    disconnectPeerBtn: document.getElementById("disconnectPeerBtn"),
    messages: document.getElementById("messages"),
    messageInput: document.getElementById("messageInput"),
    sendBtn: document.getElementById("sendBtn"),
    profileUsername: document.getElementById("profileUsername"),
    profileNode: document.getElementById("profileNode"),
    fingerprintDialog: document.getElementById("fingerprintDialog"),
    fingerprintValue: document.getElementById("fingerprintValue"),
    showFingerprintBtn: document.getElementById("showFingerprintBtn"),
    toast: document.getElementById("toast"),
    sidebar: document.querySelector(".sidebar"),
    chatPane: document.querySelector(".chat-pane"),
};

document.getElementById("createAccountBtn").addEventListener("click", createAccount);
document.getElementById("newConversationBtn").addEventListener("click", () => openConversation("", true));
document.getElementById("connectPeerBtn").addEventListener("click", startPeerSession);
document.getElementById("disconnectPeerBtn").addEventListener("click", toggleCurrentConversationConnection);
document.getElementById("sendBtn").addEventListener("click", sendMessage);
document.getElementById("backToListBtn").addEventListener("click", backToConversationList);
document.getElementById("showFingerprintBtn").addEventListener("click", showCurrentFingerprint);
document.getElementById("refreshNodesBtn").addEventListener("click", refreshNodes);
document.getElementById("deleteAccountBtn").addEventListener("click", deleteAccount);

els.createUsername.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
        event.preventDefault();
        void createAccount();
    }
});

els.newConversationInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
        event.preventDefault();
        void startPeerSession();
    }
});

els.messageInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        void sendMessage();
    }
});

window.addEventListener("beforeunload", () => {
    sendOfflineBeacon();
    closeAllSessionTransports();
});

document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
        sendOfflineBeacon();
        return;
    }
    if (state.username && state.savedSecret) {
        void heartbeat().catch(handleNetworkError);
        reconnectPersistedSessions();
    }
});

init();

async function init() {
    if (state.username && state.savedSecret) {
        try {
            const me = await apiFetch("/v1/me");
            applySession(me.user.username, state.savedSecret, me);
            return;
        } catch (error) {
            clearAllLocalUserData();
        }
    }
    renderSignedOut();
}

async function createAccount() {
    const username = normalizeUsername(els.createUsername.value);
    if (!username) {
        toast("Choose a username first.", true);
        return;
    }

    const response = await apiFetch("/v1/auth/register", {
        method: "POST",
        body: JSON.stringify({ username }),
    });

    applySession(username, response.secret || "", response);
    toast("Account created.", false, true);
}

function applySession(username, secret, payload) {
    state.username = username;
    state.savedSecret = secret || state.savedSecret;
    state.authToken = "";
    state.activeNodeUrl = payload.nodeBaseUrl || state.activeNodeUrl;
    state.nodeUrls = (payload.nodes || []).map((node) => node.baseUrl).filter(Boolean);
    state.iceServers = normalizeIceServers(payload.iceServers || state.iceServers);

    localStorage.setItem(STORAGE_KEYS.username, state.username);
    localStorage.setItem(STORAGE_KEYS.secret, state.savedSecret);
    localStorage.removeItem(STORAGE_KEYS.authToken);
    localStorage.setItem(STORAGE_KEYS.nodeUrls, JSON.stringify(state.nodeUrls));
    localStorage.setItem(STORAGE_KEYS.iceServers, JSON.stringify(state.iceServers));

    restoreSessions();

    els.sidebarUser.textContent = state.username;
    els.profileUsername.textContent = state.username;
    updateNodeUI(payload.nodeBaseUrl || state.activeNodeUrl, payload.user?.isActive);
    els.authScreen.classList.add("hidden");
    els.appShell.classList.remove("hidden");

    renderConversationList();
    startHeartbeatLoop();
    startPollLoop();
    reconnectPersistedSessions();

    const conversations = getConversations();
    if (conversations.length > 0) {
        openConversation(conversations[0].peer, false);
    } else {
        openConversation("", true);
    }
}

function renderSignedOut() {
    els.authScreen.classList.remove("hidden");
    els.appShell.classList.add("hidden");
    els.createUsername.value = "";
    els.createUsername.focus();
}

function updateNodeUI(nodeUrl, active) {
    els.sidebarNode.textContent = nodeUrl ? `Node ${nodeUrl}` : "Node unknown";
    els.profileNode.textContent = nodeUrl ? `Connected to ${nodeUrl}` : "Connected node unknown";
    els.sidebarPresence.textContent = active ? "Active" : "Idle";
}

function restoreSessions() {
    closeAllSessionTransports();
    state.sessions = {};

    const storedSessions = safeJSONParse(localStorage.getItem(sessionStorageKey()), {});
    Object.values(storedSessions).forEach((entry) => {
        if (!entry || typeof entry !== "object") {
            return;
        }
        const peer = normalizeUsername(entry.peer || "");
        if (!peer || peer === state.username) {
            return;
        }
        state.sessions[peer] = createSession(peer, {
            ...entry,
            status: "disconnected",
            wantsConnection: false,
            currentNodeHttp: "",
        });
    });

    getMessageSummaries().forEach((summary) => {
        const session = ensureSession(summary.peer);
        session.lastText = summary.lastText;
        session.lastMessageAt = summary.lastAt;
        session.lastTouchedAt = Math.max(session.lastTouchedAt || 0, summary.lastAt);
    });

    persistSessions();
}

function createSession(peer, entry = {}) {
    const now = Date.now();
    return {
        peer,
        pc: null,
        dc: null,
        status: entry.status || "disconnected",
        wantsConnection: Boolean(entry.wantsConnection),
        lastKnownLive: Boolean(entry.lastKnownLive),
        lastTouchedAt: Number(entry.lastTouchedAt) || 0,
        lastMessageAt: Number(entry.lastMessageAt) || 0,
        lastText: String(entry.lastText || ""),
        createdAt: Number(entry.createdAt) || now,
        reconnectAttempt: 0,
        reconnectTimer: null,
        pendingCandidates: [],
        fingerprint: FINGERPRINT_UNAVAILABLE,
        connectPromise: null,
        connectionEpoch: 0,
        role: "",
        currentNodeHttp: normalizeBaseUrl(entry.currentNodeHttp || ""),
    };
}

function ensureSession(peer) {
    const normalizedPeer = normalizeUsername(peer);
    if (!normalizedPeer) {
        return null;
    }
    if (!state.sessions[normalizedPeer]) {
        state.sessions[normalizedPeer] = createSession(normalizedPeer, {
            wantsConnection: false,
            status: "disconnected",
        });
        persistSessions();
    }
    return state.sessions[normalizedPeer];
}

function persistSessions() {
    if (!state.username) {
        return;
    }

    const payload = {};
    Object.values(state.sessions).forEach((session) => {
        payload[session.peer] = {
            peer: session.peer,
            lastKnownLive: session.lastKnownLive,
            lastTouchedAt: session.lastTouchedAt,
            lastMessageAt: session.lastMessageAt,
            lastText: session.lastText,
            createdAt: session.createdAt,
        };
    });

    localStorage.setItem(sessionStorageKey(), JSON.stringify(payload));
}

function sessionStorageKey() {
    return `meshline.sessions.${state.username}`;
}

function getMessageSummaries() {
    const summaries = [];
    const prefix = "meshline.messages.";
    for (let index = 0; index < localStorage.length; index += 1) {
        const key = localStorage.key(index);
        if (!key || !key.startsWith(prefix)) {
            continue;
        }
        const parts = key.split(".");
        if (parts.length !== 4 || !parts.includes(state.username)) {
            continue;
        }
        const peer = parts[2] === state.username ? parts[3] : parts[2];
        const messages = safeJSONParse(localStorage.getItem(key), []);
        if (!Array.isArray(messages) || messages.length === 0) {
            continue;
        }
        const last = messages[messages.length - 1];
        summaries.push({
            peer,
            lastText: String(last.text || ""),
            lastAt: Number(last.at) || 0,
        });
    }
    return summaries;
}

function getConversations() {
    const conversations = Object.values(state.sessions).map((session) => ({
        peer: session.peer,
        lastText: conversationPreview(session),
        lastAt: conversationTimestamp(session),
        statusLabel: sessionStatusLabel(session),
    }));

    conversations.sort((left, right) => {
        const rightScore = conversationSortScore(state.sessions[right.peer]);
        const leftScore = conversationSortScore(state.sessions[left.peer]);
        if (rightScore !== leftScore) {
            return rightScore - leftScore;
        }
        if (right.lastAt !== left.lastAt) {
            return right.lastAt - left.lastAt;
        }
        return left.peer.localeCompare(right.peer);
    });

    return conversations;
}

function conversationTimestamp(session) {
    return Math.max(
        Number(session.lastMessageAt) || 0,
        Number(session.lastTouchedAt) || 0,
        Number(session.createdAt) || 0,
    );
}

function conversationSortScore(session) {
    if (session.status === "connected") {
        return 3;
    }
    if (session.status === "connecting") {
        return 2;
    }
    if (session.wantsConnection || session.lastKnownLive) {
        return 1;
    }
    return 0;
}

function sessionStatusLabel(session) {
    if (session.status === "connected") {
        return "Connected";
    }
    if (session.status === "connecting") {
        return isReconnectOwner(session.peer) ? "Reconnecting" : "Waiting";
    }
    if (session.lastKnownLive || session.wantsConnection) {
        return "Disconnected";
    }
    return session.lastMessageAt ? "History" : "New";
}

function conversationPreview(session) {
    if (session.lastText) {
        return session.lastText;
    }
    if (session.status === "connected") {
        return "Secure channel ready.";
    }
    if (session.status === "connecting") {
        return isReconnectOwner(session.peer)
            ? "Connecting securely..."
            : "Waiting for the other device...";
    }
    if (session.lastKnownLive || session.wantsConnection) {
        return "Disconnected. Reconnect to continue.";
    }
    return "No messages yet.";
}

function renderConversationList() {
    const conversations = getConversations();
    els.conversationList.innerHTML = "";

    if (conversations.length === 0) {
        const empty = document.createElement("div");
        empty.className = "hint";
        empty.textContent = "No conversations yet. Start one with the New button.";
        els.conversationList.appendChild(empty);
        return;
    }

    conversations.forEach((conversation) => {
        const session = ensureSession(conversation.peer);
        const item = document.createElement("button");
        item.className = `conversation-item ${conversation.peer === state.currentPeer ? "active" : ""}`;
        item.innerHTML = `
            <span class="conversation-row">
                <span class="conversation-identity">
                    <span class="status-dot ${conversationDotClass(session)}"></span>
                    <strong class="conversation-name">${escapeHtml(conversation.peer)}</strong>
                </span>
                <span class="conversation-time">${escapeHtml(conversationMeta(session))}</span>
            </span>
            <span class="conversation-preview">${escapeHtml(conversation.lastText)}</span>
        `;
        item.addEventListener("click", () => openConversation(conversation.peer, false));
        els.conversationList.appendChild(item);
    });
}

function conversationMeta(session) {
    if (!session.lastMessageAt && !session.lastTouchedAt) {
        return sessionStatusLabel(session);
    }
    return new Date(conversationTimestamp(session)).toLocaleTimeString([], {
        hour: "numeric",
        minute: "2-digit",
    });
}

function conversationDotClass(session) {
    if (session.status === "connected") {
        return "online";
    }
    if (session.status === "connecting") {
        return "connecting";
    }
    return "";
}

function openConversation(peer, showComposer) {
    state.currentPeer = normalizeUsername(peer || "");
    renderConversationList();
    els.newConversationPanel.classList.toggle("hidden", !showComposer);

    if (!state.currentPeer) {
        els.chatTitle.textContent = showComposer ? "New conversation" : "Conversations";
        els.chatSubtitle.textContent = showComposer
            ? "Start a new chat by username."
            : "Pick a conversation or start a new one.";
        els.peerStatusDot.className = "status-dot";
        els.messages.innerHTML = `<div class="empty-state">${showComposer ? "Choose a username to begin a conversation." : "Select a conversation from the list."}</div>`;
        els.disconnectPeerBtn.disabled = true;
        els.disconnectPeerBtn.textContent = "Disconnect";
        els.showFingerprintBtn.disabled = true;
        updateComposerState(null);
        if (showComposer) {
            els.newConversationInput.focus();
            toggleMobilePane("compose");
        } else {
            toggleMobilePane("list");
        }
        return;
    }

    const session = ensureSession(state.currentPeer);
    els.chatTitle.textContent = state.currentPeer;
    els.chatSubtitle.textContent = chatSubtitle(session);
    els.peerStatusDot.className = `status-dot ${conversationDotClass(session)}`;
    els.disconnectPeerBtn.disabled = false;
    els.disconnectPeerBtn.textContent = session.status === "connected" || session.status === "connecting"
        ? "Disconnect"
        : "Reconnect";
    els.showFingerprintBtn.disabled = false;
    renderMessages(loadConversation(state.currentPeer), session);
    updateComposerState(session);
    toggleMobilePane("chat");
}

function backToConversationList() {
    openConversation("", false);
}

function chatSubtitle(session) {
    const profile = state.peerProfiles[session.peer];
    if (session.status === "connected") {
        return "Secure channel ready.";
    }
    if (session.status === "connecting") {
        return isReconnectOwner(session.peer)
            ? "Connecting securely..."
            : "Waiting for the peer to reconnect...";
    }
    if (session.lastKnownLive || session.wantsConnection) {
        return "Secure channel offline. Reconnect to continue.";
    }
    if (profile?.isActive) {
        return "Peer is online. Connect when you are ready.";
    }
    return "Conversation history.";
}

function updateComposerState(session) {
    const hasPeer = Boolean(session?.peer);
    const canSend = Boolean(session && session.status === "connected" && session.dc && session.dc.readyState === "open");
    els.messageInput.disabled = !canSend;
    els.sendBtn.disabled = !canSend;
    els.messageInput.placeholder = !hasPeer
        ? "Choose or start a conversation first"
        : canSend
            ? "Write a message"
            : session.status === "connecting"
                ? "Secure channel is connecting..."
                : "Reconnect this conversation to send messages";
}

async function startPeerSession() {
    if (state.isCreatingConversation) {
        return;
    }

    const peer = normalizeUsername(els.newConversationInput.value);
    if (!peer) {
        toast("Enter a username to start chatting.", true);
        return;
    }
    if (peer === state.username) {
        toast("Choose someone else to chat with.", true);
        return;
    }

    state.isCreatingConversation = true;
    setConversationPending(true, `Connecting with ${peer}...`);

    try {
        await requestPeerConnection(peer, { userInitiated: true, focusConversation: true });
        els.newConversationInput.value = "";
        setConversationPending(false);
    } catch (error) {
        setConversationPending(false);
        handleNetworkError(error);
    } finally {
        state.isCreatingConversation = false;
    }
}

async function requestPeerConnection(peer, options = {}) {
    const session = ensureSession(peer);
    if (!session) {
        throw new Error("Peer is required.");
    }
    if (session.connectPromise) {
        return session.connectPromise;
    }
    if (session.status === "connected" && session.dc?.readyState === "open") {
        if (options.focusConversation) {
            openConversation(peer, false);
        }
        return session;
    }

    session.wantsConnection = true;
    session.lastKnownLive = true;
    session.status = "connecting";
    session.lastTouchedAt = Date.now();
    session.reconnectAttempt = 0;
    persistSessions();
    renderConversationList();
    if (options.focusConversation) {
        openConversation(peer, false);
    } else if (state.currentPeer === peer) {
        openConversation(peer, false);
    }

    const connectionPromise = (async () => {
        const payload = await apiFetch("/v1/peers/connect", {
            method: "POST",
            body: JSON.stringify({ username: peer }),
        });

        state.peerProfiles[peer] = {
            ...(state.peerProfiles[peer] || {}),
            ...(payload.target || {}),
            username: peer,
        };
        session.currentNodeHttp = normalizeBaseUrl(payload.target?.currentNodeHttp || session.currentNodeHttp);
        state.iceServers = normalizeIceServers(payload.iceServers || state.iceServers);
        localStorage.setItem(STORAGE_KEYS.iceServers, JSON.stringify(state.iceServers));

        await establishOffer(peer);
        return session;
    })();

    session.connectPromise = connectionPromise;

    try {
        return await connectionPromise;
    } catch (error) {
        if (options.userInitiated && session.status !== "connected") {
            session.wantsConnection = false;
            session.status = "disconnected";
            closeSessionTransport(session);
        }
        throw error;
    } finally {
        session.connectPromise = null;
        persistSessions();
        if (state.currentPeer === peer) {
            openConversation(peer, false);
        } else {
            renderConversationList();
        }
    }
}

async function establishOffer(peer) {
    const session = preparePeerConnection(peer, true);
    session.role = "caller";
    const offer = await session.pc.createOffer();
    if (session.pc.signalingState === "closed") {
        return;
    }
    await session.pc.setLocalDescription(offer);
    await sendSignal("offer", peer, offer);
}

function preparePeerConnection(peer, isCaller) {
    const session = ensureSession(peer);
    clearReconnectTimer(session);
    closeSessionTransport(session);

    session.status = "connecting";
    session.wantsConnection = true;
    session.lastKnownLive = true;
    session.lastTouchedAt = Date.now();
    session.fingerprint = FINGERPRINT_UNAVAILABLE;
    session.pendingCandidates = [];
    session.connectionEpoch += 1;
    const epoch = session.connectionEpoch;

    session.pc = new RTCPeerConnection(buildPeerConnectionConfig());
    session.pc.onicecandidate = (event) => {
        if (!event.candidate || session.connectionEpoch !== epoch) {
            return;
        }
        void sendSignal("ice-candidate", peer, event.candidate).catch((error) => {
            console.error(error);
        });
    };
    session.pc.onconnectionstatechange = () => {
        if (session.connectionEpoch !== epoch) {
            return;
        }
        handleConnectionStateChange(peer, epoch);
    };

    if (isCaller) {
        session.dc = session.pc.createDataChannel("meshline-chat");
        bindDataChannel(peer, session.dc, epoch);
    } else {
        session.pc.ondatachannel = (event) => {
            if (session.connectionEpoch !== epoch) {
                return;
            }
            session.dc = event.channel;
            bindDataChannel(peer, session.dc, epoch);
        };
    }

    persistSessions();
    renderConversationList();
    return session;
}

function closeSessionTransport(session) {
    if (!session) {
        return;
    }

    session.connectionEpoch += 1;
    if (session.dc) {
        session.dc.onopen = null;
        session.dc.onclose = null;
        session.dc.onmessage = null;
        session.dc.onerror = null;
        try {
            session.dc.close();
        } catch (error) {
            console.error(error);
        }
    }

    if (session.pc) {
        session.pc.onicecandidate = null;
        session.pc.onconnectionstatechange = null;
        session.pc.ondatachannel = null;
        try {
            session.pc.close();
        } catch (error) {
            console.error(error);
        }
    }

    session.pc = null;
    session.dc = null;
    session.role = "";
    session.pendingCandidates = [];
    session.fingerprint = FINGERPRINT_UNAVAILABLE;
    session.currentNodeHttp = "";
}

function closeAllSessionTransports() {
    Object.values(state.sessions).forEach((session) => {
        clearReconnectTimer(session);
        closeSessionTransport(session);
    });
}

function bindDataChannel(peer, channel, epoch) {
    const session = ensureSession(peer);
    channel.onopen = () => {
        if (session.connectionEpoch !== epoch) {
            return;
        }
        session.status = "connected";
        session.wantsConnection = true;
        session.lastKnownLive = true;
        session.lastTouchedAt = Date.now();
        session.reconnectAttempt = 0;
        persistSessions();
        renderConversationList();
        if (state.currentPeer === peer) {
            openConversation(peer, false);
        }
        void refreshFingerprint(peer).catch(console.error);
    };

    channel.onmessage = (event) => {
        if (session.connectionEpoch !== epoch) {
            return;
        }
        persistMessage(peer, { from: peer, text: event.data, at: Date.now() });
        if (state.currentPeer === peer) {
            renderMessages(loadConversation(peer), session);
        }
        renderConversationList();
    };

    channel.onclose = () => {
        if (session.connectionEpoch !== epoch) {
            return;
        }
        markSessionDisconnected(peer, true);
    };

    channel.onerror = () => {
        if (session.connectionEpoch !== epoch) {
            return;
        }
        markSessionDisconnected(peer, true);
    };
}

function handleConnectionStateChange(peer, epoch) {
    const session = ensureSession(peer);
    if (!session.pc || session.connectionEpoch !== epoch) {
        return;
    }

    if (session.pc.connectionState === "connected") {
        session.status = "connected";
        session.wantsConnection = true;
        session.lastKnownLive = true;
        session.lastTouchedAt = Date.now();
        session.reconnectAttempt = 0;
        persistSessions();
        renderConversationList();
        if (state.currentPeer === peer) {
            openConversation(peer, false);
        }
        return;
    }

    if (session.pc.connectionState === "connecting") {
        session.status = "connecting";
        persistSessions();
        renderConversationList();
        if (state.currentPeer === peer) {
            openConversation(peer, false);
        }
        return;
    }

    if (session.pc.connectionState === "disconnected" || session.pc.connectionState === "failed" || session.pc.connectionState === "closed") {
        markSessionDisconnected(peer, true);
    }
}

function markSessionDisconnected(peer, shouldReconnect) {
    const session = ensureSession(peer);
    closeSessionTransport(session);
    session.status = shouldReconnect && session.wantsConnection ? "connecting" : "disconnected";
    session.lastTouchedAt = Date.now();
    persistSessions();
    renderConversationList();
    if (state.currentPeer === peer) {
        openConversation(peer, false);
    }
    if (shouldReconnect) {
        scheduleReconnect(peer, false);
    }
}

function clearReconnectTimer(session) {
    if (!session?.reconnectTimer) {
        return;
    }
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
}

function scheduleReconnect(peer, immediate) {
    const session = ensureSession(peer);
    if (!session || !session.wantsConnection) {
        return;
    }

    clearReconnectTimer(session);

    if (!isReconnectOwner(peer)) {
        session.status = "connecting";
        persistSessions();
        renderConversationList();
        if (state.currentPeer === peer) {
            openConversation(peer, false);
        }
        return;
    }

    const index = Math.min(session.reconnectAttempt, RECONNECT_DELAYS_MS.length - 1);
    const delay = immediate ? 150 : RECONNECT_DELAYS_MS[index];
    session.reconnectTimer = setTimeout(() => {
        session.reconnectTimer = null;
        if (!state.username || !state.savedSecret || !session.wantsConnection) {
            return;
        }
        session.reconnectAttempt += 1;
        void requestPeerConnection(peer).catch((error) => {
            console.error(error);
            persistSessions();
            renderConversationList();
            scheduleReconnect(peer, false);
        });
    }, delay);
}

function reconnectPersistedSessions() {
    Object.values(state.sessions).forEach((session) => {
        if (!session.wantsConnection) {
            return;
        }
        if (session.status === "connected" && session.dc?.readyState === "open") {
            return;
        }
        scheduleReconnect(session.peer, true);
    });
}

function isReconnectOwner(peer) {
    return normalizeUsername(state.username).localeCompare(normalizeUsername(peer)) < 0;
}

async function toggleCurrentConversationConnection() {
    if (!state.currentPeer) {
        return;
    }

    const session = ensureSession(state.currentPeer);
    if (session.status === "connected" || session.status === "connecting") {
        session.wantsConnection = false;
        session.status = "disconnected";
        session.lastTouchedAt = Date.now();
        clearReconnectTimer(session);
        closeSessionTransport(session);
        persistSessions();
        renderConversationList();
        openConversation(state.currentPeer, false);
        void sendSignal("disconnect", state.currentPeer, { reason: "user-disconnected" }).catch(console.error);
        toast(`Disconnected from ${state.currentPeer}.`, false, true);
        return;
    }

    try {
        await requestPeerConnection(state.currentPeer, { userInitiated: true, focusConversation: true });
        toast(`Reconnecting with ${state.currentPeer}.`, false, true);
    } catch (error) {
        handleNetworkError(error);
    }
}

async function handleSignal(signal) {
    const peer = normalizeUsername(signal.from);
    if (!peer) {
        return;
    }

    const session = ensureSession(peer);
    session.lastTouchedAt = Math.max(session.lastTouchedAt || 0, Number(signal.createdAt) || Date.now());
    session.currentNodeHttp = normalizeBaseUrl(signal.fromNodeBaseUrl || session.currentNodeHttp);
    state.peerProfiles[peer] = {
        ...(state.peerProfiles[peer] || {}),
        username: peer,
        isActive: true,
        currentNodeHttp: session.currentNodeHttp,
    };

    if (signal.type === "disconnect") {
        session.wantsConnection = false;
        session.status = "disconnected";
        clearReconnectTimer(session);
        closeSessionTransport(session);
        persistSessions();
        renderConversationList();
        if (state.currentPeer === peer) {
            openConversation(peer, false);
        }
        return;
    }

    if (signal.type === "offer") {
        session.wantsConnection = true;
        session.lastKnownLive = true;
        session.status = "connecting";

        const hasLocalOffer = session.role === "caller" && session.pc && session.pc.signalingState !== "stable";
        if (hasLocalOffer && !shouldAcceptIncomingOffer(peer)) {
            return;
        }

        const activeSession = preparePeerConnection(peer, false);
        activeSession.role = "callee";
        await activeSession.pc.setRemoteDescription(signal.payload);
        await flushPendingCandidates(peer);
        const answer = await activeSession.pc.createAnswer();
        await activeSession.pc.setLocalDescription(answer);
        await sendSignal("answer", peer, answer);
        persistSessions();
        renderConversationList();
        if (!state.currentPeer || state.currentPeer === peer) {
            openConversation(peer, false);
        }
        return;
    }

    if (signal.type === "answer") {
        if (!session.pc || session.role !== "caller" || session.pc.signalingState !== "have-local-offer") {
            return;
        }
        await session.pc.setRemoteDescription(signal.payload);
        await flushPendingCandidates(peer);
        return;
    }

    if (signal.type === "ice-candidate") {
        if (!session.pc || !session.pc.remoteDescription) {
            session.pendingCandidates.push(signal.payload);
            session.pendingCandidates = session.pendingCandidates.slice(-MAX_PENDING_ICE);
            return;
        }
        await session.pc.addIceCandidate(signal.payload);
    }
}

function shouldAcceptIncomingOffer(peer) {
    return normalizeUsername(state.username).localeCompare(normalizeUsername(peer)) > 0;
}

async function flushPendingCandidates(peer) {
    const session = ensureSession(peer);
    if (!session.pc || !session.pc.remoteDescription) {
        return;
    }
    while (session.pendingCandidates.length > 0) {
        const candidate = session.pendingCandidates.shift();
        await session.pc.addIceCandidate(candidate);
    }
}

async function sendSignal(type, to, payload) {
    await apiFetch("/v1/signals", {
        method: "POST",
        body: JSON.stringify({ type, to, payload }),
    });
}

async function sendMessage() {
    const text = els.messageInput.value.trim();
    if (!text || !state.currentPeer) {
        return;
    }

    const session = ensureSession(state.currentPeer);
    if (!session.dc || session.dc.readyState !== "open") {
        session.wantsConnection = true;
        session.lastKnownLive = true;
        session.status = "connecting";
        persistSessions();
        renderConversationList();
        if (state.currentPeer === session.peer) {
            openConversation(session.peer, false);
        }
        void requestPeerConnection(state.currentPeer, { userInitiated: true, focusConversation: true }).catch(console.error);
        toast("Secure channel is reconnecting. Try again in a moment.", true);
        return;
    }

    session.dc.send(text);
    persistMessage(state.currentPeer, { from: state.username, text, at: Date.now() });
    els.messageInput.value = "";
    renderMessages(loadConversation(state.currentPeer), session);
    renderConversationList();
}

function persistMessage(peer, message) {
    const messages = loadConversation(peer);
    messages.push(message);
    localStorage.setItem(conversationKey(peer), JSON.stringify(messages.slice(-MAX_MESSAGES)));

    const session = ensureSession(peer);
    session.lastText = String(message.text || "");
    session.lastMessageAt = Number(message.at) || Date.now();
    session.lastTouchedAt = session.lastMessageAt;
    if (message.from === state.username || message.from === peer) {
        session.lastKnownLive = true;
    }
    persistSessions();
}

function loadConversation(peer) {
    const messages = safeJSONParse(localStorage.getItem(conversationKey(peer)), []);
    return Array.isArray(messages) ? messages : [];
}

function conversationKey(peer) {
    const ids = [state.username, peer].sort();
    return `meshline.messages.${ids[0]}.${ids[1]}`;
}

function renderMessages(messages, session) {
    if (!messages.length) {
        els.messages.innerHTML = `<div class="empty-state">${emptyStateCopy(session)}</div>`;
        return;
    }

    els.messages.innerHTML = messages.map((message) => `
        <div class="message ${message.from === state.username ? "outgoing" : ""}">
            <div>${escapeHtml(message.text)}</div>
            <div class="message-meta">${escapeHtml(message.from)} - ${new Date(message.at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}</div>
        </div>
    `).join("");
    els.messages.scrollTop = els.messages.scrollHeight;
}

function emptyStateCopy(session) {
    if (!session?.peer) {
        return "Choose a username to begin a conversation.";
    }
    if (session.status === "connected") {
        return "Secure channel is ready. Say hello.";
    }
    if (session.status === "connecting") {
        return isReconnectOwner(session.peer)
            ? `Connecting with ${escapeHtml(session.peer)}...`
            : `Waiting for ${escapeHtml(session.peer)} to reconnect...`;
    }
    if (session.lastKnownLive || session.wantsConnection) {
        return "Secure channel offline. Reconnect to continue.";
    }
    return "No messages yet. Reconnect when you are ready.";
}

async function heartbeat() {
    const payload = await apiFetch("/v1/presence/heartbeat", {
        method: "POST",
        body: JSON.stringify({ nodeBaseUrl: state.activeNodeUrl }),
    });
    updateNodeUI(payload.nodeBaseUrl, payload.user?.isActive);
}

function startHeartbeatLoop() {
    clearInterval(state.heartbeatTimer);
    void heartbeat().catch(handleNetworkError);
    state.heartbeatTimer = setInterval(() => {
        void heartbeat().catch(handleNetworkError);
    }, 15000);
}

function startPollLoop() {
    if (state.pollAbort) {
        state.pollAbort.abort();
    }
    state.pollAbort = new AbortController();

    const loop = async () => {
        while (state.username && state.savedSecret) {
            try {
                const payload = await apiFetch("/v1/events/poll?timeoutMs=25000", {
                    signal: state.pollAbort.signal,
                });
                for (const signal of payload.signals || []) {
                    try {
                        await handleSignal(signal);
                    } catch (signalError) {
                        console.error(signalError);
                    }
                }
            } catch (error) {
                if (state.pollAbort.signal.aborted) {
                    return;
                }
                handleNetworkError(error);
                await sleep(1500);
            }
        }
    };

    void loop();
}

async function refreshNodes() {
    const payload = await apiFetch("/v1/nodes");
    state.nodeUrls = (payload.nodes || []).map((node) => node.baseUrl).filter(Boolean);
    localStorage.setItem(STORAGE_KEYS.nodeUrls, JSON.stringify(state.nodeUrls));
    toast("Node list refreshed.", false, true);
}

async function deleteAccount() {
    if (!confirm("Delete this account from the mesh and clear this browser's cached credentials?")) {
        return;
    }
    await apiFetch("/v1/me/delete", { method: "POST", body: "{}" });
    clearAllLocalUserData();
    renderSignedOut();
    toast("Account deleted.", false, true);
}

function clearAllLocalUserData() {
    const keys = [];
    for (let index = 0; index < localStorage.length; index += 1) {
        const key = localStorage.key(index);
        if (!key) {
            continue;
        }
        if (key.startsWith("meshline.messages.") || key === sessionStorageKey()) {
            keys.push(key);
        }
    }
    keys.forEach((key) => localStorage.removeItem(key));

    clearSession();
    localStorage.removeItem(STORAGE_KEYS.secret);
    localStorage.removeItem(STORAGE_KEYS.username);
    localStorage.removeItem(STORAGE_KEYS.nodeUrls);
    localStorage.removeItem(STORAGE_KEYS.iceServers);

    state.savedSecret = "";
    state.username = "";
    state.currentPeer = "";
    state.peerProfiles = {};
    state.sessions = {};
}

function clearSession() {
    if (state.pollAbort) {
        state.pollAbort.abort();
    }
    clearInterval(state.heartbeatTimer);
    closeAllSessionTransports();
    localStorage.removeItem(STORAGE_KEYS.authToken);
    state.authToken = "";
}

function sendOfflineBeacon() {
    const token = localStorage.getItem(STORAGE_KEYS.authToken);
    if (!token) {
        return;
    }
    navigator.sendBeacon(`${state.activeNodeUrl}/v1/presence/offline?auth=${encodeURIComponent(token)}`, new Blob(["{}"], { type: "application/json" }));
}

async function showCurrentFingerprint() {
    const session = state.currentPeer ? ensureSession(state.currentPeer) : null;
    if (session?.pc && !hasVisibleFingerprint(session.fingerprint)) {
        await refreshFingerprint(state.currentPeer).catch(console.error);
    }
    els.fingerprintValue.textContent = session?.fingerprint || FINGERPRINT_UNAVAILABLE;
    els.fingerprintDialog.showModal();
}

async function refreshFingerprint(peer) {
    const session = ensureSession(peer);
    if (!session?.pc) {
        session.fingerprint = FINGERPRINT_UNAVAILABLE;
        return;
    }

    const { localFingerprint, remoteFingerprint } = await resolvePeerFingerprints(session.pc);

    if (!localFingerprint || !remoteFingerprint) {
        session.fingerprint = unsupportedFingerprintMessage();
        if (state.currentPeer === peer) {
            els.fingerprintValue.textContent = session.fingerprint;
        }
        return;
    }

    session.fingerprint = await buildFingerprintDigest(localFingerprint, remoteFingerprint);

    if (state.currentPeer === peer) {
        els.fingerprintValue.textContent = session.fingerprint;
    }
}

async function buildAuthToken() {
    if (state.authToken) {
        return state.authToken;
    }
    if (!state.username || !state.savedSecret) {
        return "";
    }
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(state.savedSecret));
    const hash = Array.from(new Uint8Array(digest))
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
    state.authToken = `${state.username}.${hash}`;
    localStorage.setItem(STORAGE_KEYS.authToken, state.authToken);
    return state.authToken;
}

async function apiFetch(path, options = {}) {
    return apiFetchAtBase(path, options, "", true);
}

async function apiFetchAtBase(path, options = {}, preferredBaseUrl = "", updateActiveNode = true) {
    const headers = new Headers(options.headers || {});
    if (!(options.body instanceof Blob)) {
        headers.set("Content-Type", "application/json");
    }

    const authToken = await buildAuthToken();
    if (authToken) {
        headers.set("Authorization", `Bearer ${authToken}`);
    }

    const orderedAttempts = [
        preferredBaseUrl,
        state.activeNodeUrl,
        ...state.nodeUrls,
    ].filter((url, index, urls) => url && urls.indexOf(url) === index);

    let lastError = null;
    for (const baseUrl of orderedAttempts) {
        try {
            const response = await fetch(`${baseUrl}${path}`, { ...options, headers });
            if (response.status === 401) {
                clearAllLocalUserData();
                renderSignedOut();
                throw new Error("Session expired.");
            }
            if (!response.ok) {
                const payload = await response.json().catch(() => ({ error: response.statusText }));
                throw new Error(payload.error || response.statusText);
            }
            if (updateActiveNode) {
                state.activeNodeUrl = baseUrl;
            }
            return await response.json();
        } catch (error) {
            lastError = error;
        }
    }

    throw lastError || new Error("Request failed");
}

function normalizeUsername(value) {
    return String(value || "").trim().toLowerCase();
}

function isFirefoxBrowser() {
    return /firefox/i.test(navigator.userAgent || "");
}

function hasVisibleFingerprint(value) {
    return Boolean(value) && value !== FINGERPRINT_UNAVAILABLE && !String(value).startsWith("Not supported");
}

function unsupportedFingerprintMessage() {
    return isFirefoxBrowser() ? "Not supported on Firefox" : FINGERPRINT_UNSUPPORTED;
}

function normalizeFingerprint(value) {
    return String(value || "").replaceAll(":", "").toUpperCase();
}

function extractFingerprintFromSdp(description) {
    if (!description?.sdp) {
        return "";
    }

    for (const line of description.sdp.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed.startsWith("a=fingerprint:")) {
            continue;
        }

        const parts = trimmed.split(" ");
        if (parts.length < 2) {
            continue;
        }

        return normalizeFingerprint(parts[1]);
    }

    return "";
}

async function resolvePeerFingerprints(pc) {
    let localFingerprint = "";
    let remoteFingerprint = "";

    try {
        const stats = await pc.getStats();
        const certs = new Map();

        stats.forEach((report) => {
            if (report.type === "certificate") {
                const fingerprint = normalizeFingerprint(report.fingerprint || report.fingerprintSha256 || "");
                if (fingerprint) {
                    certs.set(report.id, fingerprint);
                }
            }
        });

        stats.forEach((report) => {
            if (report.type !== "transport") {
                return;
            }
            localFingerprint = certs.get(report.localCertificateId) || localFingerprint;
            remoteFingerprint = certs.get(report.remoteCertificateId) || remoteFingerprint;
        });
    } catch (error) {
        console.error("Could not read WebRTC certificate stats:", error);
    }

    if (!localFingerprint || !remoteFingerprint) {
        localFingerprint = extractFingerprintFromSdp(pc.localDescription);
        remoteFingerprint = extractFingerprintFromSdp(pc.remoteDescription);
    }

    return { localFingerprint, remoteFingerprint };
}

async function buildFingerprintDigest(localFingerprint, remoteFingerprint) {
    const canonicalPair = [localFingerprint, remoteFingerprint].sort().join("|");
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonicalPair));
    return Array.from(new Uint8Array(digest))
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("")
        .slice(0, 24)
        .match(/.{1,4}/g)
        .join("-");
}

function normalizeBaseUrl(url) {
    return String(url || "").trim().replace(/\/+$/, "");
}

function normalizeIceServers(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
        return DEFAULT_ICE_SERVERS;
    }
    const normalized = entries
        .map((entry) => ({ urls: Array.isArray(entry.urls) ? entry.urls.filter(Boolean) : [] }))
        .filter((entry) => entry.urls.length > 0);
    return normalized.length > 0 ? normalized : DEFAULT_ICE_SERVERS;
}

function restoreSessionStatus(status, wantsConnection) {
    if (status === "connected" || status === "connecting") {
        return wantsConnection ? "connecting" : "disconnected";
    }
    return status || (wantsConnection ? "connecting" : "disconnected");
}

function toast(message, isError = false, isSuccess = false) {
    els.toast.textContent = message;
    els.toast.className = `toast ${isError ? "error" : isSuccess ? "success" : ""}`;
    setTimeout(() => {
        els.toast.className = "toast hidden";
    }, 3200);
}

function handleNetworkError(error) {
    console.error(error);
    toast(error.message || "Network error", true);
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
}

function safeJSONParse(value, fallback) {
    try {
        return value ? JSON.parse(value) : fallback;
    } catch (error) {
        return fallback;
    }
}

function setConversationPending(isPending, message = "Connecting...") {
    clearTimeout(state.conversationPendingTimer);
    els.newConversationInput.disabled = isPending;
    els.connectPeerBtn.disabled = isPending;
    els.connectPeerBtn.textContent = isPending ? "Starting..." : "Start chat";
    els.newConversationStatus.textContent = message;
    els.newConversationStatus.classList.toggle("hidden", !isPending);

    if (!isPending) {
        return;
    }

    state.conversationPendingTimer = setTimeout(() => {
        els.newConversationStatus.textContent = "Still connecting. Waiting for the peer node to answer...";
    }, 5000);
}

function peerSignalBaseUrl(peer) {
    const session = state.sessions[normalizeUsername(peer)];
    return normalizeBaseUrl(session?.currentNodeHttp || state.peerProfiles[peer]?.currentNodeHttp || "");
}

function buildPeerConnectionConfig() {
    return {
        iceServers: normalizeIceServers(state.iceServers),
        iceCandidatePoolSize: 1,
    };
}

function toggleMobilePane(mode) {
    if (window.innerWidth > 920) {
        els.sidebar.classList.remove("hidden-mobile");
        els.chatPane.classList.remove("hidden-mobile");
        return;
    }

    if (mode === "chat" || mode === "compose") {
        els.sidebar.classList.add("hidden-mobile");
        els.chatPane.classList.remove("hidden-mobile");
        return;
    }

    els.sidebar.classList.remove("hidden-mobile");
    els.chatPane.classList.add("hidden-mobile");
}
