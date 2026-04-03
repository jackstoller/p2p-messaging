const state = {
    sessionToken: localStorage.getItem("meshline.sessionToken") || "",
    username: localStorage.getItem("meshline.username") || "",
    savedSecret: localStorage.getItem("meshline.secret") || "",
    nodeUrls: JSON.parse(localStorage.getItem("meshline.nodeUrls") || "[]"),
    activeNodeUrl: location.origin,
    currentPeer: "",
    peerProfiles: {},
    pc: null,
    dc: null,
    heartbeatTimer: null,
    pollAbort: null,
    fingerprint: "Unavailable",
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
    messages: document.getElementById("messages"),
    messageInput: document.getElementById("messageInput"),
    profileUsername: document.getElementById("profileUsername"),
    profileNode: document.getElementById("profileNode"),
    fingerprintDialog: document.getElementById("fingerprintDialog"),
    fingerprintValue: document.getElementById("fingerprintValue"),
    toast: document.getElementById("toast"),
    sidebar: document.querySelector(".sidebar"),
    chatPane: document.querySelector(".chat-pane"),
};

document.getElementById("createAccountBtn").addEventListener("click", createAccount);
document.getElementById("newConversationBtn").addEventListener("click", () => openConversation("", true));
document.getElementById("connectPeerBtn").addEventListener("click", startPeerSession);
document.getElementById("sendBtn").addEventListener("click", sendMessage);
document.getElementById("backToListBtn").addEventListener("click", () => toggleMobilePane("list"));
document.getElementById("showFingerprintBtn").addEventListener("click", () => {
    els.fingerprintValue.textContent = state.fingerprint;
    els.fingerprintDialog.showModal();
});
document.getElementById("refreshNodesBtn").addEventListener("click", refreshNodes);
document.getElementById("deleteAccountBtn").addEventListener("click", deleteAccount);
window.addEventListener("beforeunload", () => sendOfflineBeacon());
document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
        sendOfflineBeacon();
    } else if (state.sessionToken) {
        void heartbeat();
    }
});

init();

async function init() {
    if (state.sessionToken) {
        try {
            const me = await apiFetch("/v1/me");
            applySession(me.user.username, state.savedSecret, me);
            return;
        } catch (error) {
            clearSession();
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

    const secret = await generateSecret();
    const authSalt = bufferToBase64Url(crypto.getRandomValues(new Uint8Array(16)));
    const authVerifier = await deriveVerifier(secret, authSalt);
    const response = await apiFetch("/v1/auth/register", {
        method: "POST",
        body: JSON.stringify({ username, authSalt, authVerifier }),
    });

    applySession(username, secret, response);
    toast("Account created.", false, true);
}

function applySession(username, secret, payload) {
    state.username = username;
    state.savedSecret = secret || state.savedSecret;
    state.sessionToken = payload.session || state.sessionToken;
    state.activeNodeUrl = payload.nodeBaseUrl || state.activeNodeUrl;
    state.nodeUrls = (payload.nodes || []).map((node) => node.baseUrl).filter(Boolean);

    localStorage.setItem("meshline.username", state.username);
    localStorage.setItem("meshline.secret", state.savedSecret);
    localStorage.setItem("meshline.sessionToken", state.sessionToken);
    localStorage.setItem("meshline.nodeUrls", JSON.stringify(state.nodeUrls));

    els.sidebarUser.textContent = state.username;
    els.profileUsername.textContent = state.username;
    updateNodeUI(payload.nodeBaseUrl || state.activeNodeUrl, payload.user?.isActive);
    els.authScreen.classList.add("hidden");
    els.appShell.classList.remove("hidden");

    renderConversationList();
    startHeartbeatLoop();
    startPollLoop();

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
}

function updateNodeUI(nodeUrl, active) {
    els.sidebarNode.textContent = nodeUrl ? `Node ${nodeUrl}` : "Node unknown";
    els.profileNode.textContent = nodeUrl ? `Connected to ${nodeUrl}` : "Connected node unknown";
    els.sidebarPresence.textContent = active ? "Active" : "Idle";
}

function getConversations() {
    const conversations = [];
    for (let index = 0; index < localStorage.length; index += 1) {
        const key = localStorage.key(index);
        const prefix = "meshline.messages.";
        if (!key || !key.startsWith(prefix)) {
            continue;
        }
        const parts = key.split(".");
        if (parts.length !== 4 || !parts.includes(state.username)) {
            continue;
        }
        const peer = parts[2] === state.username ? parts[3] : parts[2];
        const messages = JSON.parse(localStorage.getItem(key) || "[]");
        if (messages.length === 0) {
            continue;
        }
        const last = messages[messages.length - 1];
        conversations.push({ peer, lastText: last.text, lastAt: last.at });
    }
    conversations.sort((a, b) => b.lastAt - a.lastAt);
    return conversations;
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
        const item = document.createElement("button");
        item.className = `conversation-item ${conversation.peer === state.currentPeer ? "active" : ""}`;
        item.innerHTML = `
            <span class="conversation-row">
                <strong class="conversation-name">${escapeHtml(conversation.peer)}</strong>
                <span class="conversation-time">${new Date(conversation.lastAt).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}</span>
            </span>
            <span class="conversation-preview">${escapeHtml(conversation.lastText)}</span>
        `;
        item.addEventListener("click", () => openConversation(conversation.peer, false));
        els.conversationList.appendChild(item);
    });
}

function openConversation(peer, showComposer) {
    state.currentPeer = peer;
    renderConversationList();
    els.newConversationPanel.classList.toggle("hidden", !showComposer);
    if (!peer) {
        els.chatTitle.textContent = "New conversation";
        els.chatSubtitle.textContent = "Start a new chat by username.";
        els.peerStatusDot.className = "status-dot";
        els.messages.innerHTML = `<div class="empty-state">Choose a username to begin a conversation.</div>`;
        toggleMobilePane("compose");
        return;
    }

    const profile = state.peerProfiles[peer];
    els.chatTitle.textContent = peer;
    els.chatSubtitle.textContent = profile ? (profile.isActive ? "Online now" : "Recently offline") : "Conversation";
    els.peerStatusDot.className = `status-dot ${profile?.isActive ? "online" : ""}`;
    renderMessages(loadConversation(peer));
    toggleMobilePane("chat");
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
    } else {
        els.sidebar.classList.remove("hidden-mobile");
        els.chatPane.classList.add("hidden-mobile");
    }
}

async function startPeerSession() {
    const peer = normalizeUsername(els.newConversationInput.value);
    if (!peer) {
        toast("Enter a username to start chatting.", true);
        return;
    }
    const payload = await apiFetch("/v1/peers/connect", {
        method: "POST",
        body: JSON.stringify({ username: peer }),
    });
    state.peerProfiles[peer] = payload.target;
    openConversation(peer, false);
    await preparePeerConnection(peer, true);
    const offer = await state.pc.createOffer();
    await state.pc.setLocalDescription(offer);
    await sendSignal("offer", peer, offer);
    els.newConversationInput.value = "";
}

async function preparePeerConnection(peer, isCaller) {
    if (state.pc) {
        state.pc.close();
    }
    state.currentPeer = peer;
    state.fingerprint = "Unavailable";
    state.pc = new RTCPeerConnection({
        iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
    });

    state.pc.onicecandidate = (event) => {
        if (event.candidate) {
            void sendSignal("ice-candidate", peer, event.candidate);
        }
    };

    state.pc.onconnectionstatechange = () => {
        const profile = state.peerProfiles[peer] || { username: peer };
        profile.isActive = state.pc.connectionState === "connected" || profile.isActive;
        state.peerProfiles[peer] = profile;
        openConversation(peer, false);
        if (state.pc.connectionState === "connected") {
            void refreshFingerprint();
        }
    };

    if (isCaller) {
        state.dc = state.pc.createDataChannel("meshline-chat");
        bindDataChannel(peer);
    } else {
        state.pc.ondatachannel = (event) => {
            state.dc = event.channel;
            bindDataChannel(peer);
        };
    }
}

function bindDataChannel(peer) {
    state.dc.onopen = () => {
        void refreshFingerprint();
    };
    state.dc.onmessage = (event) => {
        persistMessage(peer, { from: peer, text: event.data, at: Date.now() });
        if (state.currentPeer === peer) {
            renderMessages(loadConversation(peer));
        }
        renderConversationList();
    };
}

async function handleSignal(signal) {
    const peer = signal.from;
    state.peerProfiles[peer] = state.peerProfiles[peer] || { username: peer, isActive: true };

    if (signal.type === "offer") {
        await preparePeerConnection(peer, false);
        await state.pc.setRemoteDescription(signal.payload);
        const answer = await state.pc.createAnswer();
        await state.pc.setLocalDescription(answer);
        await sendSignal("answer", peer, answer);
        openConversation(peer, false);
        return;
    }
    if (signal.type === "answer" && state.pc) {
        await state.pc.setRemoteDescription(signal.payload);
        await refreshFingerprint();
        return;
    }
    if (signal.type === "ice-candidate" && state.pc) {
        await state.pc.addIceCandidate(signal.payload);
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
    if (!state.dc || state.dc.readyState !== "open") {
        toast("The secure channel is not open yet.", true);
        return;
    }
    state.dc.send(text);
    persistMessage(state.currentPeer, { from: state.username, text, at: Date.now() });
    els.messageInput.value = "";
    renderMessages(loadConversation(state.currentPeer));
    renderConversationList();
}

function persistMessage(peer, message) {
    const messages = loadConversation(peer);
    messages.push(message);
    localStorage.setItem(conversationKey(peer), JSON.stringify(messages.slice(-200)));
}

function loadConversation(peer) {
    return JSON.parse(localStorage.getItem(conversationKey(peer)) || "[]");
}

function conversationKey(peer) {
    const ids = [state.username, peer].sort();
    return `meshline.messages.${ids[0]}.${ids[1]}`;
}

function renderMessages(messages) {
    if (!messages.length) {
        els.messages.innerHTML = `<div class="empty-state">No messages yet. Say hello.</div>`;
        return;
    }
    els.messages.innerHTML = messages.map((message) => `
        <div class="message ${message.from === state.username ? "outgoing" : ""}">
            <div>${escapeHtml(message.text)}</div>
            <div class="message-meta">${escapeHtml(message.from)} · ${new Date(message.at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}</div>
        </div>
    `).join("");
    els.messages.scrollTop = els.messages.scrollHeight;
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
    void heartbeat();
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
        while (state.sessionToken) {
            try {
                const payload = await apiFetch("/v1/events/poll?timeoutMs=25000", {
                    signal: state.pollAbort.signal,
                });
                for (const signal of payload.signals || []) {
                    await handleSignal(signal);
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
    localStorage.setItem("meshline.nodeUrls", JSON.stringify(state.nodeUrls));
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
        if (key && key.startsWith("meshline.messages.")) {
            keys.push(key);
        }
    }
    keys.forEach((key) => localStorage.removeItem(key));
    clearSession();
    localStorage.removeItem("meshline.secret");
    localStorage.removeItem("meshline.username");
    localStorage.removeItem("meshline.nodeUrls");
    state.savedSecret = "";
    state.username = "";
    state.currentPeer = "";
}

function clearSession() {
    if (state.pollAbort) {
        state.pollAbort.abort();
    }
    clearInterval(state.heartbeatTimer);
    localStorage.removeItem("meshline.sessionToken");
    state.sessionToken = "";
}

function sendOfflineBeacon() {
    if (!state.sessionToken) {
        return;
    }
    navigator.sendBeacon(`${state.activeNodeUrl}/v1/presence/offline?session=${encodeURIComponent(state.sessionToken)}`, new Blob(["{}"], { type: "application/json" }));
}

async function refreshFingerprint() {
    if (!state.pc) {
        state.fingerprint = "Unavailable";
        return;
    }
    const stats = await state.pc.getStats();
    const certs = new Map();
    let localFp = "";
    let remoteFp = "";

    stats.forEach((report) => {
        if (report.type === "certificate") {
            certs.set(report.id, normalizeFingerprint(report.fingerprint || report.fingerprintSha256 || ""));
        }
    });
    stats.forEach((report) => {
        if (report.type === "transport") {
            localFp = certs.get(report.localCertificateId) || localFp;
            remoteFp = certs.get(report.remoteCertificateId) || remoteFp;
        }
    });
    if (!localFp || !remoteFp) {
        state.fingerprint = "Unavailable";
        return;
    }
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode([localFp, remoteFp].sort().join("|")));
    state.fingerprint = Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 24).match(/.{1,4}/g).join("-");
}

async function deriveVerifier(secret, saltB64Url) {
    const secretBytes = new TextEncoder().encode(secret);
    const key = await crypto.subtle.importKey("raw", secretBytes, "PBKDF2", false, ["deriveBits"]);
    const bits = await crypto.subtle.deriveBits(
        { name: "PBKDF2", hash: "SHA-256", salt: base64UrlToBytes(saltB64Url), iterations: 200000 },
        key,
        256
    );
    return bufferToBase64Url(new Uint8Array(bits));
}

async function generateSecret() {
    return bufferToBase64Url(crypto.getRandomValues(new Uint8Array(32)));
}

async function apiFetch(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (!(options.body instanceof Blob)) {
        headers.set("Content-Type", "application/json");
    }
    if (state.sessionToken) {
        headers.set("Authorization", `Bearer ${state.sessionToken}`);
    }

    const attempts = [state.activeNodeUrl, ...state.nodeUrls.filter((url) => url && url !== state.activeNodeUrl)];
    let lastError = null;
    for (const baseUrl of attempts) {
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
            state.activeNodeUrl = baseUrl;
            const data = await response.json();
            return data;
        } catch (error) {
            lastError = error;
        }
    }
    throw lastError || new Error("Request failed");
}

function normalizeUsername(value) {
    return value.trim().toLowerCase();
}

function normalizeFingerprint(value) {
    return value.replaceAll(":", "").toUpperCase();
}

function bufferToBase64Url(bytes) {
    const binary = Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
    return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function base64UrlToBytes(value) {
    const padded = value.replaceAll("-", "+").replaceAll("_", "/").padEnd(Math.ceil(value.length / 4) * 4, "=");
    const binary = atob(padded);
    return Uint8Array.from(binary, (char) => char.charCodeAt(0));
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
