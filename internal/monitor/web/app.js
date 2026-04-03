const feedList = document.getElementById('feed-list');
const meta = document.getElementById('meta');
const viewMode = document.getElementById('view-mode');
const levelFilter = document.getElementById('level-filter');
const logSearchInput = document.getElementById('log-search');
const nodeList = document.getElementById('node-list');
const nodeDetail = document.getElementById('node-detail');
const tabButtons = Array.from(document.querySelectorAll('.tab-button[data-tab-target]'));
const tabPanels = Array.from(document.querySelectorAll('.tab-panel[data-tab-panel]'));
const recordFilterInput = document.getElementById('record-filter');
const recordHealthFilter = document.getElementById('record-health-filter');
const refreshRecordsButton = document.getElementById('refresh-records');
const recordSummary = document.getElementById('record-summary');
const recordTableBody = document.getElementById('record-table-body');
const interactWriteNodeSelect = document.getElementById('interact-write-node');
const interactReadNodeSelect = document.getElementById('interact-read-node');
const interactWriteForm = document.getElementById('interact-write-form');
const interactWriteKeyInput = document.getElementById('interact-write-key');
const interactWriteValueInput = document.getElementById('interact-write-value');
const interactReadKeyInput = document.getElementById('interact-read-key');
const interactReadButton = document.getElementById('interact-read-button');
const interactStatus = document.getElementById('interact-status');
const interactReadResult = document.getElementById('interact-read-result');

const MAX_LOG_ITEMS = 15000;
const MAX_DOM_ITEMS = 1000;
const BOTTOM_SNAP_PX = 8;
const NODE_REFRESH_MS = 5000;
const RING_MODULUS = 18446744073709551616n;

let currentViewMode = 'human';
let currentLevelFilter = 'all';
let currentLogSearch = '';
let recordFilterText = '';
let recordHealthMode = 'all';
let activeTab = 'node-details';
let interactWriteNodeId = '';
let interactReadNodeId = '';
let nodeOpsBusy = false;

/* =============================
 * Section 1 - STATE
 * ============================= */

let state = Object.freeze({
    nodes: [],
    selectedNodeId: 'ALL_VNODES',
    logs: [],
    clusterRecords: []
});

function setState(patch) {
    const next = Object.freeze({ ...state, ...patch });
    const dirty = {
        feed: Object.prototype.hasOwnProperty.call(patch, 'logs'),
        nodeList: Object.prototype.hasOwnProperty.call(patch, 'nodes') || Object.prototype.hasOwnProperty.call(patch, 'selectedNodeId'),
        nodeDetail: Object.prototype.hasOwnProperty.call(patch, 'nodes')
            || Object.prototype.hasOwnProperty.call(patch, 'selectedNodeId'),
        cluster: Object.prototype.hasOwnProperty.call(patch, 'clusterRecords')
    };
    state = next;
    scheduleRender(dirty);
}

/* =============================
 * Section 2 - API
 * ============================= */

async function fetchState() {
    const response = await fetch('/api/state', { cache: 'no-store' });
    if (!response.ok) {
        throw new Error(`Failed to fetch state (${response.status})`);
    }
    return response.json();
}

async function writeRecord(nodeId, key, value) {
    const response = await fetch(`/api/nodes/${encodeURIComponent(nodeId)}/write`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, value })
    });
    if (!response.ok) {
        throw new Error(await response.text() || `Write failed (${response.status})`);
    }
    return response.json();
}

async function readRecord(nodeId, key) {
    const response = await fetch(`/api/nodes/${encodeURIComponent(nodeId)}/read?key=${encodeURIComponent(key)}`, { cache: 'no-store' });
    if (!response.ok) {
        throw new Error(await response.text() || `Read failed (${response.status})`);
    }
    return response.json();
}

async function fetchClusterRecords() {
    const response = await fetch('/api/debug/records', { cache: 'no-store' });
    if (!response.ok) {
        throw new Error(await response.text() || `Failed to load cluster activity (${response.status})`);
    }
    const records = await response.json();
    return Array.isArray(records) ? records : [];
}

async function runNodeLifecycle(action, nodeId) {
    const response = await fetch(`/api/ops/nodes/${encodeURIComponent(action)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nodeId })
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload?.ok === false) {
        throw new Error(payload?.error || payload?.output || `${action} failed (${response.status})`);
    }
    return payload;
}

function connectFeed(onSnapshot, onEvent, onReconnect) {
    let source = null;
    let closed = false;
    let reconnecting = false;

    const open = () => {
        if (closed) {
            return;
        }
        source = new EventSource('/api/stream');
        source.addEventListener('open', () => {
            reconnecting = false;
        });
        source.addEventListener('snapshot', (event) => {
            const payload = JSON.parse(event.data);
            onSnapshot(payload);
        });
        source.addEventListener('event', (event) => {
            const item = JSON.parse(event.data);
            onEvent(item);
        });
        source.onerror = () => {
            if (closed || reconnecting) {
                return;
            }
            reconnecting = true;
            onReconnect();
            if (source) {
                source.close();
            }
            setTimeout(() => {
                open();
            }, 2000);
        };
    };

    open();
    return () => {
        closed = true;
        reconnecting = false;
        if (source) {
            source.close();
        }
    };
}

/* =============================
 * Section 3 - RENDER
 * ============================= */

let framePending = false;
let pendingFlags = { feed: false, nodeList: false, nodeDetail: false, cluster: false };
let lastRenderedNodeId = '';

function scheduleRender(flags) {
    pendingFlags = {
        feed: pendingFlags.feed || Boolean(flags?.feed),
        nodeList: pendingFlags.nodeList || Boolean(flags?.nodeList),
        nodeDetail: pendingFlags.nodeDetail || Boolean(flags?.nodeDetail),
        cluster: pendingFlags.cluster || Boolean(flags?.cluster)
    };
    if (framePending) {
        return;
    }
    framePending = true;
    requestAnimationFrame(() => {
        framePending = false;
        const active = pendingFlags;
        pendingFlags = { feed: false, nodeList: false, nodeDetail: false, cluster: false };
        if (active.feed) {
            renderFeed();
        }
        if (active.nodeList) {
            renderNodeList();
        }
        if (active.nodeDetail) {
            renderNodeDetail();
        }
        if (active.cluster) {
            renderClusterTable();
        }
    });
}

function renderFeed() {
    if (!feedList || !meta) {
        return;
    }

    const shouldStickToBottom = isNearBottom();
    const visibleLogs = state.logs.filter((item) => passesLevelFilter(item, currentLevelFilter) && passesLogSearch(item, currentLogSearch));
    const renderItems = visibleLogs.slice(Math.max(0, visibleLogs.length - MAX_DOM_ITEMS));

    feedList.innerHTML = renderItems.map((item) => {
        const label = item.nodeId || item.source || 'monitor';
        const level = String(item.level || 'info').toUpperCase();
        const text = currentViewMode === 'raw' ? (item.raw || item.message) : (item.message || item.raw);
        const fields = item.fields ? JSON.stringify(item.fields, null, 2) : '{}';
        const detailsId = `details-${item.id ?? 'unknown'}`;
        return `<li class="log-item">`
            + `<details class="log-details" id="${detailsId}">`
            + `<summary class="log-summary">`
            + `<span class="log-time">${formatTime(item.time)}</span>`
            + `<span class="log-line"><span class="log-level level-${escapeHtml(String(item.level || 'info').toLowerCase())}">${escapeHtml(level)}</span> <span class="node-prefix">${escapeHtml(label)} | </span>${escapeHtml(text || '')}</span>`
            + `</summary>`
            + `<div class="log-detail-body">`
            + `<div><span class="detail-label">Event:</span> <code>${escapeHtml(item.event || 'raw.line')}</code></div>`
            + `<div><span class="detail-label">Level:</span> <code>${escapeHtml(item.level || 'info')}</code></div>`
            + `<div><span class="detail-label">Outcome:</span> <code>${escapeHtml(item.outcome || '-')}</code></div>`
            + `<div><span class="detail-label">Node:</span> <code>${escapeHtml(item.nodeId || '-')}</code></div>`
            + `<div><span class="detail-label">Source:</span> <code>${escapeHtml(item.source || '-')}</code></div>`
            + `<div class="detail-block"><div class="detail-label">Raw line</div><pre>${escapeHtml(item.raw || '')}</pre></div>`
            + `<div class="detail-block"><div class="detail-label">Parsed fields</div><pre>${escapeHtml(fields)}</pre></div>`
            + `</div>`
            + `</details>`
            + `</li>`;
    }).join('');

    meta.textContent = `Showing the latest ${Math.min(MAX_DOM_ITEMS, visibleLogs.length)} messages, out of ${state.logs.length} messages.`;

    if (shouldStickToBottom) {
        feedList.scrollTop = feedList.scrollHeight;
    }
}

function renderNodeList() {
    if (!nodeList) {
        return;
    }
    const createDisabled = nodeOpsBusy ? ' disabled' : '';
    const toolbar = `<div class="node-list-toolbar"><button class="ops-button node-add-button" type="button" data-node-action="create"${createDisabled}>+ New Node</button></div>`;
    if (!state.nodes.length) {
        nodeList.innerHTML = toolbar + '<p class="node-empty">No nodes observed yet.</p>';
        return;
    }
    const global = `<button class="node-card${state.selectedNodeId === 'ALL_VNODES' ? ' selected' : ''}" data-node-id="ALL_VNODES"><span class="node-card-id">Global</span></button>`;
    const items = state.nodes.map((node) => {
        const id = escapeHtml(node?.nodeId || 'unknown');
        const rawId = String(node?.nodeId || '');
        const badge = nodeStatusBadge(node?.status);
        const selected = node?.nodeId === state.selectedNodeId ? ' selected' : '';
        const disabled = nodeOpsBusy || !rawId ? ' disabled' : '';
        return `<div class="node-row"><button class="node-card${selected}" data-node-id="${id}"><span class="node-card-id">${id}</span><span class="node-network ${badge.className}">${badge.text}</span></button><button class="ops-button secondary node-shutdown-button" type="button" data-node-action="shutdown" data-node-id="${id}"${disabled}>Shutdown</button></div>`;
    }).join('');
    nodeList.innerHTML = toolbar + global + items;
    renderInteractNodeOptions();
}

function renderNodeDetail() {
    if (!nodeDetail) {
        return;
    }

    const selectedNodeId = state.selectedNodeId;
    const selectedChanged = selectedNodeId !== lastRenderedNodeId;

    if (selectedNodeId === 'ALL_VNODES') {
        const context = buildAllVnodesContext(state.nodes);
        if (selectedChanged) {
            nodeDetail.innerHTML = allVnodesMarkup(context);
        } else {
            patchNodeDetail(context, true);
        }
        lastRenderedNodeId = selectedNodeId;
        return;
    }

    const node = state.nodes.find((entry) => entry?.nodeId === selectedNodeId);
    if (!node) {
        lastRenderedNodeId = '';
        nodeDetail.innerHTML = '<p class="node-empty">No node selected.</p>';
        return;
    }

    const context = buildSingleNodeContext(node);
    if (selectedChanged) {
        nodeDetail.innerHTML = singleNodeMarkup(context);
    } else {
        patchNodeDetail(context, false);
    }
    lastRenderedNodeId = selectedNodeId;
}

function renderClusterTable() {
    if (!recordTableBody || !recordSummary) {
        return;
    }

    const filtered = state.clusterRecords.filter((record) => {
        const matchesText = !recordFilterText || String(record?.key || '').toLowerCase().includes(recordFilterText);
        const profile = classifyRecordProfile(record);
        return matchesText && (recordHealthMode === 'all' || recordHealthMode === profile.mode);
    });
    const writesSeen = state.clusterRecords.filter((record) => Number(record?.writeCount || 0) > 0).length;
    recordSummary.textContent = `Loaded ${state.clusterRecords.length} key(s). ${writesSeen} key(s) have observed writes.`;

    if (!filtered.length) {
        recordTableBody.innerHTML = '<tr><td colspan="7" class="node-empty-cell">No keys match the current filters.</td></tr>';
        return;
    }

    recordTableBody.innerHTML = filtered.map((record) => {
        const profile = classifyRecordProfile(record);
        const observedOn = (record?.nodes || []).map((node) => `${node.nodeId}:${node.lastOperation || '-'}`).join(', ');
        const placement = (record?.placement || []).map((node) => `${node.nodeId}:${node.role}`).join(', ');
        return `<tr><td><code>${escapeHtml(record?.key || '-')}</code></td><td>${escapeHtml(String(record?.nodeCount ?? 0))}</td><td>${escapeHtml(String(record?.readCount ?? 0))}</td><td>${escapeHtml(String(record?.writeCount ?? 0))}</td><td>${escapeHtml(String(record?.latestTimestamp ?? 0))}</td><td><span class="node-network ${profile.className}">${escapeHtml(profile.label)}</span> <span class="record-placement">${escapeHtml(placement)}</span></td><td class="record-value-cell">${escapeHtml(observedOn)}</td></tr>`;
    }).join('');
}

function patchNodeDetail(context, isGlobal) {
    const summary = nodeDetail.querySelector('[data-role="node-summary"]');
    if (summary) {
        summary.innerHTML = context.summary;
    }
    const pie = nodeDetail.querySelector('[data-role="range-pie"]');
    if (pie) {
        pie.innerHTML = context.pieHtml;
    }
    const localBody = nodeDetail.querySelector('tbody[data-role="local-records"]');
    if (localBody) {
        localBody.innerHTML = context.localRowsHtml;
    }
    const vnodeBody = nodeDetail.querySelector('tbody[data-role="vnode-rows"]');
    if (vnodeBody) {
        vnodeBody.innerHTML = context.vnodeRowsHtml;
    }
    if (isGlobal) {
        return;
    }
}

/* =============================
 * Section 4 - CONTROLLER
 * ============================= */

const logById = new Map();

if (viewMode) {
    viewMode.addEventListener('change', () => {
        currentViewMode = viewMode.value;
        scheduleRender({ feed: true });
    });
}

if (levelFilter) {
    levelFilter.addEventListener('change', () => {
        currentLevelFilter = levelFilter.value;
        scheduleRender({ feed: true });
    });
}

if (logSearchInput) {
    logSearchInput.addEventListener('input', () => {
        currentLogSearch = logSearchInput.value.toLowerCase().trim();
        scheduleRender({ feed: true });
    });
}

if (recordFilterInput) {
    recordFilterInput.addEventListener('input', () => {
        recordFilterText = recordFilterInput.value.toLowerCase().trim();
        scheduleRender({ cluster: true });
    });
}

if (recordHealthFilter) {
    recordHealthFilter.addEventListener('change', () => {
        recordHealthMode = recordHealthFilter.value;
        scheduleRender({ cluster: true });
    });
}

if (refreshRecordsButton) {
    refreshRecordsButton.addEventListener('click', () => {
        refreshClusterData(true);
    });
}

if (nodeList) {
    nodeList.addEventListener('click', (event) => {
        const actionButton = event.target.closest('button[data-node-action]');
        if (actionButton) {
            const action = actionButton.getAttribute('data-node-action') || '';
            if (action === 'create') {
                void createNodeFromList();
            } else if (action === 'shutdown') {
                const nodeId = actionButton.getAttribute('data-node-id') || '';
                if (nodeId) {
                    void shutdownNodeFromList(nodeId);
                }
            }
            return;
        }
        const button = event.target.closest('button[data-node-id]');
        if (!button) {
            return;
        }
        const nodeId = button.getAttribute('data-node-id') || '';
        setState({ selectedNodeId: nodeId });
    });
}

function guessNextNodeId() {
    let max = 0;
    for (const node of state.nodes) {
        const id = String(node?.nodeId || '').trim().toLowerCase();
        const match = /^node(\d+)$/.exec(id);
        if (!match) {
            continue;
        }
        const value = Number.parseInt(match[1], 10);
        if (Number.isFinite(value) && value > max) {
            max = value;
        }
    }
    return `node${max + 1}`;
}

function setNodeOpsPending(isBusy) {
    nodeOpsBusy = isBusy;
    scheduleRender({ nodeList: true });
}

async function createNodeFromList() {
    if (nodeOpsBusy) {
        return;
    }
    const nodeId = guessNextNodeId();
    if (!window.confirm(`Start a new node container as ${nodeId}?`)) {
        return;
    }
    setNodeOpsPending(true);
    try {
        await runNodeLifecycle('start', nodeId);
        await refreshNodeState();
        setTimeout(() => {
            refreshNodeState();
        }, 1500);
    } catch (error) {
        window.alert(error?.message || `Failed to start ${nodeId}.`);
    } finally {
        setNodeOpsPending(false);
    }
}

async function shutdownNodeFromList(nodeId) {
    if (nodeOpsBusy) {
        return;
    }
    if (!window.confirm(`Shutdown node ${nodeId}?`)) {
        return;
    }
    setNodeOpsPending(true);
    try {
        await runNodeLifecycle('stop', nodeId);
        await refreshNodeState();
    } catch (error) {
        window.alert(error?.message || `Failed to stop ${nodeId}.`);
    } finally {
        setNodeOpsPending(false);
    }
}

if (interactWriteNodeSelect) {
    interactWriteNodeSelect.addEventListener('change', () => {
        interactWriteNodeId = interactWriteNodeSelect.value;
    });
}

if (interactReadNodeSelect) {
    interactReadNodeSelect.addEventListener('change', () => {
        interactReadNodeId = interactReadNodeSelect.value;
    });
}

if (interactWriteForm) {
    interactWriteForm.addEventListener('submit', async (event) => {
        event.preventDefault();
        const nodeId = interactWriteNodeId;
        const key = interactWriteKeyInput?.value?.trim() || '';
        const value = interactWriteValueInput?.value || '';
        if (!nodeId || !key) {
            if (interactStatus) {
                interactStatus.textContent = 'Select a target node and provide a write key.';
            }
            return;
        }
        await submitWrite(nodeId, key, value);
    });
}

if (interactReadButton) {
    interactReadButton.addEventListener('click', async () => {
        const nodeId = interactReadNodeId;
        const key = interactReadKeyInput?.value?.trim() || '';
        if (!nodeId || !key) {
            if (interactStatus) {
                interactStatus.textContent = 'Select a target node and provide a read key.';
            }
            return;
        }
        await submitRead(nodeId, key);
    });
}

for (const button of tabButtons) {
    button.addEventListener('click', () => {
        const target = button.getAttribute('data-tab-target') || '';
        setActiveTab(target);
    });
}

async function refreshNodeState() {
    try {
        const payload = await fetchState();
        applyNodesFromState(payload);
    } catch {
        // Ignore transient polling errors while live stream keeps updating.
    }
}

function applyNodesFromState(payload) {
    const nodes = Array.isArray(payload?.nodes) ? payload.nodes.slice() : [];
    nodes.sort((a, b) => String(a?.nodeId || '').localeCompare(String(b?.nodeId || '')));
    const selected = state.selectedNodeId;
    const selectedIsGlobal = selected === 'ALL_VNODES';
    const hasSelected = selected && (selectedIsGlobal || nodes.some((node) => node?.nodeId === selected));
    const nextSelected = hasSelected ? selected : 'ALL_VNODES';
    setState({
        nodes,
        selectedNodeId: nextSelected
    });
}

async function refreshClusterData(showLoadingMessage) {
    if (refreshRecordsButton) {
        refreshRecordsButton.disabled = true;
    }
    if (showLoadingMessage && recordSummary) {
        recordSummary.textContent = 'Loading cluster activity...';
    }
    try {
        const records = await fetchClusterRecords();
        setState({ clusterRecords: records });
    } catch (error) {
        if (recordSummary) {
            recordSummary.textContent = error?.message || 'Failed to load cluster activity.';
        }
    } finally {
        if (refreshRecordsButton) {
            refreshRecordsButton.disabled = false;
        }
    }
}

async function submitWrite(nodeId, key, value) {
    if (interactStatus) {
        interactStatus.textContent = `Writing ${key}...`;
    }
    try {
        const result = await writeRecord(nodeId, key, value);
        if (interactStatus) {
            interactStatus.textContent = `Write ok for ${result.key} at timestamp ${result.timestamp}.`;
        }
        await refreshNodeState();
        await refreshClusterData(false);
    } catch (error) {
        if (interactStatus) {
            interactStatus.textContent = error?.message || 'Write failed.';
        }
    }
}

async function submitRead(nodeId, key) {
    if (interactStatus) {
        interactStatus.textContent = `Reading ${key}...`;
    }
    try {
        const result = await readRecord(nodeId, key);
        if (interactStatus) {
            interactStatus.textContent = result?.found ? `Read ok for ${key}.` : `${key} not found.`;
        }
        if (interactReadResult) {
            interactReadResult.innerHTML = renderReadResult(result);
        }
    } catch (error) {
        if (interactStatus) {
            interactStatus.textContent = error?.message || 'Read failed.';
        }
    }
}

const stopFeed = connectFeed(
    (payload) => {
        const snapshotFeed = Array.isArray(payload?.state?.feed) ? payload.state.feed : [];
        const logs = sortLogs(trimLogs(snapshotFeed));
        logById.clear();
        for (const item of logs) {
            if (item && item.id != null) {
                logById.set(item.id, item);
            }
        }
        setState({ logs });
        applyNodesFromState(payload?.state || {});
    },
    (item) => {
        if (item && item.id != null && logById.has(item.id)) {
            return;
        }
        const logs = sortLogs(trimLogs(state.logs.concat(item)));
        if (item && item.id != null) {
            logById.set(item.id, item);
        }
        setState({ logs });
    },
    () => {
        // Reconnect state is intentionally internal to API section.
    }
);

window.addEventListener('beforeunload', () => {
    stopFeed();
});

scheduleRender({ feed: true, nodeList: true, nodeDetail: true, cluster: true });
refreshNodeState();
refreshClusterData(false);
setInterval(() => {
    refreshNodeState();
}, NODE_REFRESH_MS);
setInterval(() => {
    refreshClusterData(false);
}, NODE_REFRESH_MS * 2);

function buildSingleNodeContext(node) {
    const uniqueVnodes = dedupeVnodes(ownVnodesForNode(node)).sort(compareByPositionThenId);
    const ranges = buildGlobalRangeContext([{ ...node, vnodes: uniqueVnodes }]);
    const nodeId = String(node?.nodeId || 'unknown');
    const inventory = { records: Array.isArray(node?.records) ? node.records : [] };
    const activeCount = uniqueVnodes.filter((vnode) => vnode?.active).length;
    const inactiveCount = Math.max(0, uniqueVnodes.length - activeCount);
    const counts = nodeStatusCounts(state.nodes);
    const badge = nodeStatusBadge(node?.status);
    return {
        nodeId,
        nodeAddr: String(node?.nodeAddr || '-'),
        badge,
        summary: `Nodes (total/online/suspect/offline): <strong>${counts.total}/${counts.online}/${counts.suspect}/${counts.offline}</strong> | Virtual Nodes (total/active/inactive): <strong>${uniqueVnodes.length}/${activeCount}/${inactiveCount}</strong>`,
        pieHtml: renderRangePieChart(uniqueVnodes, `node-${safeDomId(nodeId)}-pie`, false),
        localRowsHtml: renderLocalRecordRows(inventory.records || []),
        vnodeRowsHtml: renderVNodeRows(uniqueVnodes, ranges, nodeId)
    };
}

function buildAllVnodesContext(nodes) {
    const canonicalNodes = buildCanonicalNodes(nodes);
    const vnodeMap = new Map();
    for (const node of canonicalNodes) {
        for (const vnode of node?.vnodes || []) {
            const key = String(vnode?.vnodeId || '');
            if (!key || vnodeMap.has(key)) {
                continue;
            }
            vnodeMap.set(key, { ...vnode, nodeId: node?.nodeId || 'unknown' });
        }
    }
    const allVnodes = Array.from(vnodeMap.values()).sort(compareByPositionThenId);
    const ranges = buildGlobalRangeContext(canonicalNodes);
    const activeCount = allVnodes.filter((vnode) => vnode?.active).length;
    const counts = nodeStatusCounts(state.nodes);
    return {
        summary: `Nodes (total/online/suspect/offline): <strong>${counts.total}/${counts.online}/${counts.suspect}/${counts.offline}</strong> | Virtual Nodes (total/active/inactive): <strong>${allVnodes.length}/${activeCount}/${Math.max(0, allVnodes.length - activeCount)}</strong>`,
        pieHtml: renderRangePieChart(allVnodes, 'all-vnodes-pie', true),
        vnodeRowsHtml: renderVNodeRows(allVnodes, ranges, '', true),
        localRowsHtml: '',
        readResultHtml: '',
        statusText: ''
    };
}

function singleNodeMarkup(context) {
    const nodeIdEscaped = escapeHtml(context.nodeId);
    return `<div class="node-detail-head">`
        + `<div><h2>${nodeIdEscaped}</h2><p>${escapeHtml(context.nodeAddr)}</p></div>`
        + `<span class="node-network ${context.badge.className}">${context.badge.text}</span>`
        + `</div>`
        + `<p class="node-summary" data-role="node-summary">${context.summary}</p>`
        + `<div data-role="range-pie">${context.pieHtml}</div>`
        + `<div class="panel-subhead">Data Activity</div>`
        + `<div class="node-table-wrap"><table class="node-table"><thead><tr><th>Key</th><th>Last Op</th><th>Reads</th><th>Writes</th><th>Virtual Node</th><th>Timestamp</th></tr></thead><tbody data-role="local-records">${context.localRowsHtml}</tbody></table></div>`
        + `<div class="node-table-wrap"><table class="node-table"><thead><tr><th>Virtual Node</th><th>Position</th><th>Range Start</th><th>Range End</th><th>Status</th></tr></thead><tbody data-role="vnode-rows">${context.vnodeRowsHtml}</tbody></table></div>`;
}

function allVnodesMarkup(context) {
    return `<div class="node-detail-head"><div><h2>Global</h2></div><span class="node-network net-active">GLOBAL</span></div>`
        + `<p class="node-summary" data-role="node-summary">${context.summary}</p>`
        + `<div data-role="range-pie">${context.pieHtml}</div>`
        + `<div class="node-table-wrap"><table class="node-table"><thead><tr><th>Virtual Node</th><th>Node</th><th>Position</th><th>Range Start</th><th>Range End</th><th>Status</th></tr></thead><tbody data-role="vnode-rows">${context.vnodeRowsHtml || '<tr><td colspan="6" class="node-empty-cell">No vnode data yet.</td></tr>'}</tbody></table></div>`;
}

function renderVNodeRows(vnodes, ranges, nodeId, includeNodeColumn) {
    if (!vnodes.length) {
        return includeNodeColumn
            ? '<tr><td colspan="6" class="node-empty-cell">No vnode data yet.</td></tr>'
            : '<tr><td colspan="5" class="node-empty-cell">No vnode data yet.</td></tr>';
    }
    return vnodes.map((vnode) => {
        const vnodeId = escapeHtml(vnode?.vnodeId || '-');
        const key = `${vnode?.nodeId || nodeId || ''}|${vnode?.vnodeId || ''}`;
        const range = ranges.get(key);
        const start = escapeHtml(range?.start || vnode?.rangeStart || '-');
        const end = escapeHtml(range?.end || vnode?.rangeEnd || '-');
        const statusText = vnode?.active ? 'ACTIVE' : 'INACTIVE';
        const statusClass = vnode?.active ? 'net-active' : 'net-inactive';
        const cols = includeNodeColumn
            ? `<td><code>${escapeHtml(vnode?.nodeId || '-')}</code></td>`
            : '';
        return `<tr><td><code>${vnodeId}</code></td>${cols}<td><code>${escapeHtml(vnode?.position || '-')}</code></td><td><code>${start}</code></td><td><code>${end}</code></td><td><span class="node-network ${statusClass}">${statusText}</span></td></tr>`;
    }).join('');
}

function buildCanonicalNodes(sourceNodes) {
    return (sourceNodes || []).map((node) => ({ ...node, vnodes: dedupeVnodes(ownVnodesForNode(node)) }));
}

function dedupeVnodes(vnodes) {
    const seen = new Set();
    return (vnodes || []).filter((vnode) => {
        const id = String(vnode?.vnodeId || '');
        if (!id || seen.has(id)) {
            return false;
        }
        seen.add(id);
        return true;
    });
}

function ownVnodesForNode(node) {
    const all = Array.isArray(node?.vnodes) ? node.vnodes.slice() : [];
    const nodeId = String(node?.nodeId || '');
    return nodeId ? all.filter((vnode) => String(vnode?.vnodeId || '').startsWith(nodeId + ':')) : all;
}

function compareByPositionThenId(a, b) {
    const apos = toBigIntPosition(a?.position);
    const bpos = toBigIntPosition(b?.position);
    if (apos != null && bpos != null && apos !== bpos) {
        return apos < bpos ? -1 : 1;
    }
    if (apos != null && bpos == null) {
        return -1;
    }
    if (apos == null && bpos != null) {
        return 1;
    }
    return String(a?.vnodeId || '').localeCompare(String(b?.vnodeId || ''));
}

function renderRangePieChart(vnodes, chartId, showNodeLegend) {
    const segments = buildRangeSegments(vnodes);
    if (!segments.length) {
        return '<div class="range-pie-card"><p class="range-pie-empty">Range pie unavailable until vnode positions are observed.</p></div>';
    }
    const cx = 80;
    const cy = 80;
    const outer = 86;
    const inner = 36;
    let start = 0;
    const arcs = [];
    for (const segment of segments) {
        const sweep = Number(segment.ratio) * 360;
        const end = start + sweep;
        arcs.push(sweep >= 359.99
            ? `<circle cx="${cx}" cy="${cy}" r="${outer}" fill="${segment.color}" class="range-pie-segment"/>`
            : `<path d="${describeDonutSlice(cx, cy, outer, inner, start, end)}" fill="${segment.color}" class="range-pie-segment"/>`);
        start = end;
    }
    const legend = segments.map((segment) => {
        const label = showNodeLegend
            ? `${escapeHtml(segment.vnodeId)} @ ${escapeHtml(segment.nodeId)}`
            : escapeHtml(segment.vnodeId);
        return `<li class="range-legend-row"><span class="range-chip" style="background:${segment.color}"></span><span class="range-legend-label">${label}</span><span class="range-legend-size">${(Number(segment.ratio) * 100).toFixed(2)}%</span></li>`;
    }).join('');
    return `<div class="range-pie-card"><div class="range-pie-head"><h3>Ring Distribution</h3><p>Ordered around the ring by vnode position.</p></div><div class="range-pie-layout"><svg id="${chartId}" class="range-pie" viewBox="0 0 160 160" role="img" aria-label="Vnode range pie chart">${arcs.join('')}<circle cx="${cx}" cy="${cy}" r="${inner}" class="range-pie-core"></circle></svg><ul class="range-legend">${legend}</ul></div></div>`;
}

function buildRangeSegments(vnodes) {
    const items = (vnodes || []).map((vnode) => ({ vnode, position: toBigIntPosition(vnode?.position) })).filter((item) => item.position != null);
    if (!items.length) {
        return [];
    }
    if (items.length === 1) {
        const only = items[0].vnode;
        return [{ vnodeId: String(only?.vnodeId || 'unknown'), nodeId: String(only?.nodeId || 'unknown'), color: colorForVNode(only), ratio: 1 }];
    }
    items.sort((a, b) => (a.position < b.position ? -1 : (a.position > b.position ? 1 : 0)));
    return items.map((current, i) => {
        const previous = items[(i - 1 + items.length) % items.length];
        const span = modPositive(current.position - previous.position, RING_MODULUS);
        const ratio = Number(span) / Number(RING_MODULUS);
        return {
            vnodeId: String(current.vnode?.vnodeId || 'unknown'),
            nodeId: String(current.vnode?.nodeId || 'unknown'),
            color: colorForVNode(current.vnode),
            ratio: Number.isFinite(ratio) ? ratio : 0
        };
    }).filter((segment) => segment.ratio > 0);
}

function buildGlobalRangeContext(allNodes) {
    const ranges = new Map();
    const active = [];
    const all = [];
    for (const node of allNodes || []) {
        for (const vnode of node?.vnodes || []) {
            const pos = toBigIntPosition(vnode?.position);
            if (pos == null) {
                continue;
            }
            const item = { key: `${node?.nodeId || ''}|${vnode?.vnodeId || ''}`, position: pos, text: String(vnode?.position || ''), active: Boolean(vnode?.active) };
            all.push(item);
            if (item.active) {
                active.push(item);
            }
        }
    }
    if (!active.length) {
        return ranges;
    }
    active.sort((a, b) => (a.position < b.position ? -1 : (a.position > b.position ? 1 : 0)));
    for (const vnode of all) {
        const prev = previousActiveForPosition(active, vnode.position);
        if (prev) {
            ranges.set(vnode.key, { start: prev.text, end: vnode.text });
        }
    }
    return ranges;
}

function previousActiveForPosition(sortedActive, targetPosition) {
    let previous = sortedActive[sortedActive.length - 1];
    for (const candidate of sortedActive) {
        if (candidate.position < targetPosition) {
            previous = candidate;
            continue;
        }
        break;
    }
    return previous;
}

function renderLocalRecordRows(records) {
    if (!records.length) {
        return '<tr><td colspan="6" class="node-empty-cell">No read or write activity observed for this node yet.</td></tr>';
    }
    return records.map((record) => `<tr><td><code>${escapeHtml(record?.key || '-')}</code></td><td>${escapeHtml(record?.lastOperation || '-')}</td><td>${escapeHtml(String(record?.readCount ?? 0))}</td><td>${escapeHtml(String(record?.writeCount ?? 0))}</td><td><code>${escapeHtml(record?.vnodeId || '-')}</code></td><td>${escapeHtml(String(record?.timestamp ?? '-'))}</td></tr>`).join('');
}

function nodeStatusBadge(status) {
    const normalized = String(status || '').toLowerCase();
    if (normalized === 'online') {
        return { text: 'ONLINE', className: 'net-active' };
    }
    if (normalized === 'suspect') {
        return { text: 'SUSPECT', className: 'net-warn' };
    }
    return { text: 'OFFLINE', className: 'net-inactive' };
}

function nodeStatusCounts(nodes) {
    const counts = { total: 0, online: 0, suspect: 0, offline: 0 };
    for (const node of nodes || []) {
        counts.total += 1;
        const status = String(node?.status || '').toLowerCase();
        if (status === 'online') {
            counts.online += 1;
            continue;
        }
        if (status === 'suspect') {
            counts.suspect += 1;
            continue;
        }
        counts.offline += 1;
    }
    return counts;
}

function renderReadResult(result) {
    if (!result) {
        return '';
    }
    if (!result.found) {
        return `<div class="read-result"><strong>Read result:</strong> not found for <code>${escapeHtml(result.key || '')}</code></div>`;
    }
    return `<div class="read-result"><strong>Read result:</strong> <code>${escapeHtml(result.key || '')}</code> = <code>${escapeHtml(result.value || '')}</code></div>`;
}

function classifyRecordProfile(record) {
    const writes = Number(record?.writeCount || 0);
    return writes > 0
        ? { mode: 'write', label: 'WRITE', className: 'net-active' }
        : { mode: 'read-only', label: 'READ', className: 'net-warn' };
}

function trimLogs(source) {
    return source.length > MAX_LOG_ITEMS ? source.slice(source.length - MAX_LOG_ITEMS) : source;
}

function sortLogs(source) {
    return source.slice().sort((a, b) => {
        const aid = a?.id ?? 0;
        const bid = b?.id ?? 0;
        if (aid !== bid) {
            return aid - bid;
        }
        return Date.parse(a?.time || 0) - Date.parse(b?.time || 0);
    });
}

function toBigIntPosition(value) {
    if (value == null || value === '') {
        return null;
    }
    try {
        return BigInt(String(value));
    } catch {
        return null;
    }
}

function modPositive(value, modulus) {
    const result = value % modulus;
    return result >= 0 ? result : result + modulus;
}

function colorForVNode(vnode) {
    const input = `${vnode?.nodeId || ''}|${vnode?.vnodeId || ''}`;
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = ((hash * 31) + input.charCodeAt(i)) >>> 0;
    }
    return `hsl(${hash % 360} 68% 56%)`;
}

function describeDonutSlice(cx, cy, rOuter, rInner, startDeg, endDeg) {
    const startOuter = polarToCartesian(cx, cy, rOuter, endDeg);
    const endOuter = polarToCartesian(cx, cy, rOuter, startDeg);
    const startInner = polarToCartesian(cx, cy, rInner, endDeg);
    const endInner = polarToCartesian(cx, cy, rInner, startDeg);
    const large = endDeg - startDeg > 180 ? 1 : 0;
    return [`M ${startOuter.x} ${startOuter.y}`, `A ${rOuter} ${rOuter} 0 ${large} 0 ${endOuter.x} ${endOuter.y}`, `L ${endInner.x} ${endInner.y}`, `A ${rInner} ${rInner} 0 ${large} 1 ${startInner.x} ${startInner.y}`, 'Z'].join(' ');
}

function polarToCartesian(cx, cy, radius, angleDeg) {
    const radians = ((angleDeg - 90) * Math.PI) / 180;
    return { x: cx + (radius * Math.cos(radians)), y: cy + (radius * Math.sin(radians)) };
}

function safeDomId(value) {
    return String(value).replace(/[^a-zA-Z0-9_-]/g, '-');
}

function passesLevelFilter(item, filter) {
    return filter === 'all' ? true : levelRank(item?.level) >= levelRank(filter);
}

function passesLogSearch(item, query) {
    if (!query) {
        return true;
    }
    const fieldText = item?.fields ? JSON.stringify(item.fields) : '';
    const haystack = [
        item?.message,
        item?.raw,
        item?.source,
        item?.nodeId,
        item?.event,
        item?.outcome,
        fieldText
    ].map((value) => String(value || '').toLowerCase()).join('\n');
    return haystack.includes(query);
}

function levelRank(level) {
    switch (String(level || '').toLowerCase()) {
    case 'debug': return 10;
    case 'info': return 20;
    case 'warn':
    case 'warning': return 30;
    case 'error': return 40;
    default: return 20;
    }
}

function isNearBottom() {
    if (!feedList) {
        return false;
    }
    return (feedList.scrollHeight - feedList.scrollTop - feedList.clientHeight) <= BOTTOM_SNAP_PX;
}

function formatTime(value) {
    return value ? new Date(value).toLocaleTimeString() : 'n/a';
}

function formatLevelFilter(filter) {
    switch (filter) {
    case 'debug': return 'Debug+';
    case 'info': return 'Info+';
    case 'warn': return 'Warn+';
    case 'error': return 'Error only';
    default: return 'All levels';
    }
}

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === 'function') {
        return window.CSS.escape(String(value));
    }
    return String(value).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
}

function renderInteractNodeOptions() {
    if (!interactWriteNodeSelect || !interactReadNodeSelect) {
        return;
    }
    const previousWrite = interactWriteNodeId;
    const previousRead = interactReadNodeId;
    const options = state.nodes.map((node) => {
        const id = String(node?.nodeId || '');
        if (!id) {
            return '';
        }
        return `<option value="${escapeHtml(id)}">${escapeHtml(id)}</option>`;
    }).filter(Boolean);
    interactWriteNodeSelect.innerHTML = options.length
        ? options.join('')
        : '<option value="">No nodes available</option>';
    interactReadNodeSelect.innerHTML = options.length
        ? options.join('')
        : '<option value="">No nodes available</option>';
    if (!options.length) {
        interactWriteNodeId = '';
        interactReadNodeId = '';
        interactWriteNodeSelect.disabled = true;
        interactReadNodeSelect.disabled = true;
        if (interactStatus) {
            interactStatus.textContent = 'No nodes available yet.';
        }
        return;
    }
    interactWriteNodeSelect.disabled = false;
    interactReadNodeSelect.disabled = false;
    const hasPreviousWrite = state.nodes.some((node) => String(node?.nodeId || '') === previousWrite);
    const hasPreviousRead = state.nodes.some((node) => String(node?.nodeId || '') === previousRead);
    interactWriteNodeId = hasPreviousWrite ? previousWrite : String(state.nodes[0]?.nodeId || '');
    interactReadNodeId = hasPreviousRead ? previousRead : String(state.nodes[0]?.nodeId || '');
    interactWriteNodeSelect.value = interactWriteNodeId;
    interactReadNodeSelect.value = interactReadNodeId;
    if (interactStatus && !interactStatus.textContent.includes('Writing') && !interactStatus.textContent.includes('Reading')) {
        interactStatus.textContent = '';
    }
}

function setActiveTab(tabName) {
    if (!tabName) {
        return;
    }
    activeTab = tabName;
    for (const button of tabButtons) {
        const on = button.getAttribute('data-tab-target') === tabName;
        button.classList.toggle('active', on);
        button.setAttribute('aria-selected', on ? 'true' : 'false');
    }
    for (const panel of tabPanels) {
        const on = panel.getAttribute('data-tab-panel') === tabName;
        panel.classList.toggle('active', on);
        panel.hidden = !on;
    }
}

setActiveTab(activeTab);
