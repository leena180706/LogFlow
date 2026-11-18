
// ==================================================
// LOGFLOW - DISTRIBUTED LOG AGGREGATOR & INDEXER
// Dashboard JavaScript
// Automatic workload + dynamic worker monitoring
// ==================================================

let severityChart = null;
let serverChart = null;
let currentLogs = [];

let refreshInProgress = false;


// --------------------------------------------------
// SAFE HTML DISPLAY
// --------------------------------------------------

function escapeHTML(value) {

    return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
    })[char]);
}


// --------------------------------------------------
// SAFE NUMBER DISPLAY
// --------------------------------------------------

function safeNumber(value) {

    const number = Number(value);

    return Number.isFinite(number) ? number : 0;
}


// --------------------------------------------------
// API REQUEST WITH TIMEOUT
// --------------------------------------------------

async function apiRequest(url, options = {}, timeout = 15000) {

    const controller = new AbortController();

    const timeoutId = setTimeout(
        () => controller.abort(),
        timeout
    );

    try {

        const response = await fetch(url, {
            ...options,
            cache: "no-store",
            signal: controller.signal
        });

        return response;

    } finally {

        clearTimeout(timeoutId);

    }
}


// --------------------------------------------------
// LOAD STATISTICS
// --------------------------------------------------

async function loadStats() {

    try {

        const response = await apiRequest("/api/stats");

        if (!response.ok) {
            throw new Error(
                `Statistics API returned ${response.status}`
            );
        }

        const data = await response.json();

        const severity = data.severity_counts || {};
        const servers = data.server_counts || {};

        document.getElementById("totalLogs").textContent =
            safeNumber(data.total_logs);

        document.getElementById("infoLogs").textContent =
            safeNumber(severity.INFO);

        document.getElementById("warningLogs").textContent =
            safeNumber(severity.WARNING);

        document.getElementById("errorLogs").textContent =
            safeNumber(severity.ERROR);

        updateCharts(data);

    } catch (error) {

        console.error("[STATISTICS ERROR]", error);

    }
}


// --------------------------------------------------
// CHARTS
// --------------------------------------------------

function updateCharts(data) {

    const severity = data.severity_counts || {};
    const servers = data.server_counts || {};

    const severityData = [
        safeNumber(severity.INFO),
        safeNumber(severity.WARNING),
        safeNumber(severity.ERROR)
    ];

    const serverData = [
        safeNumber(servers["SERVER-1"]),
        safeNumber(servers["SERVER-2"]),
        safeNumber(servers["SERVER-3"])
    ];

    const severityCanvas =
        document.getElementById("severityChart");

    const serverCanvas =
        document.getElementById("serverChart");

    if (!severityCanvas || !serverCanvas) {
        return;
    }

    // Severity chart.
    if (!severityChart) {

        severityChart = new Chart(
            severityCanvas,
            {
                type: "doughnut",

                data: {
                    labels: [
                        "INFO",
                        "WARNING",
                        "ERROR"
                    ],

                    datasets: [{
                        data: severityData,

                        backgroundColor: [
                            "#3B82F6",
                            "#F59E0B",
                            "#EF4444"
                        ],

                        borderWidth: 3,
                        borderColor: "#FFFFFF"
                    }]
                },

                options: {
                    responsive: true,
                    maintainAspectRatio: false,

                    plugins: {
                        legend: {
                            position: "bottom"
                        }
                    }
                }
            }
        );

    } else {

        severityChart.data.datasets[0].data =
            severityData;

        severityChart.update("none");
    }

    // Server chart.
    if (!serverChart) {

        serverChart = new Chart(
            serverCanvas,
            {
                type: "bar",

                data: {
                    labels: [
                        "SERVER-1",
                        "SERVER-2",
                        "SERVER-3"
                    ],

                    datasets: [{
                        label: "Number of Logs",

                        data: serverData,

                        backgroundColor: [
                            "#2563EB",
                            "#60A5FA",
                            "#93C5FD"
                        ],

                        borderRadius: 7
                    }]
                },

                options: {
                    responsive: true,
                    maintainAspectRatio: false,

                    scales: {
                        y: {
                            beginAtZero: true,

                            ticks: {
                                precision: 0
                            }
                        }
                    }
                }
            }
        );

    } else {

        serverChart.data.datasets[0].data =
            serverData;

        serverChart.update("none");
    }
}


// --------------------------------------------------
// LIVE SYSTEM MONITOR
// --------------------------------------------------

async function loadSystemStatus() {

    try {

        const response = await apiRequest(
            "/api/system",
            {},
            20000
        );

        if (!response.ok) {

            throw new Error(
                `System API returned ${response.status}`
            );

        }

        const data = await response.json();

        const workers = data.workers || {};
        const queue = data.queue || {};
        const workload = data.workload || {};
        const publisher = data.publisher || {};

        // ------------------------------------------
        // WORKER METRICS
        // ------------------------------------------

        const activeWorkers =
            safeNumber(workers.active_workers);

        document.getElementById("activeWorkers").textContent =
            activeWorkers;

        document.getElementById("workerCountLabel").textContent =
            activeWorkers;

        document.getElementById("processedLogs").textContent =
            safeNumber(workers.total_processed);

        document.getElementById("failedLogs").textContent =
            safeNumber(workers.total_failed);

        // ------------------------------------------
        // QUEUE METRICS
        // ------------------------------------------

        document.getElementById("waitingMessages").textContent =
            safeNumber(queue.messages_waiting);

        document.getElementById("processingMessages").textContent =
            safeNumber(queue.messages_processing);

        // ------------------------------------------
        // WORKLOAD METRICS
        // ------------------------------------------

        document.getElementById("generatedLogs").textContent =
            safeNumber(workload.total_generated);

        document.getElementById("workloadRate").textContent =
            safeNumber(workload.current_rate);

        const phase = String(
            workload.current_phase || "STARTING"
        ).toUpperCase();

        document.getElementById("workloadPhase").textContent =
            phase;

        document.getElementById("workloadDescription").textContent =
            getPhaseDescription(phase);

        updatePhaseTrack(phase);

        // ------------------------------------------
        // ACTUAL WORKER DETAILS
        // ------------------------------------------

        updateWorkerCards(
            workers.workers || [],
            workers.active_workers || 0
        );

        // ------------------------------------------
        // SYSTEM STATUS
        // ------------------------------------------

        updateSystemIndicator(
            data.system_running,
            workers.running,
            queue.error,
            workers.last_error
        );

        // ------------------------------------------
        // OPTIONAL PUBLISHER METRICS
        // ------------------------------------------

        updatePublisherMetrics(publisher);

    } catch (error) {

        console.error(
            "[SYSTEM MONITOR ERROR]",
            error
        );

        const systemStatus =
            document.getElementById("systemStatus");

        if (systemStatus) {

            systemStatus.textContent =
                error.name === "AbortError"
                    ? "API TIMEOUT"
                    : "CONNECTION ERROR";

            systemStatus.className =
                "system-pill offline";
        }

        const sidebarText =
            document.getElementById("sidebarStatusText");

        const sidebarDot =
            document.getElementById("sidebarStatusDot");

        if (sidebarText) {
            sidebarText.textContent = "Connection Error";
        }

        if (sidebarDot) {
            sidebarDot.classList.remove("online");
        }
    }
}


// --------------------------------------------------
// SYSTEM STATUS INDICATOR
// --------------------------------------------------

function updateSystemIndicator(
    systemRunning,
    workersRunning,
    queueError,
    workerError
) {

    const systemStatus =
        document.getElementById("systemStatus");

    const sidebarText =
        document.getElementById("sidebarStatusText");

    const sidebarDot =
        document.getElementById("sidebarStatusDot");

    if (!systemStatus) return;

    const healthy =
        Boolean(systemRunning) &&
        Boolean(workersRunning) &&
        !queueError &&
        !workerError;

    if (healthy) {

        systemStatus.textContent = "SYSTEM RUNNING";
        systemStatus.className = "system-pill online";

        if (sidebarText) {
            sidebarText.textContent = "System Active";
        }

        if (sidebarDot) {
            sidebarDot.classList.add("online");
        }

    } else {

        systemStatus.textContent = "SYSTEM WARNING";
        systemStatus.className = "system-pill offline";

        if (sidebarText) {
            sidebarText.textContent = "System Warning";
        }

        if (sidebarDot) {
            sidebarDot.classList.remove("online");
        }
    }
}


// --------------------------------------------------
// PUBLISHER METRICS
// --------------------------------------------------

function updatePublisherMetrics(publisher) {

    // These metrics are available in the backend.
    // Existing dashboard layout is not modified.

    console.debug("[PUBLISHER METRICS]", {

        pending: safeNumber(publisher.pending_local),

        sent: safeNumber(publisher.total_sent),

        failed: safeNumber(publisher.total_send_failed),

        batches: safeNumber(publisher.total_batches)

    });
}


// --------------------------------------------------
// WORKLOAD PHASE DESCRIPTION
// --------------------------------------------------

function getPhaseDescription(phase) {

    const descriptions = {

        NORMAL:
            "Normal log generation. Minimum workers handle the workload.",

        INCREASING:
            "Incoming log volume is increasing. Workers monitor the backlog.",

        SPIKE:
            "A sudden increase in log generation is being simulated.",

        HEAVY:
            "Heavy workload. Additional workers can be created automatically.",

        DECREASING:
            "Workload is decreasing. Extra workers can be stopped.",

        STOPPED:
            "Automatic workload generation has stopped.",

        STARTING:
            "Initializing automatic workload generation."

    };

    return descriptions[phase] ||
        "Monitoring automatic workload.";
}


// --------------------------------------------------
// WORKLOAD PHASE TRACK
// --------------------------------------------------

function updatePhaseTrack(phase) {

    document.querySelectorAll(".phase-step").forEach(step => {

        step.classList.remove("phase-active");

        if (step.dataset.phase === phase) {

            step.classList.add("phase-active");

        }

    });
}


// --------------------------------------------------
// ACTUAL WORKER CARDS
// --------------------------------------------------

function updateWorkerCards(workers, activeCount) {

    const grid = document.getElementById("workerGrid");

    if (!grid) return;

    if (!Array.isArray(workers)) {
        workers = [];
    }

    if (workers.length === 0) {

        grid.innerHTML = `
            <div class="empty-worker">
                No active workers at the moment.
            </div>
        `;

        return;
    }

    const cards = workers.map(worker => {

        const state = String(
            worker.state || "idle"
        ).toUpperCase();

        const isActive =
            state !== "STOPPED" &&
            state !== "ERROR";

        const workerName =
            worker.name || `Worker ${worker.id}`;

        return `

            <div class="worker-card">

                <div class="worker-card-top">

                    <div class="worker-icon">⚙</div>

                    <span class="worker-live">

                        <span class="worker-dot"></span>

                        ${escapeHTML(state)}

                    </span>

                </div>

                <h3>${escapeHTML(workerName)}</h3>

                <p>Python processing thread</p>

                <div class="worker-card-bottom">

                    <span>Amazon SQS</span>

                    <strong>
                        ${isActive ? "Running" : "Stopped"}
                    </strong>

                </div>

                <div class="worker-card-bottom">

                    <span>Processed</span>

                    <strong>
                        ${safeNumber(worker.processed)}
                    </strong>

                </div>

                <div class="worker-card-bottom">

                    <span>Failed</span>

                    <strong>
                        ${safeNumber(worker.failed)}
                    </strong>

                </div>

            </div>

        `;

    });

    grid.innerHTML = cards.join("");

}


// --------------------------------------------------
// READ FILTER VALUES
// --------------------------------------------------

function getLogFilters() {

    const keywordElement =
        document.getElementById("keyword");

    const levelElement =
        document.getElementById("levelFilter");

    const serverElement =
        document.getElementById("serverFilter");

    const keyword = keywordElement
        ? keywordElement.value.trim()
        : "";

    let level = levelElement
        ? levelElement.value.trim().toUpperCase()
        : "";

    let server = serverElement
        ? serverElement.value.trim().toUpperCase()
        : "";

    if (
        ["ALL", "ALL LEVELS", "NONE"].includes(level)
    ) {
        level = "";
    }

    if (
        ["ALL", "ALL SERVERS", "NONE"].includes(server)
    ) {
        server = "";
    }

    return {
        keyword,
        level,
        server
    };
}


// --------------------------------------------------
// LOAD LOGS WITH FILTERS
// --------------------------------------------------

async function loadLogs() {

    try {

        const filters = getLogFilters();

        const params = new URLSearchParams();

        if (filters.keyword) {
            params.set("keyword", filters.keyword);
        }

        if (filters.level) {
            params.set("level", filters.level);
        }

        if (filters.server) {
            params.set("server", filters.server);
        }

        const query = params.toString();

        const response = await apiRequest(
            "/api/logs" + (query ? "?" + query : "")
        );

        if (!response.ok) {

            throw new Error(
                `Logs API returned ${response.status}`
            );

        }

        const data = await response.json();

        currentLogs = Array.isArray(data.logs)
            ? data.logs
            : [];

        const table =
            document.getElementById("logsTable");

        const countLabel =
            document.getElementById("logCountLabel");

        if (!table) return;

        if (countLabel) {

            countLabel.textContent =
                safeNumber(data.count) + " records";

        }

        if (currentLogs.length === 0) {

            table.innerHTML = `
                <tr>
                    <td colspan="6">
                        No matching logs found.
                    </td>
                </tr>
            `;

            return;
        }

        table.innerHTML = currentLogs.map(log => `

            <tr>

                <td>${safeNumber(log.id)}</td>

                <td>${escapeHTML(log.timestamp)}</td>

                <td>${escapeHTML(log.server)}</td>

                <td>

                    <span class="severity ${escapeHTML(log.level)}">

                        ${escapeHTML(log.level)}

                    </span>

                </td>

                <td>${escapeHTML(log.message)}</td>

                <td>

                    <button
                        class="view-btn"
                        onclick="showLogDetails(${safeNumber(log.id)})">

                        View

                    </button>

                </td>

            </tr>

        `).join("");

    } catch (error) {

        console.error(
            "[LOG LOADING ERROR]",
            error
        );

    }
}


// --------------------------------------------------
// SEARCH BUTTON
// --------------------------------------------------

function searchLogs() {

    loadLogs();

}


// --------------------------------------------------
// ERROR ALERTS
// --------------------------------------------------

async function loadErrorAlerts() {

    try {

        const response = await apiRequest(
            "/api/logs?level=ERROR"
        );

        if (!response.ok) {

            throw new Error(
                `Error API returned ${response.status}`
            );

        }

        const data = await response.json();

        const errors = Array.isArray(data.logs)
            ? data.logs.slice(0, 5)
            : [];

        const alertCount =
            document.getElementById("alertCount");

        const errorList =
            document.getElementById("errorList");

        if (alertCount) {

            alertCount.textContent =
                safeNumber(data.count) + " Errors";

        }

        if (!errorList) return;

        if (errors.length === 0) {

            errorList.innerHTML = `

                <p class="no-errors">

                    ✓ No errors detected. System looks good!

                </p>

            `;

            return;
        }

        errorList.innerHTML = errors.map(log => `

            <div class="error-item">

                <div class="error-top">

                    <strong>
                        🔴 ${escapeHTML(log.server)}
                    </strong>

                    <span>
                        ${escapeHTML(log.timestamp)}
                    </span>

                </div>

                <p>${escapeHTML(log.message)}</p>

                <small>
                    Log ID: ${safeNumber(log.id)}
                </small>

            </div>

        `).join("");

    } catch (error) {

        console.error(
            "[ERROR ALERT LOADING ERROR]",
            error
        );

    }
}


// --------------------------------------------------
// VIEW LOG DETAILS
// --------------------------------------------------

function showLogDetails(id) {

    const log = currentLogs.find(
        item => Number(item.id) === Number(id)
    );

    if (!log) {

        alert("Log details not found.");

        return;
    }

    const details =
        document.getElementById("logDetails");

    const modal =
        document.getElementById("logModal");

    if (!details || !modal) return;

    details.innerHTML = `

        <div class="detail-item">

            <label>Log ID</label>

            <strong>${safeNumber(log.id)}</strong>

        </div>

        <div class="detail-item">

            <label>Timestamp</label>

            <strong>
                ${escapeHTML(log.timestamp)}
            </strong>

        </div>

        <div class="detail-item">

            <label>Server</label>

            <strong>
                ${escapeHTML(log.server)}
            </strong>

        </div>

        <div class="detail-item">

            <label>Severity Level</label>

            <strong>

                <span class="severity ${escapeHTML(log.level)}">

                    ${escapeHTML(log.level)}

                </span>

            </strong>

        </div>

        <div class="detail-item">

            <label>Log Message</label>

            <strong>
                ${escapeHTML(log.message)}
            </strong>

        </div>

    `;

    modal.style.display = "flex";

}


// --------------------------------------------------
// CLOSE LOG DETAILS
// --------------------------------------------------

function closeLogDetails() {

    const modal =
        document.getElementById("logModal");

    if (modal) {
        modal.style.display = "none";
    }

}


// --------------------------------------------------
// CLOSE MODAL OUTSIDE
// --------------------------------------------------

function closeModalOutside(event) {

    if (event.target.id === "logModal") {

        closeLogDetails();

    }

}


// --------------------------------------------------
// EXPORT CSV
// --------------------------------------------------

function exportLogs() {

    if (currentLogs.length === 0) {

        alert("No logs available to export!");

        return;
    }

    const headers = [
        "ID",
        "Timestamp",
        "Server",
        "Severity",
        "Message"
    ];

    const rows = currentLogs.map(log => [

        log.id,
        log.timestamp,
        log.server,
        log.level,
        log.message

    ]);

    const csvContent = [headers, ...rows]

        .map(row => row.map(value =>

            `"${String(value ?? "").replace(/"/g, '""')}"`

        ).join(","))

        .join("\r\n");

    const blob = new Blob(

        ["\uFEFF" + csvContent],

        {
            type: "text/csv;charset=utf-8;"
        }

    );

    const url = URL.createObjectURL(blob);

    const link = document.createElement("a");

    link.href = url;

    link.download = "Distributed_Logs.csv";

    document.body.appendChild(link);

    link.click();

    link.remove();

    URL.revokeObjectURL(url);

}


// --------------------------------------------------
// CLEAR FILTERS
// --------------------------------------------------

function clearFilters() {

    const keyword =
        document.getElementById("keyword");

    const level =
        document.getElementById("levelFilter");

    const server =
        document.getElementById("serverFilter");

    if (keyword) keyword.value = "";

    if (level) level.value = "";

    if (server) server.value = "";

    loadLogs();

}


// --------------------------------------------------
// CLEAR DATABASE LOGS
// --------------------------------------------------

async function clearAllLogs() {

    const confirmed = confirm(

        "Clear all currently stored database logs? " +
        "New incoming logs will continue."

    );

    if (!confirmed) return;

    try {

        const response = await apiRequest(

            "/api/clear",

            {
                method: "POST"
            }

        );

        const data = await response.json();

        if (!response.ok) {

            throw new Error(
                data.error || "Unable to clear logs"
            );

        }

        alert(data.message);

        clearFilters();

        await refreshDashboard();

    } catch (error) {

        alert(error.message);

    }

}


// --------------------------------------------------
// REFRESH DASHBOARD
// --------------------------------------------------

async function refreshDashboard() {

    if (refreshInProgress) return;

    refreshInProgress = true;

    try {

        await Promise.all([

            loadStats(),

            loadSystemStatus(),

            loadLogs(),

            loadErrorAlerts()

        ]);

    } finally {

        refreshInProgress = false;

    }

}


// --------------------------------------------------
// DASHBOARD STARTUP
// --------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {

    console.log("[LOGFLOW] Dashboard initialized.");

    // Search button.
    const searchButton = document.querySelector(
        'button[onclick="loadLogs()"], ' +
        'button[onclick="searchLogs()"]'
    );

    if (searchButton) {

        searchButton.onclick = searchLogs;

    }

    // Enter key search.
    const keywordInput =
        document.getElementById("keyword");

    if (keywordInput) {

        keywordInput.addEventListener(
            "keydown",
            event => {

                if (event.key === "Enter") {

                    searchLogs();

                }

            }
        );

    }

    // Initial dashboard refresh.
    refreshDashboard();

    // Live system monitoring.
    setInterval(
        loadSystemStatus,
        3000
    );

    // Dashboard data refresh.
    setInterval(
        refreshDashboard,
        5000
    );

});
