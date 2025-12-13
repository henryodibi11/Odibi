"""UI constants and shared definitions.

Centralizes emoji maps, CSS, and other UI constants to avoid duplication.
"""

from typing import Final

# Tool emoji mappings - used for status display and activity feed
TOOL_EMOJI: Final[dict[str, str]] = {
    # File operations
    "read_file": "📖",
    "write_file": "✏️",
    "list_directory": "📁",
    "undo_edit": "↩️",
    # Search operations
    "grep": "🔍",
    "glob": "🔍",
    "search": "🧠",
    # Shell operations
    "run_command": "⚡",
    "pytest": "🧪",
    "ruff": "🔧",
    "diagnostics": "🩺",
    "typecheck": "📝",
    "odibi_run": "🚀",
    # Web operations
    "web_search": "🌐",
    "read_web_page": "🌐",
    # Task management
    "todo_write": "📋",
    "todo_read": "📋",
    # Diagrams
    "mermaid": "📊",
    # Git operations
    "git_status": "📦",
    "git_diff": "📦",
    "git_log": "📦",
    # Sub-agents
    "task": "🤖",
    "parallel_tasks": "🚀",
    # Code execution
    "execute_python": "🐍",
    "sql": "🗃️",
    "list_tables": "📋",
    "describe_table": "📊",
    # Memory
    "remember": "💾",
    "recall": "🧠",
}

# Memory type emoji mappings
MEMORY_TYPE_EMOJI: Final[dict[str, str]] = {
    "decision": "🔵",
    "learning": "🟢",
    "bug_fix": "🔴",
    "preference": "🟣",
    "todo": "🟡",
    "feature": "🟠",
    "context": "⚪",
}

# Default emoji for unknown tools/types
DEFAULT_TOOL_EMOJI: Final[str] = "🔧"
DEFAULT_MEMORY_EMOJI: Final[str] = "📝"


def get_tool_emoji(tool_name: str) -> str:
    """Get emoji for a tool, with fallback."""
    return TOOL_EMOJI.get(tool_name, DEFAULT_TOOL_EMOJI)


def get_memory_emoji(memory_type: str) -> str:
    """Get emoji for a memory type, with fallback."""
    return MEMORY_TYPE_EMOJI.get(memory_type, DEFAULT_MEMORY_EMOJI)


# Status messages with animations
STATUS_THINKING = "🤔 Thinking..."
STATUS_PROCESSING = "🔄 Processing..."
STATUS_STOPPED = "⏹️ Stopped by user."
STATUS_AWAITING = "⏳ Awaiting confirmation..."

# Token cost estimates (per 1K tokens, in USD)
# Based on OpenAI pricing as of late 2024
TOKEN_COSTS: Final[dict[str, dict[str, float]]] = {
    "gpt-4o": {"input": 0.0025, "output": 0.01},
    "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
    "gpt-4.1": {"input": 0.002, "output": 0.008},
    "o1": {"input": 0.015, "output": 0.06},
    "o1-mini": {"input": 0.003, "output": 0.012},
    "o1-preview": {"input": 0.015, "output": 0.06},
    # Default for unknown models
    "default": {"input": 0.005, "output": 0.015},
}


def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate cost for a completion.

    Args:
        model: Model name.
        input_tokens: Number of input tokens.
        output_tokens: Number of output tokens.

    Returns:
        Estimated cost in USD.
    """
    model_lower = model.lower()
    costs = TOKEN_COSTS.get(model_lower, TOKEN_COSTS["default"])
    return (input_tokens / 1000 * costs["input"]) + (output_tokens / 1000 * costs["output"])


# CSS for the enhanced UI
ENHANCED_CSS = """
/* Base styles */
.gradio-container {
    max-width: 100% !important;
    width: 100% !important;
    margin: 0 !important;
    padding: 12px !important;
}

footer {
    display: none !important;
}

/* Chat area */
.chatbot {
    height: 600px !important;
    min-height: 400px !important;
}

/* Compact side panels */
.memory-list, .activity-feed {
    max-height: 250px;
    overflow-y: auto;
}

/* Tighter spacing */
.gr-accordion {
    margin-bottom: 8px !important;
}

.gr-block {
    padding: 8px !important;
}

/* Animated status bar */
.status-bar {
    min-height: 32px;
    padding: 12px 16px;
    background: linear-gradient(90deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #00d4ff;
    border-radius: 8px;
    color: #00d4ff;
    font-size: 15px;
    font-weight: 500;
    margin: 12px 0;
    animation: pulse 1.5s ease-in-out infinite;
    box-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
}

.status-bar:empty {
    display: none;
    animation: none;
}

.status-bar p {
    margin: 0 !important;
}

@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.7; }
}

/* Typing indicator */
.typing-indicator {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 8px 12px;
    background: rgba(0, 212, 255, 0.1);
    border-radius: 12px;
    margin: 4px 0;
}

.typing-indicator .dot {
    width: 8px;
    height: 8px;
    background: #00d4ff;
    border-radius: 50%;
    animation: typing-bounce 1.4s infinite ease-in-out;
}

.typing-indicator .dot:nth-child(1) { animation-delay: 0s; }
.typing-indicator .dot:nth-child(2) { animation-delay: 0.2s; }
.typing-indicator .dot:nth-child(3) { animation-delay: 0.4s; }

@keyframes typing-bounce {
    0%, 80%, 100% { transform: translateY(0); }
    40% { transform: translateY(-6px); }
}

/* Activity feed */
.activity-feed {
    font-family: 'Consolas', 'Monaco', monospace;
    font-size: 12px;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 8px;
    max-height: 300px;
    overflow-y: auto;
}

.activity-item {
    padding: 4px 8px;
    border-left: 2px solid #30363d;
    margin: 4px 0;
    transition: border-color 0.2s;
}

.activity-item.active {
    border-left-color: #00d4ff;
    background: rgba(0, 212, 255, 0.05);
}

.activity-item.completed {
    border-left-color: #2ea043;
    opacity: 0.7;
}

.activity-item .timestamp {
    color: #8b949e;
    font-size: 10px;
}

/* Thinking panel */
.thinking-panel {
    background: linear-gradient(135deg, #1a1a2e 0%, #0d1117 100%);
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 12px;
    margin: 8px 0;
    font-family: 'Consolas', monospace;
    font-size: 13px;
    color: #8b949e;
    max-height: 200px;
    overflow-y: auto;
}

.thinking-panel.active {
    border-color: #f0883e;
    box-shadow: 0 0 8px rgba(240, 136, 62, 0.2);
}

.thinking-panel .thinking-text {
    white-space: pre-wrap;
    line-height: 1.5;
}

/* Collapsible tool results */
.tool-result {
    border: 1px solid #30363d;
    border-radius: 6px;
    margin: 8px 0;
    overflow: hidden;
}

.tool-result-header {
    background: #161b22;
    padding: 8px 12px;
    cursor: pointer;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-weight: 500;
}

.tool-result-header:hover {
    background: #1f2937;
}

.tool-result-content {
    padding: 12px;
    background: #0d1117;
    max-height: 300px;
    overflow-y: auto;
}

.tool-result-content.collapsed {
    display: none;
}

/* Token counter */
.token-counter {
    display: flex;
    gap: 12px;
    font-size: 11px;
    color: #8b949e;
    padding: 4px 8px;
    background: rgba(0, 0, 0, 0.2);
    border-radius: 4px;
}

.token-counter .cost {
    color: #f0883e;
}

/* File links */
.file-link {
    color: #58a6ff;
    text-decoration: none;
    cursor: pointer;
}

.file-link:hover {
    text-decoration: underline;
}

/* Diff preview */
.diff-preview {
    font-family: 'Consolas', monospace;
    font-size: 12px;
    background: #0d1117;
    border-radius: 6px;
    overflow: hidden;
}

.diff-line {
    padding: 2px 8px;
    white-space: pre;
}

.diff-line.added {
    background: rgba(46, 160, 67, 0.2);
    color: #3fb950;
}

.diff-line.removed {
    background: rgba(248, 81, 73, 0.2);
    color: #f85149;
}

.diff-line.context {
    color: #8b949e;
}

/* Memory sidebar */
.memory-sidebar {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 8px;
}

.memory-item {
    padding: 8px;
    border-bottom: 1px solid #21262d;
    cursor: pointer;
}

.memory-item:hover {
    background: rgba(0, 212, 255, 0.05);
}

.memory-item:last-child {
    border-bottom: none;
}

.memory-badge {
    display: inline-block;
    padding: 2px 6px;
    border-radius: 10px;
    font-size: 10px;
    font-weight: 500;
    text-transform: uppercase;
}

/* Theme toggle */
.theme-toggle {
    position: relative;
    width: 50px;
    height: 26px;
    background: #30363d;
    border-radius: 13px;
    cursor: pointer;
    transition: background 0.3s;
}

.theme-toggle.light {
    background: #0969da;
}

.theme-toggle-slider {
    position: absolute;
    top: 3px;
    left: 3px;
    width: 20px;
    height: 20px;
    background: white;
    border-radius: 50%;
    transition: transform 0.3s;
}

.theme-toggle.light .theme-toggle-slider {
    transform: translateX(24px);
}

/* Light theme overrides */
body.light-theme {
    --bg-primary: #ffffff;
    --bg-secondary: #f6f8fa;
    --text-primary: #1f2328;
    --text-secondary: #656d76;
    --border-color: #d0d7de;
    --accent-color: #0969da;
}

body.light-theme .gradio-container {
    background: var(--bg-primary);
    color: var(--text-primary);
}

body.light-theme .status-bar {
    background: linear-gradient(90deg, #f6f8fa 0%, #ffffff 100%);
    border-color: var(--accent-color);
    color: var(--accent-color);
}

body.light-theme .activity-feed,
body.light-theme .thinking-panel {
    background: var(--bg-secondary);
    border-color: var(--border-color);
    color: var(--text-secondary);
}

/* Sub-agent progress */
.subagent-progress {
    background: linear-gradient(90deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #6e40c9;
    border-radius: 8px;
    padding: 12px;
    margin: 8px 0;
}

.subagent-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
    color: #a371f7;
    font-weight: 500;
}

.subagent-steps {
    font-size: 12px;
    color: #8b949e;
    padding-left: 24px;
}

.subagent-step {
    padding: 2px 0;
    opacity: 0.6;
}

.subagent-step.active {
    opacity: 1;
    color: #00d4ff;
}

.subagent-step.completed {
    color: #2ea043;
}

/* Conversation branch */
.branch-indicator {
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 11px;
    color: #8b949e;
    padding: 4px 8px;
    background: rgba(110, 64, 201, 0.1);
    border-radius: 4px;
    margin: 4px 0;
}

.branch-button {
    padding: 4px 8px;
    font-size: 11px;
    background: #21262d;
    border: 1px solid #30363d;
    border-radius: 4px;
    color: #8b949e;
    cursor: pointer;
}

.branch-button:hover {
    background: #30363d;
    color: #c9d1d9;
}

/* Progress spinner */
.spinner {
    display: inline-block;
    width: 16px;
    height: 16px;
    border: 2px solid rgba(0, 212, 255, 0.3);
    border-top-color: #00d4ff;
    border-radius: 50%;
    animation: spin 1s linear infinite;
}

@keyframes spin {
    to { transform: rotate(360deg); }
}

/* Notification sound indicator */
.sound-enabled {
    color: #2ea043;
}

.sound-disabled {
    color: #8b949e;
}
"""

# JavaScript for enhanced interactions
ENHANCED_JS = """
<script>
// Sound notification
let soundEnabled = true;
const notificationSound = new Audio(
    'data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgA' +
    'ZGF0YQoGAACBhYqFbF1fdH2JkJWRgnNka2t4goyRj4V3Z2JocHyGjI+MgXVpZGlxfIaL' +
    'j42CdmtnaG94gYiLi4d+dHBwdHl/hIiJiIR/enl6fH+Bg4SFhIN/fX19fn+AgYGBgYB/' +
    'fn5+fn9/gICAgIB/f39/f39/gICAgIB/f39/f39/f4CAgICAf39/f39/f39/gICAgH9/' +
    'f39/f39/f3+AgIB/f39/f39/f39/f4CAf39/f39/f39/f39/gH9/f39/f39/f39/f39/' +
    'f39/f39/f39/f39/f39/f39/f39/f39/f39/f39/fw=='
);

function playNotification() {
    if (soundEnabled) {
        notificationSound.play().catch(() => {});
    }
}

function toggleSound() {
    soundEnabled = !soundEnabled;
    const btn = document.getElementById('sound-toggle');
    if (btn) {
        btn.className = soundEnabled ? 'sound-enabled' : 'sound-disabled';
        btn.textContent = soundEnabled ? '🔔' : '🔕';
    }
}

// Theme toggle
function toggleTheme() {
    document.body.classList.toggle('light-theme');
    const theme = document.body.classList.contains('light-theme') ? 'light' : 'dark';
    localStorage.setItem('theme', theme);
}

// Restore theme on load
document.addEventListener('DOMContentLoaded', () => {
    if (localStorage.getItem('theme') === 'light') {
        document.body.classList.add('light-theme');
    }
});

// File link handler
function openFileLink(path) {
    // Try to open in VS Code
    const vscodeUrl = 'vscode://file/' + encodeURIComponent(path);
    window.open(vscodeUrl, '_blank');
}

// Collapsible sections
function toggleCollapse(id) {
    const content = document.getElementById(id);
    if (content) {
        content.classList.toggle('collapsed');
    }
}

// Auto-scroll activity feed
function scrollActivityFeed() {
    const feed = document.querySelector('.activity-feed');
    if (feed) {
        feed.scrollTop = feed.scrollHeight;
    }
}
</script>
"""
