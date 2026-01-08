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
    # Explorer
    "explorer_experiment": "🧪",
    # Implementation
    "implement_feature": "🛠️",
    "find_tests": "🔎",
    "code_review": "📝",
    # Campaign
    "campaign": "🚀",
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

/* ============================================
   STREAMING TOOL BLOCKS (Amp-style)
   ============================================ */

/* Tool execution container */
.tool-block {
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 10px;
    margin: 10px 0;
    overflow: hidden;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif;
    font-size: 13px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.12);
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
}

.tool-block.running {
    border-color: #388bfd;
    box-shadow: 0 0 0 1px rgba(56, 139, 253, 0.4), 0 2px 8px rgba(56, 139, 253, 0.15);
}

.tool-block.success {
    border-color: #238636;
}

.tool-block.error {
    border-color: #da3633;
}

/* Tool block header - Amp style with cleaner look */
.tool-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 16px;
    background: linear-gradient(180deg, #161b22 0%, #0d1117 100%);
    border-bottom: 1px solid #21262d;
    cursor: pointer;
    user-select: none;
}

.tool-header:hover {
    background: linear-gradient(180deg, #1c2128 0%, #161b22 100%);
}

.tool-header-left {
    display: flex;
    align-items: center;
    gap: 10px;
}

.tool-header-right {
    display: flex;
    align-items: center;
    gap: 10px;
    color: #8b949e;
    font-size: 12px;
}

.tool-name {
    font-weight: 600;
    color: #e6edf3;
    font-size: 13px;
}

.tool-status-spinner {
    display: inline-block;
    width: 14px;
    height: 14px;
    border: 2px solid rgba(56, 139, 253, 0.25);
    border-top-color: #388bfd;
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
}

.tool-elapsed {
    color: #7d8590;
    font-family: 'SF Mono', 'Consolas', 'Monaco', monospace;
    font-size: 11px;
}

/* Status badges - Amp-style pills */
.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}

.status-badge.running {
    background: rgba(56, 139, 253, 0.15);
    color: #58a6ff;
    border: 1px solid rgba(56, 139, 253, 0.4);
}

.status-badge.success {
    background: rgba(35, 134, 54, 0.15);
    color: #3fb950;
    border: 1px solid rgba(35, 134, 54, 0.4);
}

.status-badge.error {
    background: rgba(218, 54, 51, 0.15);
    color: #f85149;
    border: 1px solid rgba(218, 54, 51, 0.4);
}

/* Exit code badges */
.exit-badge {
    display: inline-flex;
    align-items: center;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
    font-family: 'SF Mono', 'Consolas', monospace;
}

.exit-badge.success {
    background: rgba(35, 134, 54, 0.2);
    color: #3fb950;
}

.exit-badge.error {
    background: rgba(218, 54, 51, 0.2);
    color: #f85149;
}

/* Collapsible sections in tool blocks - Amp style */
.tool-block details {
    margin: 0;
    border: none;
    border-radius: 0;
    border-top: 1px solid #21262d;
    background: #0d1117;
}

.tool-block details summary {
    padding: 10px 16px;
    background: #161b22;
    color: #7d8590;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    user-select: none;
    display: flex;
    align-items: center;
    gap: 8px;
    transition: background 0.15s ease;
}

.tool-block details summary::before {
    content: '▸';
    font-size: 10px;
    transition: transform 0.15s ease;
}

.tool-block details[open] summary::before {
    transform: rotate(90deg);
}

.tool-block details summary:hover {
    background: #1c2128;
    color: #e6edf3;
}

.tool-block details[open] summary {
    border-bottom: 1px solid #21262d;
}

.tool-block details .output-content {
    padding: 12px 16px;
    background: #010409;
    max-height: 250px;
    overflow-y: auto;
    white-space: pre-wrap;
    word-wrap: break-word;
    line-height: 1.5;
    font-family: 'SF Mono', 'Consolas', 'Monaco', 'Menlo', monospace;
    font-size: 12px;
}

/* Truncated output with "Show more" */
.output-truncated {
    position: relative;
}

.output-truncated::after {
    content: '';
    position: absolute;
    bottom: 0;
    left: 0;
    right: 0;
    height: 60px;
    background: linear-gradient(transparent, #010409);
    pointer-events: none;
}

.show-more-btn {
    display: block;
    width: 100%;
    padding: 8px;
    background: #161b22;
    border: none;
    border-top: 1px solid #21262d;
    color: #58a6ff;
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    text-align: center;
    transition: background 0.15s ease;
}

.show-more-btn:hover {
    background: #1c2128;
}

/* Stdout styling */
.output-stdout {
    color: #e6edf3;
}

/* Stderr styling */
.output-stderr {
    color: #ffa657;
}

/* Nested subprocess output - Amp style */
.subprocess-output {
    margin: 0;
    padding: 12px 16px;
    background: #010409;
    border-top: 1px solid #21262d;
    font-family: 'SF Mono', 'Consolas', 'Monaco', monospace;
    font-size: 12px;
    max-height: 200px;
    overflow-y: auto;
}

.subprocess-output.running {
    position: relative;
}

.subprocess-output.running::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 3px;
    background: linear-gradient(180deg, #388bfd 0%, #a371f7 100%);
    animation: subprocess-pulse 1.5s ease-in-out infinite;
}

@keyframes subprocess-pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

.subprocess-line {
    padding: 2px 0;
    color: #7d8590;
    line-height: 1.4;
}

.subprocess-line.stdout {
    color: #e6edf3;
}

.subprocess-line.stderr {
    color: #ffa657;
}

/* Live message indicator - Amp style */
.live-indicator {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 6px 12px;
    background: rgba(56, 139, 253, 0.1);
    border: 1px solid rgba(56, 139, 253, 0.3);
    border-radius: 6px;
    font-size: 12px;
    color: #58a6ff;
}

.live-dot {
    width: 8px;
    height: 8px;
    background: #58a6ff;
    border-radius: 50%;
    animation: live-blink 1.2s ease-in-out infinite;
}

@keyframes live-blink {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.85); }
}

/* Prominent running banner - Amp style */
.running-banner {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 14px 18px;
    margin: 12px 0;
    background: linear-gradient(90deg, rgba(56, 139, 253, 0.08) 0%, rgba(163, 113, 247, 0.08) 100%);
    border: 1px solid rgba(56, 139, 253, 0.4);
    border-radius: 10px;
    backdrop-filter: blur(8px);
}

.running-banner-left {
    display: flex;
    align-items: center;
    gap: 12px;
}

.running-banner-spinner {
    width: 18px;
    height: 18px;
    border: 2px solid rgba(56, 139, 253, 0.25);
    border-top-color: #58a6ff;
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
}

.running-banner-text {
    font-weight: 600;
    color: #e6edf3;
    font-size: 13px;
}

.running-banner-step {
    color: #a371f7;
    font-weight: 500;
}

.running-banner-elapsed {
    color: #7d8590;
    font-size: 12px;
    font-family: 'SF Mono', 'Consolas', monospace;
    background: rgba(110, 118, 129, 0.1);
    padding: 4px 10px;
    border-radius: 6px;
}

/* Smaller inline running indicator */
.running-inline {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 3px 10px;
    background: rgba(56, 139, 253, 0.1);
    border-radius: 12px;
    font-size: 11px;
    color: #58a6ff;
}

.running-inline-dot {
    width: 6px;
    height: 6px;
    background: #58a6ff;
    border-radius: 50%;
    animation: running-dot-pulse 1s ease-in-out infinite;
}

@keyframes running-dot-pulse {
    0%, 100% {
        transform: scale(1);
        opacity: 1;
    }
    50% {
        transform: scale(1.2);
        opacity: 0.6;
    }
}

/* Agent thinking block - Amp style */
.thinking-block {
    background: #0d1117;
    border: 1px solid rgba(163, 113, 247, 0.4);
    border-radius: 10px;
    padding: 14px 18px;
    margin: 10px 0;
    position: relative;
    overflow: hidden;
}

.thinking-block::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 3px;
    background: linear-gradient(180deg, #a371f7 0%, #388bfd 100%);
}

.thinking-block .thinking-header {
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 500;
    color: #e6edf3;
}

.thinking-block .thinking-spinner {
    display: inline-block;
    width: 14px;
    height: 14px;
    border: 2px solid rgba(163, 113, 247, 0.25);
    border-top-color: #a371f7;
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
}

.thinking-block .thinking-message {
    margin-top: 8px;
    color: #7d8590;
    font-size: 13px;
}

/* Step progress indicator - Amp style */
.step-block {
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 14px 18px;
    margin: 10px 0;
}

.step-block.running {
    border-color: rgba(56, 139, 253, 0.4);
}

.step-block.completed {
    border-color: rgba(35, 134, 54, 0.4);
}

.step-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;
}

.step-title {
    font-weight: 600;
    color: #e6edf3;
    font-size: 14px;
}

.step-counter {
    color: #7d8590;
    font-size: 12px;
    font-family: 'SF Mono', 'Consolas', monospace;
    background: rgba(110, 118, 129, 0.1);
    padding: 3px 8px;
    border-radius: 6px;
}

.step-progress {
    height: 6px;
    background: #21262d;
    border-radius: 3px;
    overflow: hidden;
}

.step-progress-bar {
    height: 100%;
    background: linear-gradient(90deg, #388bfd 0%, #a371f7 100%);
    border-radius: 3px;
    transition: width 0.4s cubic-bezier(0.4, 0, 0.2, 1);
}

/* Warning/error blocks - Amp style */
.warning-block {
    background: rgba(210, 153, 34, 0.08);
    border: 1px solid rgba(210, 153, 34, 0.4);
    border-radius: 10px;
    padding: 12px 16px;
    margin: 10px 0;
    color: #d29922;
    display: flex;
    align-items: flex-start;
    gap: 10px;
}

.error-block {
    background: rgba(218, 54, 51, 0.08);
    border: 1px solid rgba(218, 54, 51, 0.4);
    border-radius: 10px;
    padding: 12px 16px;
    margin: 10px 0;
    color: #f85149;
    display: flex;
    align-items: flex-start;
    gap: 10px;
}

/* Generic collapsible styling (for Gradio Markdown) */
details {
    margin: 8px 0;
    border: 1px solid #30363d;
    border-radius: 6px;
    background: #161b22;
    overflow: hidden;
}

details summary {
    padding: 8px 12px;
    background: #21262d;
    cursor: pointer;
    color: #c9d1d9;
    font-weight: 500;
    user-select: none;
}

details summary:hover {
    background: #2d333b;
}

details[open] summary {
    border-bottom: 1px solid #30363d;
}

details > *:not(summary) {
    padding: 12px;
}

/* Code blocks inside details */
details pre {
    margin: 0;
    padding: 12px !important;
    background: #0d1117 !important;
    border-radius: 0;
}

details code {
    background: transparent !important;
}
"""

# JavaScript for enhanced interactions
ENHANCED_JS = """
<script>
// Sound notification state
let soundEnabled = true;
let notificationSound = null;

// Initialize audio lazily (browser restricts autoplay)
function initAudio() {
    if (!notificationSound) {
        notificationSound = new Audio(
            'data:audio/wav;base64,UklGRnoGAABXQVZFZm10IBAAAAABAAEAQB8AAEAfAAABAAgA' +
            'ZGF0YQoGAACBhYqFbF1fdH2JkJWRgnNka2t4goyRj4V3Z2JocHyGjI+MgXVpZGlxfIaL' +
            'j42CdmtnaG94gYiLi4d+dHBwdHl/hIiJiIR/enl6fH+Bg4SFhIN/fX19fn+AgYGBgYB/' +
            'fn5+fn9/gICAgIB/f39/f39/gICAgIB/f39/f39/f4CAgICAf39/f39/f39/gICAgH9/' +
            'f39/f39/f3+AgIB/f39/f39/f39/f4CAf39/f39/f39/f39/gH9/f39/f39/f39/f39/' +
            'f39/f39/f39/f39/f39/f39/f39/f39/f39/f39/fw=='
        );
    }
}

function playNotification() {
    if (soundEnabled) {
        initAudio();
        if (notificationSound) {
            notificationSound.play().catch(() => {});
        }
    }
}

// Toggle sound notifications
function toggleSound() {
    soundEnabled = !soundEnabled;
    // Update button text - Gradio wraps buttons, so search for span inside
    const buttons = document.querySelectorAll('button');
    buttons.forEach(btn => {
        const text = btn.textContent || btn.innerText;
        if (text.includes('🔔') || text.includes('🔕')) {
            btn.textContent = soundEnabled ? '🔔' : '🔕';
        }
    });
    console.log('[Odibi] Sound notifications:', soundEnabled ? 'ON' : 'OFF');
}

// Toggle theme between light and dark
function toggleTheme() {
    document.body.classList.toggle('light-theme');
    const isLight = document.body.classList.contains('light-theme');
    localStorage.setItem('odibi-theme', isLight ? 'light' : 'dark');

    // Also apply to Gradio container
    const container = document.querySelector('.gradio-container');
    if (container) {
        if (isLight) {
            container.classList.add('light-theme');
        } else {
            container.classList.remove('light-theme');
        }
    }

    // Update theme button icon
    const buttons = document.querySelectorAll('button');
    buttons.forEach(btn => {
        const text = btn.textContent || btn.innerText;
        if (text.includes('🌓') || text.includes('☀️') || text.includes('🌙')) {
            btn.textContent = isLight ? '☀️' : '🌙';
        }
    });

    console.log('[Odibi] Theme:', isLight ? 'light' : 'dark');
}

// Restore saved theme preference
function restoreTheme() {
    const savedTheme = localStorage.getItem('odibi-theme');
    if (savedTheme === 'light') {
        document.body.classList.add('light-theme');
        const container = document.querySelector('.gradio-container');
        if (container) {
            container.classList.add('light-theme');
        }
    }
}

// Restore theme on various load stages (Gradio loads dynamically)
restoreTheme();
document.addEventListener('DOMContentLoaded', restoreTheme);
setTimeout(restoreTheme, 300);
setTimeout(restoreTheme, 1000);

// Attach click handlers to toggle buttons
function attachToggleHandlers() {
    // Gradio wraps buttons - elem_id is on container, find actual button inside
    const themeContainer = document.getElementById('theme-toggle');
    if (themeContainer) {
        const themeBtn = themeContainer.querySelector('button') || themeContainer;
        if (!themeBtn._odibiHandlerAttached) {
            themeBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                toggleTheme();
            });
            themeBtn._odibiHandlerAttached = true;
            console.log('[Odibi] Theme toggle handler attached to:', themeBtn.tagName);
        }
    }
    const soundContainer = document.getElementById('sound-toggle');
    if (soundContainer) {
        const soundBtn = soundContainer.querySelector('button') || soundContainer;
        if (!soundBtn._odibiHandlerAttached) {
            soundBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                toggleSound();
            });
            soundBtn._odibiHandlerAttached = true;
            console.log('[Odibi] Sound toggle handler attached to:', soundBtn.tagName);
        }
    }
}

// Retry attaching handlers (Gradio loads components dynamically)
attachToggleHandlers();
document.addEventListener('DOMContentLoaded', attachToggleHandlers);
setTimeout(attachToggleHandlers, 500);
setTimeout(attachToggleHandlers, 1500);
setTimeout(attachToggleHandlers, 3000);
setTimeout(attachToggleHandlers, 5000);

// File link handler - opens in VS Code
function openFileLink(path) {
    const vscodeUrl = 'vscode://file/' + encodeURIComponent(path);
    window.open(vscodeUrl, '_blank');
}

// Collapsible sections toggle
function toggleCollapse(id) {
    const content = document.getElementById(id);
    if (content) {
        content.classList.toggle('collapsed');
    }
}

// Auto-scroll activity feed to bottom
function scrollActivityFeed() {
    const feed = document.querySelector('.activity-feed');
    if (feed) {
        feed.scrollTop = feed.scrollHeight;
    }
}

// Prevent chatbot from forcing scroll when user has scrolled up
let userScrolledUp = false;
let scrollTimeout = null;

function setupChatScrollBehavior() {
    const chatContainer = document.querySelector('.chatbot, [data-testid="chatbot"]');
    if (!chatContainer || chatContainer._odibiScrollSetup) return;

    // Find the scrollable element inside chatbot
    const scrollable = chatContainer.querySelector('.overflow-y-auto, .messages-wrapper') || chatContainer;

    scrollable.addEventListener('scroll', function() {
        const isAtBottom = scrollable.scrollHeight - scrollable.scrollTop - scrollable.clientHeight < 100;
        userScrolledUp = !isAtBottom;

        // Reset after user stops scrolling and is at bottom
        if (isAtBottom) {
            clearTimeout(scrollTimeout);
            scrollTimeout = setTimeout(() => { userScrolledUp = false; }, 500);
        }
    });

    // Override Gradio's auto-scroll by intercepting scrollTop changes
    const originalScrollTop = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollTop');
    if (originalScrollTop && originalScrollTop.set) {
        Object.defineProperty(scrollable, 'scrollTop', {
            get: function() { return originalScrollTop.get.call(this); },
            set: function(val) {
                // Only block if user scrolled up and it's trying to scroll to bottom
                if (userScrolledUp && val > this.scrollTop) {
                    return; // Block auto-scroll to bottom
                }
                originalScrollTop.set.call(this, val);
            }
        });
    }

    chatContainer._odibiScrollSetup = true;
    console.log('[Odibi] Chat scroll behavior configured');
}

// Try to setup scroll behavior multiple times as Gradio loads dynamically
setTimeout(setupChatScrollBehavior, 1000);
setTimeout(setupChatScrollBehavior, 3000);
setTimeout(setupChatScrollBehavior, 5000);
</script>
"""

# Raw JS for Gradio's js= parameter (no script tags)
ENHANCED_JS_RAW = """
() => {
    console.log('[Odibi] Initializing UI enhancements...');

    // Theme toggle
    window.odibiToggleTheme = function() {
        document.body.classList.toggle('light-theme');
        const isLight = document.body.classList.contains('light-theme');
        localStorage.setItem('odibi-theme', isLight ? 'light' : 'dark');
        const container = document.querySelector('.gradio-container');
        if (container) container.classList.toggle('light-theme', isLight);
        console.log('[Odibi] Theme:', isLight ? 'light' : 'dark');
    };

    // Sound toggle
    window.odibiSoundEnabled = true;
    window.odibiToggleSound = function() {
        window.odibiSoundEnabled = !window.odibiSoundEnabled;
        console.log('[Odibi] Sound:', window.odibiSoundEnabled ? 'ON' : 'OFF');
    };

    // Restore theme
    const savedTheme = localStorage.getItem('odibi-theme');
    if (savedTheme === 'light') {
        document.body.classList.add('light-theme');
        const container = document.querySelector('.gradio-container');
        if (container) container.classList.add('light-theme');
    }

    // Attach handlers with retry
    function attachHandlers() {
        document.querySelectorAll('button').forEach(btn => {
            const text = btn.textContent || '';
            if (text.includes('🌓') && !btn._themeHandler) {
                btn.addEventListener('click', () => window.odibiToggleTheme());
                btn._themeHandler = true;
                console.log('[Odibi] Theme button found');
            }
            if ((text.includes('🔔') || text.includes('🔕')) && !btn._soundHandler) {
                btn.addEventListener('click', () => {
                    window.odibiToggleSound();
                    btn.textContent = window.odibiSoundEnabled ? '🔔' : '🔕';
                });
                btn._soundHandler = true;
                console.log('[Odibi] Sound button found');
            }
        });
    }

    attachHandlers();
    setTimeout(attachHandlers, 1000);
    setTimeout(attachHandlers, 3000);

    // Scroll fix: prevent auto-scroll when user scrolled up
    function setupScroll() {
        const chat = document.querySelector('.chatbot');
        if (!chat || chat._scrollFixed) return;

        let userScrolled = false;
        chat.addEventListener('scroll', () => {
            userScrolled = chat.scrollHeight - chat.scrollTop - chat.clientHeight > 150;
        }, true);

        // MutationObserver to catch content changes
        const observer = new MutationObserver(() => {
            if (!userScrolled) {
                chat.scrollTop = chat.scrollHeight;
            }
        });
        observer.observe(chat, { childList: true, subtree: true });
        chat._scrollFixed = true;
        console.log('[Odibi] Scroll behavior fixed');
    }

    setTimeout(setupScroll, 2000);
    setTimeout(setupScroll, 5000);

    // Keyboard shortcuts
    function setupKeyboardShortcuts() {
        document.addEventListener('keydown', (e) => {
            // Ctrl+Enter or Cmd+Enter: Send message
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                const sendBtn = document.querySelector('button.primary');
                if (sendBtn) {
                    e.preventDefault();
                    sendBtn.click();
                    console.log('[Odibi] Ctrl+Enter: Send');
                }
            }

            // Escape: Stop generation
            if (e.key === 'Escape') {
                const stopBtn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.textContent && b.textContent.includes('⏹️'));
                if (stopBtn) {
                    stopBtn.click();
                    console.log('[Odibi] Escape: Stop');
                }
            }

            // Ctrl+L: Clear chat
            if ((e.ctrlKey || e.metaKey) && e.key === 'l') {
                const clearBtn = Array.from(document.querySelectorAll('button'))
                    .find(b => b.textContent && b.textContent.includes('🗑️'));
                if (clearBtn) {
                    e.preventDefault();
                    clearBtn.click();
                    console.log('[Odibi] Ctrl+L: Clear');
                }
            }

            // Ctrl+/ : Focus input
            if ((e.ctrlKey || e.metaKey) && e.key === '/') {
                const input = document.querySelector('textarea');
                if (input) {
                    e.preventDefault();
                    input.focus();
                    console.log('[Odibi] Ctrl+/: Focus input');
                }
            }
        });
        console.log('[Odibi] Keyboard shortcuts registered: Ctrl+Enter (send), Esc (stop), Ctrl+L (clear), Ctrl+/ (focus)');
    }

    setupKeyboardShortcuts();
}
"""
