# Autonomous Learning Session UI Guide

This document describes how to use the UI for running long, unattended autonomous learning sessions (Phase 9.B).

## Overview

The Learning Session UI provides a safe window into the autonomous learning engine. It allows you to:

- **Start** a learning session
- **Monitor** progress in real-time
- **Stop** a session safely

This is **not orchestration**—it is "launch and observe."

## Safety Guarantees

Learning sessions are **observation-only**. The following invariants are mechanically enforced:

| Guarantee | Enforcement |
|-----------|-------------|
| ❌ No code modifications | `max_improvements=0` always |
| ❌ ImprovementAgent NEVER invoked | `GuardedCycleRunner._should_skip_step()` |
| ❌ ReviewAgent NEVER invoked | `GUARDED_SKIP_STEPS` constant |
| ✅ Disk guards active | `DiskUsageGuard` enforces budgets |
| ✅ Heartbeat updated | `HeartbeatWriter` writes `.odibi/heartbeat.json` |
| ✅ Session survives UI refresh | Read heartbeat to reconnect |

**If you attempt to enable improvements, the system will reject the configuration.**

## Starting a Learning Session

1. Open the Odibi Assistant UI
2. Select **Guided Execution** mode
3. Expand **📚 Learning Session (Long-Running)**
4. Configure:
   - **Project Root**: Directory to analyze (required)
   - **Max Cycles**: Default 100 (up to 500)
   - **Max Wall-Clock Hours**: Default 168 (1 week), max 336 (2 weeks)
5. Click **▶️ Start Learning Session**

### Example Configuration

```
Project Root: d:/odibi/examples
Max Cycles: 100
Max Wall-Clock Hours: 168
```

This will run up to 100 learning cycles over 7 days maximum.

## Monitoring a Session

The **Session Monitor** panel shows read-only status by reading from `.odibi/heartbeat.json`:

| Field | Description |
|-------|-------------|
| **State** | `NOT_STARTED`, `RUNNING`, `STOPPING`, `COMPLETED`, `FAILED` |
| **Session ID** | UUID of the current session |
| **Cycles Completed** | Number of successful cycles |
| **Cycles Failed** | Number of failed cycles |
| **Last Heartbeat** | Timestamp of last heartbeat write |
| **Last Cycle ID** | UUID of most recent cycle |
| **Last Status** | Status of most recent cycle |
| **Disk Usage** | Storage used by artifacts/reports/index |

Click **🔄 Refresh Status** to read the latest heartbeat file.

### Reconnecting After UI Refresh

If you refresh the browser or close/reopen the UI:

1. The scheduler continues running independently
2. Click **🔄 Refresh Status** to read current state from heartbeat
3. The monitor will show the current session status

**The UI does not control the scheduler—it only observes it.**

## Stopping a Session

1. Click **⏹️ Stop Session**
2. The state will change to `STOPPING`
3. The scheduler will finish the current cycle and exit cleanly
4. Final state will show `COMPLETED`

**Stop is graceful—it does not kill the process.**

## What Cannot Happen

The following are **impossible** by design:

1. **ImprovementAgent execution** — Step is always skipped
2. **ReviewAgent execution** — Step is always skipped
3. **Code writes** — `LearningModeGuard` prevents all writes
4. **Proposal generation** — Guard asserts no proposals
5. **Disk budget overflow** — `DiskUsageGuard` rotates old data
6. **Silent failures** — All errors surface to UI

## Files Read by the UI

The monitor panel reads these files (read-only):

```
.odibi/
├── heartbeat.json       # Session state, cycle counts, disk usage
├── reports/             # Cycle reports (for viewing)
├── artifacts/           # Execution evidence
└── index/               # Indexed learnings
```

**The UI never writes to these files.**

## Troubleshooting

### Session shows `NOT_STARTED` but I started one

- The scheduler may have failed to start
- Check the chat history for error messages
- Verify the project root path exists

### Session stuck in `RUNNING` after completion

- Click **🔄 Refresh Status** to read latest heartbeat
- The heartbeat file may not have been written (disk issue)

### Cycles failing repeatedly

- Check `.odibi/reports/` for error details
- Common causes: LLM API errors, disk full, invalid project config

### State shows `FAILED`

- The session encountered an unrecoverable error
- Check `Last Status` field for the error reason
- Review `.odibi/artifacts/` for detailed error logs

## Best Practices

1. **Start with smaller runs** — Test with 3-5 cycles before long runs
2. **Monitor disk usage** — Check the disk usage display periodically
3. **Use Refresh Status** — Don't assume the UI state is current
4. **Read heartbeat manually** — For external monitoring, read `.odibi/heartbeat.json`

## Technical Details

### Session Lifecycle

```
NOT_STARTED → STARTING → RUNNING → STOPPING → COMPLETED
                  │                     │
                  └─────── FAILED ──────┘
```

### Heartbeat File Format

```json
{
  "last_cycle_id": "uuid-of-last-cycle",
  "timestamp": "2024-01-15T08:30:00",
  "last_status": "SESSION_RUNNING",
  "cycles_completed": 42,
  "cycles_failed": 1,
  "disk_usage": {
    "artifacts": 1073741824,
    "reports": 52428800,
    "index": 209715200
  }
}
```

### Relevant Code Paths

- UI Panel: `agents/ui/components/cycle_panel.py`
- Scheduler: `agents/core/autonomous_learning.py`
- Disk Guard: `agents/core/disk_guard.py`
- Heartbeat: `agents/core/disk_guard.py::HeartbeatWriter`
