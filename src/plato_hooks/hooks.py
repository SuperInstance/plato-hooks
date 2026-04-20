"""Git hook management — conditional triggers, chains, rate limiting, callbacks."""
import subprocess
import os
import time
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Optional
from collections import defaultdict

@dataclass
class Hook:
    name: str
    event: str
    command: str = ""
    callback: str = ""  # reference to registered callback
    enabled: bool = True
    trigger_count: int = 0
    last_triggered: float = 0.0
    condition: str = ""  # eval condition
    rate_limit: float = 0.0  # min seconds between triggers
    priority: int = 0  # higher = runs first
    chain: list[str] = field(default_factory=list)  # hooks to run after this one

class HookManager:
    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self._hooks: dict[str, list[Hook]] = {}
        self._callbacks: dict[str, Callable] = {}
        self._trigger_log: list[dict] = []
        self._failures: list[dict] = []

    def register(self, name: str, event: str, command: str = "",
                 callback: str = "", condition: str = "",
                 rate_limit: float = 0.0, priority: int = 0) -> Hook:
        h = Hook(name=name, event=event, command=command, callback=callback,
                condition=condition, rate_limit=rate_limit, priority=priority)
        if event not in self._hooks:
            self._hooks[event] = []
        self._hooks[event].append(h)
        self._hooks[event].sort(key=lambda x: x.priority, reverse=True)
        return h

    def register_callback(self, name: str, fn: Callable):
        self._callbacks[name] = fn

    def add_chain(self, hook_name: str, chain_to: list[str]):
        for event_hooks in self._hooks.values():
            for h in event_hooks:
                if h.name == hook_name:
                    h.chain.extend(chain_to)
                    return

    def trigger(self, event: str, context: dict = None) -> list[dict]:
        results = []
        context = context or {}
        triggered_names = set()

        for h in self._hooks.get(event, []):
            if not h.enabled:
                continue
            if h.rate_limit > 0 and time.time() - h.last_triggered < h.rate_limit:
                results.append({"hook": h.name, "status": "rate_limited",
                               "message": f"Rate limited ({h.rate_limit}s)"})
                continue
            if h.condition:
                try:
                    if not eval(h.condition, {"__builtins__": {}}, context):
                        results.append({"hook": h.name, "status": "skipped",
                                       "message": "Condition not met"})
                        continue
                except Exception as e:
                    results.append({"hook": h.name, "status": "error",
                                   "message": f"Condition error: {e}"})
                    continue

            h.trigger_count += 1
            h.last_triggered = time.time()
            triggered_names.add(h.name)

            # Execute command or callback
            if h.callback and h.callback in self._callbacks:
                try:
                    cb_result = self._callbacks[h.callback](context)
                    results.append({"hook": h.name, "status": "ok",
                                   "type": "callback", "result": str(cb_result)})
                except Exception as e:
                    results.append({"hook": h.name, "status": "error",
                                   "type": "callback", "message": str(e)})
                    self._failures.append({"hook": h.name, "error": str(e), "timestamp": time.time()})
            elif h.command:
                try:
                    result = subprocess.run(h.command, shell=True, capture_output=True,
                                           text=True, cwd=self.repo_path, timeout=30)
                    status = "ok" if result.returncode == 0 else "error"
                    output = result.stdout[:500]
                    if result.returncode != 0:
                        output += result.stderr[:200]
                        self._failures.append({"hook": h.name, "output": output,
                                              "code": result.returncode, "timestamp": time.time()})
                    results.append({"hook": h.name, "status": status,
                                   "type": "command", "output": output, "code": result.returncode})
                except Exception as e:
                    results.append({"hook": h.name, "status": "error",
                                   "type": "command", "message": str(e)})
                    self._failures.append({"hook": h.name, "error": str(e), "timestamp": time.time()})

            # Process chains
            for chain_name in h.chain:
                chain_results = self.trigger(chain_name, context)
                results.extend(chain_results)

        self._log(event, context, results)
        return results

    def _log(self, event: str, context: dict, results: list[dict]):
        entry = {"event": event, "timestamp": time.time(),
                "results": results, "context_keys": list(context.keys())}
        self._trigger_log.append(entry)
        if len(self._trigger_log) > 1000:
            self._trigger_log = self._trigger_log[-1000:]

    def enable(self, name: str):
        for hooks in self._hooks.values():
            for h in hooks:
                if h.name == name: h.enabled = True

    def disable(self, name: str):
        for hooks in self._hooks.values():
            for h in hooks:
                if h.name == name: h.enabled = False

    def list_hooks(self, event: str = "") -> list[dict]:
        events = {event} if event else set(self._hooks.keys())
        result = []
        for e in events:
            for h in self._hooks.get(e, []):
                result.append({"name": h.name, "event": e, "enabled": h.enabled,
                              "triggers": h.trigger_count, "priority": h.priority,
                              "has_chain": bool(h.chain), "has_condition": bool(h.condition),
                              "rate_limit": h.rate_limit})
        return result

    @property
    def stats(self) -> dict:
        events = {e: len(hs) for e, hs in self._hooks.items()}
        total_triggers = sum(h.trigger_count for hs in self._hooks.values() for h in hs)
        return {"events": events, "total_hooks": sum(len(v) for v in self._hooks.values()),
                "total_triggers": total_triggers, "callbacks": len(self._callbacks),
                "failures": len(self._failures), "log_entries": len(self._trigger_log)}
