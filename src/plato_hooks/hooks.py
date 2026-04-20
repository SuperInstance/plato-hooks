"""Git hook management — conditional triggers, chains, rate limiting, error recovery, and audit."""
import subprocess
import os
import time
import hashlib
import re
from dataclasses import dataclass, field
from typing import Callable, Optional
from collections import defaultdict

class HookResult(Enum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    CONDITION_FALSE = "condition_false"
    CHAIN_ABORTED = "chain_aborted"

@dataclass
class Hook:
    name: str
    event: str
    command: str = ""
    callback: str = ""
    enabled: bool = True
    trigger_count: int = 0
    last_triggered: float = 0.0
    condition: str = ""
    rate_limit: float = 0.0
    priority: int = 0
    chain: list[str] = field(default_factory=list)
    timeout: float = 30.0
    retry_count: int = 0
    max_retries: int = 0
    retry_delay: float = 1.0
    fail_open: bool = True  # if True, failure doesn't block
    tags: list[str] = field(default_factory=list)

@dataclass
class HookExecution:
    hook_name: str
    event: str
    result: HookResult
    output: str = ""
    error: str = ""
    duration_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    retry_num: int = 0

@dataclass
class HookPattern:
    pattern: str  # glob pattern for hook matching
    event: str
    handler: Callable
    priority: int = 0
    description: str = ""

class HookManager:
    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self._hooks: dict[str, list[Hook]] = defaultdict(list)
        self._callbacks: dict[str, Callable] = {}
        self._patterns: list[HookPattern] = []
        self._trigger_log: deque = deque(maxlen=500)
        self._failures: list[HookExecution] = []
        self._global_rate_limit: float = 0.0
        self._last_global_trigger: float = 0.0

    def register(self, hook: Hook):
        self._hooks[hook.event].append(hook)
        self._hooks[hook.event].sort(key=lambda h: h.priority, reverse=True)

    def register_callback(self, name: str, fn: Callable):
        self._callbacks[name] = fn

    def register_pattern(self, pattern: str, event: str, handler: Callable,
                        priority: int = 0, description: str = ""):
        self._patterns.append(HookPattern(pattern, event, handler, priority, description))
        self._patterns.sort(key=lambda p: p.priority, reverse=True)

    def trigger(self, event: str, context: dict = None) -> list[HookExecution]:
        if context is None:
            context = {}
        results = []
        hooks = self._hooks.get(event, [])
        # Check global rate limit
        if self._global_rate_limit > 0:
            if time.time() - self._last_global_trigger < self._global_rate_limit:
                return results

        for hook in hooks:
            if not hook.enabled:
                results.append(HookExecution(hook.name, event, HookResult.SKIPPED))
                continue
            # Rate limit check
            if hook.rate_limit > 0 and hook.last_triggered > 0:
                if time.time() - hook.last_triggered < hook.rate_limit:
                    results.append(HookExecution(hook.name, event, HookResult.RATE_LIMITED))
                    continue
            # Condition check
            if hook.condition:
                try:
                    if not eval(hook.condition, {"__builtins__": {}}, context):
                        results.append(HookExecution(hook.name, event, HookResult.CONDITION_FALSE))
                        continue
                except:
                    results.append(HookExecution(hook.name, event, HookResult.CONDITION_FALSE))
                        continue
            # Execute with retries
            result = self._execute_hook(hook, event, context)
            results.append(result)
            hook.last_triggered = time.time()
            self._trigger_log.append(result)
            if result.result == HookResult.FAILED and not hook.fail_open:
                self._failures.append(result)
                break  # abort chain on hard failure
            # Chain execution
            if result.result == HookResult.SUCCESS and hook.chain:
                for chain_name in hook.chain:
                    chain_hook = self._find_hook(chain_name)
                    if chain_hook:
                        chain_result = self._execute_hook(chain_hook, event, context)
                        results.append(chain_result)
                        if chain_result.result == HookResult.FAILED and not chain_hook.fail_open:
                            break
        # Pattern matching
        for pattern in self._patterns:
            if pattern.event == event:
                try:
                    pattern.handler(event, context)
                except:
                    pass
        self._last_global_trigger = time.time()
        return results

    def _execute_hook(self, hook: Hook, event: str, context: dict) -> HookExecution:
        start = time.time()
        last_error = ""
        for attempt in range(hook.max_retries + 1):
            try:
                if hook.callback and hook.callback in self._callbacks:
                    output = self._callbacks[hook.callback](event, context)
                    hook.trigger_count += 1
                    return HookExecution(hook.name, event, HookResult.SUCCESS,
                                        output=str(output) if output else "",
                                        duration_ms=(time.time() - start) * 1000,
                                        retry_num=attempt)
                elif hook.command:
                    env = os.environ.copy()
                    env.update({k: str(v) for k, v in context.items() if isinstance(v, (str, int, float))})
                    result = subprocess.run(hook.command, shell=True, cwd=self.repo_path,
                                          capture_output=True, text=True, timeout=hook.timeout, env=env)
                    hook.trigger_count += 1
                    if result.returncode != 0:
                        last_error = result.stderr
                        if attempt < hook.max_retries:
                            time.sleep(hook.retry_delay)
                            continue
                        return HookExecution(hook.name, event, HookResult.FAILED,
                                           output=result.stdout, error=result.stderr,
                                           duration_ms=(time.time() - start) * 1000,
                                           retry_num=attempt)
                    return HookExecution(hook.name, event, HookResult.SUCCESS,
                                       output=result.stdout,
                                       duration_ms=(time.time() - start) * 1000,
                                       retry_num=attempt)
            except subprocess.TimeoutExpired:
                last_error = f"Timeout after {hook.timeout}s"
                if attempt < hook.max_retries:
                    time.sleep(hook.retry_delay)
                    continue
            except Exception as e:
                last_error = str(e)
                if attempt < hook.max_retries:
                    time.sleep(hook.retry_delay)
                    continue
        return HookExecution(hook.name, event, HookResult.FAILED, error=last_error,
                           duration_ms=(time.time() - start) * 1000,
                           retry_num=hook.max_retries)

    def _find_hook(self, name: str) -> Optional[Hook]:
        for event_hooks in self._hooks.values():
            for hook in event_hooks:
                if hook.name == name:
                    return hook
        return None

    def hooks_for_event(self, event: str) -> list[Hook]:
        return self._hooks.get(event, [])

    def execution_log(self, limit: int = 50) -> list[HookExecution]:
        return list(self._trigger_log)[-limit:]

    def failure_log(self, limit: int = 50) -> list[HookExecution]:
        return self._failures[-limit:]

    def clear_failures(self):
        self._failures.clear()

    def disable_all(self, event: str = ""):
        if event:
            for hook in self._hooks.get(event, []):
                hook.enabled = False
        else:
            for event_hooks in self._hooks.values():
                for hook in event_hooks:
                    hook.enabled = False

    def enable_all(self, event: str = ""):
        if event:
            for hook in self._hooks.get(event, []):
                hook.enabled = True
        else:
            for event_hooks in self._hooks.values():
                for hook in event_hooks:
                    hook.enabled = True

    @property
    def stats(self) -> dict:
        total = sum(len(h) for h in self._hooks.values())
        enabled = sum(1 for h_list in self._hooks.values() for h in h_list if h.enabled)
        events = len(self._hooks)
        callbacks = len(self._callbacks)
        patterns = len(self._patterns)
        triggers = sum(h.trigger_count for h_list in self._hooks.values() for h in h_list)
        return {"hooks": total, "enabled": enabled, "events": events,
                "callbacks": callbacks, "patterns": patterns,
                "total_triggers": triggers, "failures": len(self._failures),
                "log_entries": len(self._trigger_log)}

from collections import deque
from enum import Enum
