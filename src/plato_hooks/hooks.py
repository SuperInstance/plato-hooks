"""Git hook management for room events."""
import subprocess, os, json, time
from dataclasses import dataclass, field
from typing import Callable, Optional

@dataclass
class Hook:
    name: str
    event: str
    command: str
    enabled: bool = True
    trigger_count: int = 0
    last_triggered: float = 0.0

class HookManager:
    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self._hooks: dict[str, list[Hook]] = {}

    def register(self, name: str, event: str, command: str) -> Hook:
        h = Hook(name=name, event=event, command=command)
        if event not in self._hooks:
            self._hooks[event] = []
        self._hooks[event].append(h)
        return h

    def trigger(self, event: str, context: dict = None) -> list[dict]:
        results = []
        for h in self._hooks.get(event, []):
            if not h.enabled: continue
            h.trigger_count += 1
            h.last_triggered = time.time()
            try:
                result = subprocess.run(h.command, shell=True, capture_output=True,
                                       text=True, cwd=self.repo_path, timeout=30)
                results.append({"hook": h.name, "status": "ok" if result.returncode == 0 else "error",
                               "output": result.stdout[:500], "code": result.returncode})
            except Exception as e:
                results.append({"hook": h.name, "status": "error", "output": str(e)[:200]})
        return results

    def enable(self, name: str): 
        for hooks in self._hooks.values():
            for h in hooks:
                if h.name == name: h.enabled = True
    def disable(self, name: str):
        for hooks in self._hooks.values():
            for h in hooks:
                if h.name == name: h.enabled = False

    @property
    def stats(self) -> dict:
        events = {e: len(hs) for e, hs in self._hooks.items()}
        total = sum(h.trigger_count for hs in self._hooks.values() for h in hs)
        return {"events": events, "total_hooks": sum(len(v) for v in self._hooks.values()),
                "total_triggers": total}
