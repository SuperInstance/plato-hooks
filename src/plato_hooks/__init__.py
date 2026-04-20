"""Hooks — git hooks for real-time room events, commit triggers.
Part of the PLATO framework."""
from .hooks import HookManager, Hook
__version__ = "0.1.0"
__all__ = ["HookManager", "Hook"]
