from __future__ import annotations

from dataclasses import dataclass, field

from TRACE.core.actions.types import ActionDef


@dataclass
class ActionRegistry:
    actions: dict[str, ActionDef] = field(default_factory=dict)

    def register(self, action: ActionDef) -> None:
        if action.name in self.actions:
            raise ValueError(f"action already registered: {action.name}")
        self.actions[action.name] = action

    def require(self, action_name: str) -> ActionDef:
        try:
            return self.actions[action_name]
        except KeyError as exc:
            raise KeyError(f"action not registered: {action_name}") from exc

    def allowed_ops(self) -> set[str]:
        return set(self.actions)
