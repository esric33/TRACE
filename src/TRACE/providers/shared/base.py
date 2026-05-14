from typing import Any, Dict, Protocol, Optional
from pathlib import Path
import json


class Planner(Protocol):
    def __call__(self, capsule: Dict[str, Any]) -> Dict[str, Any]: ...


class Provider(Protocol):
    def make_plan_fn(self) -> Optional[Planner]: ...


def load_schema_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
