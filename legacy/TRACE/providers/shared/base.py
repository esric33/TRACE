from typing import Any, Dict, List, Protocol, Optional
from pathlib import Path
import json


class Planner(Protocol):
    def __call__(self, capsule: Dict[str, Any]) -> Dict[str, Any]: ...


class Lookup(Protocol):
    def __call__(
        self,
        node_id: str,
        query: str,
        capsule: Dict[str, Any],
        extracts_by_snippet: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]: ...


class Provider(Protocol):
    def make_plan_fn(self) -> Optional[Planner]: ...
    def make_lookup_fn(self) -> Lookup: ...


def load_schema_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
