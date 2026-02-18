# reason_bench/providers/shared/dag_validator.py

from __future__ import annotations

from typing import Any, Dict, Set


ALLOWED_OPS: Set[str] = {
    "TEXT_LOOKUP",
    "GET_QUANTITY",
    "CONVERT_SCALE",
    "FX_LOOKUP",
    "CPI_LOOKUP",
    "CONST",
    "ADD",
    "MUL",
    "DIV",
    "GT",
    "LT",
    "EQ",
}


def validate_dag_obj(planner: dict) -> dict:
    if not isinstance(planner, dict) or set(planner.keys()) != {"dag"}:
        raise ValueError('Top-level JSON must be exactly {"dag": ...}')

    dag = planner["dag"]
    if not isinstance(dag, dict) or set(dag.keys()) != {"nodes", "output"}:
        raise ValueError("dag must contain exactly keys: nodes, output")

    nodes = dag["nodes"]
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("dag.nodes must be a non-empty list")

    def is_ref(x: Any) -> bool:
        return isinstance(x, str) and x.startswith("ref:")

    def ref_id(x: str) -> str:
        return x.split("ref:", 1)[1]

    ids: Set[str] = set()

    # First pass: validate node shapes + collect ids (so we can validate output/refs)
    for n in nodes:
        if not isinstance(n, dict) or set(n.keys()) != {"id", "op", "args"}:
            raise ValueError("each node must have exactly keys: id, op, args")

        nid = n["id"]
        op = n["op"]
        args = n["args"]

        if not isinstance(nid, str) or not nid:
            raise ValueError("node.id must be non-empty string")
        if nid in ids:
            raise ValueError(f"duplicate node id: {nid}")
        ids.add(nid)

        if not isinstance(op, str) or op not in ALLOWED_OPS:
            raise ValueError(f"invalid op: {op}")

        if not isinstance(args, dict):
            raise ValueError(f"node.args must be object for {nid}")

        # operator-specific args + no extras
        if op == "TEXT_LOOKUP":
            if (
                set(args.keys()) != {"query"}
                or not isinstance(args["query"], str)
                or not args["query"].strip()
            ):
                raise ValueError(
                    f"TEXT_LOOKUP args must be exactly {{query}} (non-empty) in {nid}"
                )

        elif op == "GET_QUANTITY":
            if set(args.keys()) != {"fact"} or not is_ref(args["fact"]):
                raise ValueError(
                    f"GET_QUANTITY args must be exactly {{fact}} with ref: in {nid}"
                )

        elif op == "CONVERT_SCALE":
            if set(args.keys()) != {"q", "target_scale"}:
                raise ValueError(
                    f"CONVERT_SCALE args must be exactly {{q, target_scale}} in {nid}"
                )
            if not is_ref(args["q"]):
                raise ValueError(f"CONVERT_SCALE.q must be ref:<id> in {nid}")
            if not isinstance(args["target_scale"], (int, float)):
                raise ValueError(f"CONVERT_SCALE.target_scale must be number in {nid}")

        elif op == "FX_LOOKUP":
            if set(args.keys()) != {"series_id", "year"}:
                raise ValueError(
                    f"FX_LOOKUP args must be exactly {{series_id, year}} in {nid}"
                )
            if not isinstance(args["series_id"], str) or not args["series_id"].strip():
                raise ValueError(
                    f"FX_LOOKUP.series_id must be non-empty string in {nid}"
                )
            if not isinstance(args["year"], (int, float)):
                raise ValueError(f"FX_LOOKUP.year must be a number in {nid}")

        elif op == "CPI_LOOKUP":
            if set(args.keys()) != {"series_id", "from_year", "to_year"}:
                raise ValueError(
                    f"CPI_LOOKUP args must be exactly {{series_id, from_year, to_year}} in {nid}"
                )
            if not isinstance(args["series_id"], str) or not args["series_id"].strip():
                raise ValueError(
                    f"CPI_LOOKUP.series_id must be non-empty string in {nid}"
                )
            if not isinstance(args["from_year"], (int, float)):
                raise ValueError(f"CPI_LOOKUP.from_year must be a number in {nid}")
            if not isinstance(args["to_year"], (int, float)):
                raise ValueError(f"CPI_LOOKUP.to_year must be a number in {nid}")

        elif op == "CONST":
            if set(args.keys()) != {"value"} or not isinstance(
                args["value"], (int, float)
            ):
                raise ValueError(
                    f"CONST args must be exactly {{value}} (number) in {nid}"
                )

        elif op in {"ADD", "MUL", "DIV", "GT", "LT", "EQ"}:
            if set(args.keys()) != {"a", "b"}:
                raise ValueError(f"{op} args must be exactly {{a,b}} in {nid}")
            if not is_ref(args["a"]) or not is_ref(args["b"]):
                raise ValueError(f"{op}.a and {op}.b must be ref:<id> in {nid}")

        else:
            # should be unreachable because of ALLOWED_OPS
            raise ValueError(f"unhandled op: {op}")

    # Validate dag.output
    if not is_ref(dag["output"]):
        raise ValueError("dag.output must be a ref:<id> string")
    out_id = ref_id(dag["output"])
    if out_id not in ids:
        raise ValueError(f"dag.output references unknown node id: {out_id}")

    # Second pass: validate that all refs point to existing nodes AND to earlier nodes
    # (enforces acyclic forward-only dataflow)
    seen: Set[str] = set()
    for n in nodes:
        nid = n["id"]
        op = n["op"]
        args = n["args"]

        def check_ref(r: str, *, field: str) -> None:
            rid = ref_id(r)
            if rid not in ids:
                raise ValueError(f"{nid}.{field} references unknown node id: {rid}")
            if rid not in seen:
                raise ValueError(
                    f"{nid}.{field} references node {rid} that appears later (or same node). "
                    "Refs must point to earlier nodes."
                )

        if op == "GET_QUANTITY":
            check_ref(args["fact"], field="fact")
        elif op == "CONVERT_SCALE":
            check_ref(args["q"], field="q")
        elif op in {"ADD", "MUL", "DIV", "GT", "LT", "EQ"}:
            check_ref(args["a"], field="a")
            check_ref(args["b"], field="b")

        seen.add(nid)

    return dag
