from __future__ import annotations

from pathlib import Path


_SRC_PACKAGE = Path(__file__).resolve().parent.parent / "src" / "TRACE"

__path__ = [str(_SRC_PACKAGE)]
__file__ = str(_SRC_PACKAGE / "__init__.py")

exec(compile((_SRC_PACKAGE / "__init__.py").read_text(encoding="utf-8"), __file__, "exec"))
