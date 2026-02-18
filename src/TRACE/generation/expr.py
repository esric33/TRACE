from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple


@dataclass(frozen=True)
class LookupQty:
    """
    AST leaf: “lookup the fact bound to var_name and return its quantity”.
    This expands to TEXT_LOOKUP + GET_QUANTITY in the executor language.
    """

    var_name: str


@dataclass(frozen=True)
class ConvertScale:
    expr: object
    target_scale_in: Tuple[float, ...]


@dataclass(frozen=True)
class ConvertScaleTo:
    expr: object
    to_var: str


@dataclass(frozen=True)
class Add:
    left: object
    right: object


@dataclass(frozen=True)
class CpiLookup:
    """
    Lookup CPI adjustment rate between the FY years of two bound records.

    Semantics: CPI(to_year) / CPI(from_year), i.e. multiply money(from_year) by this
    to express in to_year dollars.
    """

    from_var: str
    to_var: str
    series_id: str = "CPI_US_CPIU"


@dataclass(frozen=True)
class FxLookup:
    var_name: str
    base: Optional[str] = None  # if None, base := bindings[var_name].unit
    quote: Optional[str] = None  # if None, sample from quote_in
    quote_in: Optional[Sequence[str]] = (
        None  # allowed quotes (post-filtered by availability)
    )


@dataclass(frozen=True)
class FxLookupTo:
    """
    Deterministic FX lookup where quote currency is taken from another bound var.

    base  := base if provided else unit(from_var)
    quote := unit(to_var)

    Intended use: convert money(from_var) into the currency of to_var (same FY year).
    """

    from_var: str
    to_var: str
    base: Optional[str] = None


@dataclass(frozen=True)
class FxLookupAt:
    """
    FX(base->quote) at the FY year of at_var.
    base defaults to unit(base_var) unless provided.
    quote defaults to unit(quote_var) unless provided.
    """

    base_var: str  # where base currency comes from (usually A)
    at_var: str  # year comes from (usually B)
    quote_var: str  # quote currency comes from (usually B)
    base: Optional[str] = None
    quote: Optional[str] = None


# -------------------
# NEW: scalar constant
# -------------------
@dataclass(frozen=True)
class Const:
    value: float


# -------------------
# NEW: division
# -------------------
@dataclass(frozen=True)
class Div:
    left: object
    right: object


# -------------------
# NEW: multiplication
# -------------------
@dataclass(frozen=True)
class Mul:
    left: object
    right: object


@dataclass(frozen=True)
class Gt:
    left: object
    right: object


@dataclass(frozen=True)
class Lt:
    left: object
    right: object


@dataclass(frozen=True)
class Eq:
    left: object
    right: object
