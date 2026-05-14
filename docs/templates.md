# Templates

Templates define how TRACE samples questions and gold reasoning graphs.

A template typically specifies:

- the slots it needs from extracted records,
- constraints over those slots,
- the natural-language question form,
- the gold operation structure,
- metadata used for reporting.

TRACE-UFR groups templates into lookup, arithmetic, and boolean/comparison families. Template registries expose `ALL_SPECS`, `SPECS_BY_ID`, `SPECS_BY_FAMILY`, and `FAMILIES`.
