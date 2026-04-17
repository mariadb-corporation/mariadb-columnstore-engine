# `dbcon/rbo/lib` — RBO primitive library

Low-level, rule-agnostic building blocks used by every file under
`dbcon/rbo/`.  Each module is independently tested under
`columnstore/tests/rbo_lib_*_test.cpp`.

The goal is that an RBO rule reads as a sequence of plan-level intent
("promote this CSEP to a derived table", "wrap this column as
`SELECT_SOME(col)`"), not as raw `new execplan::ParseTree(new
execplan::LogicOperator("and"))` boilerplate.

## Module index

| Module | Purpose |
|---|---|
| [`parse_tree_ops`](#parse_tree_ops) | Construction and folding of `ParseTree` AND/OR skeletons. |
| [`filter_builders`](#filter_builders) | Construction of `SimpleFilter` leaves, `PredicateOperator`s and common `ConstantColumn` values. |
| [`derived_column`](#derived_column) | Three entry points for binding a `SimpleColumn` to a derived-table projection slot. |
| [`derived_table`](#derived_table) | CSEP-level "promote to derived subquery" and "wrap as derived" transitions. |
| [`agg_wrap`](#agg_wrap) | Wrap a column in `AggregateColumn(SELECT_SOME)` + structural deduplication of expression ids. |
| [`column_classify`](#column_classify) | Classifiers for columns and column-bearing expressions. |
| [`csep_walk`](#csep_walk) | Walkers for nested CSEPs and `OuterJoinOnFilter` parse-tree leaves. |

---

## `parse_tree_ops`

Factories and utilities for the `execplan::ParseTree` AND/OR scaffolding.

**Header:** `lib/parse_tree_ops.h` · **Tests:** `rbo_lib_parse_tree_ops_test.cpp`

| Function | Purpose |
|---|---|
| `newLogicNode(opName)` | Build a fresh `ParseTree(new LogicOperator(opName))`. |
| `newAndNode()` / `newOrNode()` | Convenience wrappers over `newLogicNode("and" / "or")`. |
| `andAll(vec)` / `orAll(vec)` | Left-deep fold of a `vector<ParseTree*>` into a single AND / OR tree.  Returns `nullptr` for an empty input and the sole leaf for a singleton. |
| `andWith(lhs, rhs)` / `orWith(lhs, rhs)` | Binary variant that tolerates either operand being `nullptr`. |
| `collectConjuncts(root, out)` | Flatten a ParseTree into its top-level AND-leaves, descending through AND nodes only. |
| `deleteOneNode(nodePtrPtr)` | Remove a ParseTree node in place, splicing whichever child survives into the parent slot. |
| `replaceInPlace(nodePtrPtr, replacement)` | Swap one ParseTree node for another in place. |
| `isAnd(pt)` / `isOr(pt)` / `logicOpType(pt)` | Classifiers over `LogicOperator` nodes. |

---

## `filter_builders`

Construction of comparison leaves and the two most common
`ConstantColumn` values.

**Header:** `lib/filter_builders.h` · **Tests:** `rbo_lib_filter_builders_test.cpp`

| Function | Purpose |
|---|---|
| `makePredicateOp(opSym, lhsType, rhsType)` | Build a `PredicateOperator(opSym)` with both operand types propagated via `setOpType`, and `resultType` set to `BOOLEAN`. |
| `makeCmpFilter(lhs, opSym, rhs, timeZone)` | Build a `ParseTree(new SimpleFilter(op, lhs, rhs, timeZone))` using `makePredicateOp` under the hood. |
| `makeIsNullFilter(col)` / `makeIsNotNullFilter(col)` | Build an `IS NULL` / `IS NOT NULL` leaf with a `ConstantColumnNull` of the same type as `col`. |
| `makeConstUInt(value, scale, precision)` | Build a `ConstantColumnUInt`. |
| `makeConstNull()` | Build a `ConstantColumnNull`. |

---

## `derived_column`

Three distinct entry points for constructing or re-binding a
`SimpleColumn` that references a derived-table projection slot.  Each
entry point's field-set is documented per-function in the header
(`derived_column.h`) because the historical impls in rewrite_distinct,
decorrelate and parallel_ces touched **different** fields and a single
one-size-fits-all factory would obscure the intent.

**Header:** `lib/derived_column.h` · **Tests:** `rbo_lib_derived_column_test.cpp`

| Function | Flavour | Notes |
|---|---|---|
| `bindSCToDerivedProjectionCore(sc, alias, colPos)` | shared core | Sets `tableAlias`, `derivedTable`, `colPosition` — the universal minimum. |
| `cloneAsSimpleColumn(rc, alias, colPos)` | rewrite_distinct | Build a new `SimpleColumn` from an arbitrary `ReturnedColumn`.  Richest field set (charsetNumber, operationType, colSource, alias, derivedRefCol with one-level-indirection-follow). |
| `makeDerivedColumnRef(refCol, alias, colPos, tz)` | decorrelate | Build a new `SimpleColumn` whose `columnName` is `refCol->alias()`.  Sets `tableName`, `data`, `resultType`, `timeZone`, `sequence`, `derivedRefCol` (same indirection-follow as the distinct flavour). |
| `rebindSCToDerivedInPlace(sc, alias, colPos, scAlias?)` | parallel_ces | Mutate an existing `SimpleColumn` to reference a derived projection slot.  Preserves the parser-time `sequence` deliberately. |

---

## `derived_table`

Plan-level primitives for "promote this CSEP to a derived subquery" and
"wrap this CSEP as a derived subquery".

**Header:** `lib/derived_table.h` · **Tests:** `rbo_lib_derived_table_test.cpp`

| Function | Purpose |
|---|---|
| `promoteCSEPToDerived(csep, alias)` | In-place: set `derivedTbAlias`, mark the CSEP as a subquery, clear outer-only state (e.g. `subType`).  Used when an existing CSEP is moving from outer-query role to derived-table role. |
| `wrapCSEPAsDerived(inner, alias, outerSessionID, outerTimeZone)` | Allocate a fresh outer CSEP, attach `inner` as a derived table, propagate session/timezone, and return the new outer CSEP together with pre-built projection columns pointing at `inner`'s returned columns. |

---

## `agg_wrap`

Wrap a `ReturnedColumn` as `AggregateColumn(SELECT_SOME, col)` plus a
small deduplicator for structurally-equal expressions.

**Header:** `lib/agg_wrap.h` · **Tests:** `rbo_lib_agg_wrap_test.cpp`

| Entity | Purpose |
|---|---|
| `wrapIntoSelectSomeAgg<ColPtr>(rc, timeZone)` | Factory that builds a fresh `AggregateColumn(SELECT_SOME)` with `alias`, `asc`, `charsetNumber` (via `resultType`), `orderPos`, `resultType`, `sessionID` copied from `rc`. |
| `struct AggExprDedup` | Deduplicates SELECT_SOME expressions via `AggregateColumn::operator==` and hands out monotonically-increasing expression ids through `assignId(ac)`. |

Note on the equality semantics observed during testing (documented in
the test file): `AggregateColumn::operator==` compares `aggParms[]`
through `ReturnedColumn::operator==`, which looks only at base-class
fields.  `ColType::operator==` in turn compares only a subset of its
fields — notably NOT `colDataType` or `charsetNumber`.  The lib's
`AggExprDedup` therefore relies on the *plan*-visible fields that those
equality operators actually inspect.

---

## `column_classify`

Classifiers for columns and column-bearing expressions.

**Header:** `lib/column_classify.h` · **Tests:** `rbo_lib_column_classify_test.cpp`

| Function | Purpose |
|---|---|
| `columnBelongsToCSTableList(sc, tableList)` | True iff `sc` references a ColumnStore-backed table whose `(schema, table, alias)` triple matches an entry of `tableList`.  Preserves the historical alias-as-table fallback (empty `tbl.table` matches when `sc->tableName() == tbl.alias`). |
| `containsAggregate(col, maxExprIdSink=nullptr)` | Walks through `ParseTree` / `AggregateColumn` aggParms / `ArithmeticColumn` expression / `FunctionColumn` args / `SimpleFilter` lhs+rhs / `WindowFunctionColumn` args+partitions and returns true iff any `AggregateColumn` is reachable.  If `maxExprIdSink` is non-null, every `ReturnedColumn::expressionId()` seen (excluding the `-1` sentinel) updates the tracked maximum. |

---

## `csep_walk`

Recursive walkers over CSEP sub-plan trees and over
`OuterJoinOnFilter`-rooted parse-tree sub-trees.

**Header:** `lib/csep_walk.h` · **Tests:** `rbo_lib_csep_walk_test.cpp`

| Function | Purpose |
|---|---|
| `walkNestedCSEPs<F>(csep, visitor)` | Header-only template.  Visits every CSEP reachable from `csep` via `subSelectList`, `derivedTableList`, `unionVec` in pre-order (but never `csep` itself).  Returning `true` from the visitor stops the walk; the function returns whether the visitor ever stopped. |
| `collectLeavesInOuterJoinOn(root, out, predicate)` | Walks `root`, descending into every `OuterJoinOnFilter`'s inner parse tree, and appends to `out` every leaf whose `TreeNode` satisfies `predicate(...)`.  Non-OJF branches are traversed only to find further OJF subtrees. |

---

## Intentional omissions

A handful of call sites deliberately did **not** migrate to the lib:

- `rbo_apply_rewrite_distinct.cpp`'s ORDER-BY-not-in-projection wrap
  builds an `AggregateColumn` with a narrower field-set than
  `wrapIntoSelectSomeAgg`.  Using the full factory there would change
  plan semantics.
- `rulebased_optimizer.cpp::Rule::walk` mixes CSEP traversal with
  rule-application and context mutation; wrapping it around
  `walkNestedCSEPs` would require either an out-parameter through the
  visitor or splitting pre/post concerns for negligible savings.
- `rbo_apply_parallel_ces.cpp::isOuterQueryColumn` is a simple
  `std::set<string>`-membership lambda with a `$added_sub_` prefix
  carve-out; it shares no meaningful logic with
  `columnBelongsToCSTableList`.

Each of these is noted in the relevant commit message.

---

## Writing a new RBO rule

When adding a new rule under `dbcon/rbo/`, prefer the lib functions
above over hand-rolled constructions.  Typical patterns:

- Building an AND/OR skeleton from a vector of conjuncts:
  `optimizer::lib::andAll(conjuncts)` — not `new ParseTree(new LogicOperator("and"))`.
- Building a `col = const` filter:
  `optimizer::lib::makeCmpFilter(col, "=", optimizer::lib::makeConstUInt(1, 0, 1), tz)`.
- Binding a `SimpleColumn` to a derived-table projection:
  pick the entry point in `derived_column.h` that matches your field-set
  requirements, and document any divergence in your rule's commit.
- Walking sub-CSEPs:
  `optimizer::lib::walkNestedCSEPs(csep, [&](const auto& sub) { ... return stop; })`.

If you catch yourself writing `new execplan::ParseTree(new execplan::...)`
inside a rule file, there's likely a lib primitive that already covers
your case — or a missing one worth adding here.
