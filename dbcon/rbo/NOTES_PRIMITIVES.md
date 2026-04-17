# RBO — каталог примитивов по правилам

Цель: найти повторяющиеся строительные блоки, которые стоит вынести в общую
RBO-библиотеку, чтобы новые правила писались короче, а в старых меньше
копипасты.

Все файлы ниже — в `storage/columnstore/columnstore/dbcon/rbo/`.

---

## 0. Инфраструктура (`rulebased_optimizer.{h,cpp}`)

Уже общее:

- `Rule { name, mayApply, applyRule, applyOnlyOnce }` и `Rule::apply` /
  `Rule::walk` — DFS по дереву CSEP c заходом в `unionVec()` и `subSelectList()`.
- `RBOptimizerContext` — `gwi`, `thd`, `uniqueId`, `maxExpressionId`,
  `appliedRules`, `accumulatedTableAliasMap`, флаг логирования,
  `cesOptimizationParallelFactor`.
- `optimizeCSEPWithRules` — читает включённость правила из config + PRON
  overrides.
- `getRewrittenSubTableAlias(table, ctx)` — генератор уникального
  `$added_sub_<schema>_<table>_<uniqueId>` alias.

Примитивы сравнения таблиц:

- `TableAliasLessThan` (schema+table+alias).
- `TableSchemaTableLessThan` (schema+table only).
- `SimpleColumnLessThan` (по columnName).

Типаж-сравнитель `NodeSemanticComparator` (семантическое сравнение
`ParseTree*`) — определён в `common_leaf_conjunctions.cpp`, но по смыслу общий.

---

## 1. `common_leaf_conjunctions.{h,cpp}` + `rbo_common_leaf_conjunctions_to_top.{h,cpp}`

Назначение: поднять общие листовые конъюнкции наружу WHERE/ON.

Примитивы:

- `castToFilter(ParseTree*) -> Filter*` — `dynamic_cast` враппер.
- `castToSimpleFilter(TreeNode*) -> SimpleFilter*`.
- `operatorType(ParseTree*) -> OpType` (OP_AND/OR/UNKNOWN).
- `oppositeOperator(OpType)` — перевернуть неравенство (GT<->LT, GE<->LE).
- `normalizeNode(lhs, rhs, op)` — лексикографическая нормализация
  SimpleFilter для сравнения семантической эквивалентности.
- `simpleFiltersCmp(SimpleFilter*, SimpleFilter*)`.
- `NodeSemanticComparator` — компаратор для set<ParseTree*>.
- DFS со стек-фреймом `StackFrameWithSet { node, direction, orMet,
  andParent, localset }` — обход парс-дерева с *локальным* множеством на
  каждом уровне и intersection/union при выходе наверх.
- `advanceSetUp` — intersection под OR, union под AND при подъёме.
- `collectCommonConjuctions` — сам обход.
- `StackFrame { node**, direction, containsLeft, containsRight }` — ещё
  один DFS-фрейм, уже для *модификации* дерева по месту.
- `addStackFrame`, `replaceContainsTypeFlag`.
- `deleteOneNode(ParseTree**)` — безопасное удаление узла (обнуляет
  left/right, удаляет, зануляет указатель).
- `fixUpTree(node**, ltype, rtype, parentFrame?)` — replace/unchain/delete
  детей и родителя по флагам `ChildType { Unchain, Delete, Leave }`.
- `removeFromTreeIterative(ParseTree**, common)` — удаление всех копий
  общих конъюнкций итеративным DFS.
- `newAndNode()` — фабрика `ParseTree(LogicOperator("and"))`.
- `appendToRoot(tree, common)` — сшить список конъюнкций как
  right-deep AND над исходным `tree`.
- `dumpTreeFiles(tree, name)` — отладочный dump в .data/.dot/.png
  (под `#if debug_rewrites`).
- `printTreeLevel`, `printContainer` — debug-print помощники.
- `extractCommonLeafConjunctionsToRoot<stableSort>` — верхний API.

---

## 2. `rbo_predicate_pushdown.{h,cpp}`

Назначение: заталкивание фильтров в derived-таблицы (MCS `@bug5635`).

Примитивы:

- `DerivedToFiltersMap = map<string, ParseTree*>` — stack фильтров на
  derivedAlias.
- `setDerivedTable(ParseTree*)` — пометка узла derived table по потомкам
  (правила слияния `*`/`""`/equal).
- `setDerivedFilter(gwip, n, map, derivedTbList)` — рекурсивный walker:
  если узел относится к одной derived table — вынимаем, заменяем на
  `ConstantColumn(1)`; иначе рекурсия, с обрубом на OR/XOR.
- Создание AND-узла `new ParseTree(new LogicOperator("and"))` + `left/right`
  — повторяется 3 раза (main filter, derived filter, union filter).
- `replaceRefCol(filter, derivedColList)` — замена ссылок на проекцию
  derived (функция уже существует вне RBO).

---

## 3. `rbo_groupby_wrap_columns.{h,cpp}`

Назначение: обернуть в `AggregateColumn(SELECT_SOME)` колонки, не
входящие в GROUP BY (для implicit group by / корректности agg + non-agg).

Примитивы:

- `ColumnWrapperContext { gbCols, origGbCols, tableList, aggExprs,
  nextId, applied }` — локальный контекст правила.
- `isAggregateColumn(col, ctx)` — DFS по variant<ParseTree*, TreeNode*>,
  с обходом AggregateColumn::aggParms, ArithmeticColumn::expression,
  FunctionColumn::functionParms, SimpleFilter::lhs/rhs,
  WindowFunctionColumn::functionParms/partitions. Попутно обновляет
  `ctx.maxExpressionId`.
- `hasAggregateColumns(csep, ctx)` — пробегает returnedCols, orderByCols,
  having.
- `needWrap(TreeNode*, lctx)` — классификатор: нужна ли обёртка.
  Содержит *логику владения таблицей*: `sc.schema/table/alias` против
  `tableList` с учётом alias-как-table fallback.
- `wrapColumn<T>(rc, lctx, ctx)` — создаёт `AggregateColumn(SELECT_SOME)`,
  дедуплицирует по `aggExprs`, переиспользует expressionId.
- Собственный мануальный DFS (`Stack::Frame { node, step }`) для
  хождения по ParseTree + TreeNode-детям (ArithmeticColumn,
  FunctionColumn, SimpleFilter, WindowFunctionColumn), со сменой
  контекста `gbCols` (empty при входе в arith/func/window).
- `processColumn(rc, lctx, ctx)` — итеративный walker, применяющий
  `wrapColumn` на нужных листьях.
- `wrapIntoAggregate(SRCP&, lctx, ctx)`.

---

## 4. `rbo_decorrelate_outer_join_sub.{h,cpp}` (MCOL-4250)

Назначение: превратить коррелированный scalar-subquery в ON LEFT JOIN
в equi-join с derived table.

Примитивы:

- `SubqueryPattern { leafNode, selectFilter, sub, subAlias, aggCol,
  aggColPos, corrEquis, localPreds }` — карточка матча.
- `CorrEqui { subSide, outerSide, filterNode }`.
- `collectSelectFiltersInOJF(root, out)` — DFS по `OuterJoinOnFilter`
  листьям, собирает SelectFilter/SimpleScalarFilter внутри ON.
- `collectConjuncts(root, leaves)` — декомпозиция AND-дерева в вектор
  листьев (fail если не чистый AND).
- `isSupportedAggOp(op)` — whitelist.
- `matchSubqueryPattern(leaf, out)` — pattern matcher с кучей шейповых
  предикатов на sub-CSEP (`tableList.size()==1`, нет union/derived/
  subselect/having/distinct, один AGG в проекции и т.д.).
- `treeHasScalarSubFilter(root)` / `treeHasUnsupportedOuterJoinSub(root)`
  — shape-прекондиция для filter’а правила.
- `buildAndTree(leaves)` — right-deep AND из вектора листьев
  (аналог `appendToRoot` в common_leaf_conjunctions).
- `makeDerivedColumnRef(refCol, derivedAlias, colPos, tz)` —
  свежий SimpleColumn, ссылающийся на проекцию derived. Устанавливает
  `columnName=alias`, `tableAlias=derivedTable=derivedAlias`,
  `derivedRefCol`, `colPosition`, `sequence`, `resultType`, `timeZone`,
  `incRefCount`.
- `rewriteMatchedPattern`: включает операции:
  - генерация уникальных `derivedAlias`/`aggAlias`/`dec_kN` суффиксов по
    `ctx.uniqueId`;
  - присвоение alias’ов returnedCols суб-CSEP;
  - deep-copy локальных предикатов (`new ParseTree(*lp)`), удаление
    старого filter-дерева, подстановка нового через `buildAndTree`;
  - promotion суб-CSEP: `location = FROM`, `subType = FROM_SUBS`,
    `derivedTbAlias`;
  - transfer `SCSEP` из SelectFilter в `derivedTableList`, добавление в
    `tableList`;
  - генерация equi-предикатов + main-предиката как AND-дерева;
  - **in-place swap содержимого ParseTree-узла** (data/left/right),
    чтобы сохранить место в окружающем OJF-дереве;
  - удаление старого TreeNode.
- `outerJoinOnContainsScalarSubselect(csep)` — рекурсивный обход по
  subSelectList/derivedTableList/unionVec (дублирует часть `Rule::walk`).

---

## 5. `rbo_apply_rewrite_distinct.{h,cpp}`

Назначение: `SELECT DISTINCT ...` -> derived table + `GROUP BY`.

Примитивы:

- `cloneAsSimpleColumn(rc, tableAlias, colPos)` — делает SimpleColumn,
  ссылающийся на derived-проекцию (по сути родственник
  `makeDerivedColumnRef` из `decorrelate_outer_join_sub`, только ещё
  переносит timeZone c учётом конкретного подтипа
  Simple/Function/Aggregate/WindowFunctionColumn).
- «Переобёртывание CSEP в derived»:
  - `origCSEP = csep.clone()`
  - `origCSEP->location/subType/derivedTbAlias` = FROM/FROM_SUBS/alias
  - `csep.tableList({make_aliasview("", "", alias, "")})`
  - `csep.derivedTableList({origCSEP})`
  - сброс `subSelectList`, `subSelects`, `selectSubList`, `unionVec`,
    `filters`, `having`, `distinct`.
  Этот sequence почти буквально повторяется и в parallelCES.
- Сопоставление ORDER BY с проекцией, докидывание недостающих колонок в
  projection и обёртывание недостающих в `AggregateColumn(SELECT_SOME)`
  (пересекается по идее с `groupby_wrap_columns`).

---

## 6. `rbo_apply_parallel_ces.{h,cpp}`

Назначение: CES (column engine scan) — создать derived с UNION’ом
range-партиций для foreign table со статистикой.

Примитивы:

- `tableIsInUnion(table, csep)`.
- `someAreForeignTables(csep)`, `someForeignTablesHasStatisticsAndMbIndex`.
- `filtersWithNewRange(csep, column, bound, isLast)` — строит
  `col >= low AND col < high`, для последнего bound — inclusive `<=` и
  дистрибуция с `IS NULL` (`(A OR isNull) AND (B OR isNull)`). Затем
  AND’ится с текущим `csep->filters()`. **Много ручного new-аллокаций
  SimpleColumn / ConstantColumn / ParseTree / LogicOperator** — типовая
  «конструкторная фабрика».
- `chooseKeyColumnAndStatistics(table, ctx)` — поиск подходящей колонки
  по статистике.
- `decodeU64(bytes)` — memcpy в uint64_t.
- `populateRangeBoundsFromHistogram<T>`, `populateRangeBoundsFromEquallyDistributedRange<T>`,
  `populateRangeBounds<T>` — диспатч по типу статистики.
- `makeUnionFromTable(csep, table, ctx)` — создание вектора клонов
  CSEP’а с разными range-фильтрами (`csep.cloneForTableWORecursiveSelectsGbObHaving`).
- `createDerivedTableFromTable(csep, table, newAlias, ctx, scToPosMap)` —
  аналогично «сделать derived CSEP», что и в rewrite_distinct, + наполнение
  projection из `SCToPosCounterMap`.
- `updateScToUseRewrittenDerived(sc, newAlias, pos, scAlias)` — правка SC
  in-place (`oid=0, schemaName="", tableName/Alias/derivedTable=alias,
  data=\``.`alias`.`col```, colPosition, isColumnStore=true`).
- `findOrInsertColumnPosition(sc, map, curCursor) -> {pos, isNew}` —
  счётчик позиций для derived projection.
- `cloneSCForDerivedProjection(sc)` — `clone()` + сбросить `joinInfo`.
- `tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap)` —
  main update entry.
- `updateSCsUsingIteration(map, rcs)` — цикл по RC →
  `simpleColumnListExtended` → update.
- `updateSCsUsingWalkers(map, pt)` — через `ParseTree::walk` +
  `getSimpleColsExtended`.
- `findPositionsForExtraSCs(map, extraSCs) -> SCsAndTheirProjectionPositions`.
- Лямбда `updateExistsCorrelated` — ходит по EXISTS/SimpleScalarFilter/
  SelectFilter и для outer-колонок вызывает `tryToUpdateScToUseRewrittenDerived`.
  (Здесь же встречается та же тема «узнать, что колонка принадлежит
  внешнему запросу» — аналог `needWrap`-логики в groupby_wrap).
- Сопровождение локального/аккумулированного `TableAliasToNewAliasAndSCPositionsMap`
  (также используется в контексте для передачи между CSEP’ами).

---

## 7. `check_filters_limit.{h,cpp}`

Примитив-утилита: `checkFiltersLimit(tree, limit)` — walker, считающий
макс размер `OR`/`IN` списка. Уже «библиотечно» написан, шаблон для
других лимит-чекеров.

---

# Кандидаты в `rbo::lib/*` (RBO toolbox)

Сгруппировал по функциональным блокам; уже явно повторяется
≥ 2 раза между правилами.

### A. ParseTree building blocks  (нужно почти всем)

- `newAndNode()` / `newOrNode()` / `newLogicNode(op)` — фабрика.
  (commonLeafConj + predicatePushdown + parallelCES + decorrelate).
- `andAll(vector<ParseTree*>) -> ParseTree*` — right-deep AND
  (aka `appendToRoot` / `buildAndTree`). ⇒ **унифицировать одной
  реализацией**.
- `andWith(existing, extra)` — add AND при ненулевом левом операнде
  (встречается буквально 4 раза: predicatePushdown × 3 + parallelCES).
- `deleteOneNode(ParseTree**)` — безопасный delete с null’ированием.
- `inPlaceReplaceTreeNode(dst, src)` — swap data/left/right и убить src
  (decorrelate делает это руками — полезно для любого правила, которое
  хочет сохранить место узла в родительском дереве).
- `collectConjuncts(root, out)` — строгий AND-декомпозер
  (decorrelate + потенциально полезен predicatePushdown / common_leaf).
- `walkParseTree(root, visitor, ctx)` — тонкая обёртка над
  `ParseTree::walk` (уже есть в execplan, но часто хочется C++-лямбду
  без void*).

### B. DFS-стек для правки ParseTree

- «Фрейм с direction + side-effect flags» (StackFrame в common_leaf,
  Stack::Frame в groupby_wrap) — общий паттерн. Хотя реализации
  отличаются по наполнению — сам skeleton `enter-left / enter-right /
  up` стоит вынести как template visitor.

### C. Derived table construction

Почти идентичная последовательность в `rewrite_distinct` и
`parallelCES`, частично в `decorrelate_outer_join_sub`:

- `promoteCSEPToDerived(csep, alias)`:
  `location=FROM; subType=FROM_SUBS; derivedTbAlias=alias`.
- `wrapCSEPIntoDerived(outer, inner, alias)` — очистить
  subSelectList/selectSubList/unionVec/filters/having/distinct,
  выставить tableList на `make_aliasview("", "", alias, "")`,
  добавить `derivedTableList.push_back(inner)`.
- `addDerivedTableEntry(csep, alias)` — push в tableList и derivedTableList.

### D. Derived column reference factory

- `makeDerivedColumnRef(refCol, derivedAlias, colPos, timeZone)` —
  унифицирует `cloneAsSimpleColumn` (rewrite_distinct) и
  `makeDerivedColumnRef` (decorrelate_outer_join_sub) и
  `updateScToUseRewrittenDerived` (parallelCES — in-place вариант).
- `inPlaceRebindSCToDerived(sc, alias, pos, scAlias?)` — in-place вариант.

### E. TreeNode predicates / classifiers

- `isAggregateSubtree(node)` — собрать в один API то, что делает
  `isAggregateColumn` в groupby_wrap (walks AggregateColumn, Arithmetic,
  Function, SimpleFilter, WindowFunction).
- `forEachSimpleColumn(tree, f)` / `forEachRC(tree, f)` —
  уже используется через `getSimpleColsExtended`, но фасад сократит
  копипасту в parallelCES и potentially decorrelate.
- `columnBelongsToTableSet(sc, tableList)` — логика из `needWrap`
  (+alias-as-table fallback), также встречается в
  `isOuterQueryColumn` лямбде parallelCES.

### F. SimpleFilter / operator factories

- `makeCmpFilter(lhs, opSym, rhs, tz)` — создать SimpleFilter с
  `PredicateOperator("=")` / "<" / ">=" и т.д. и уже вызвать
  `setOpType`. В `filtersWithNewRange` и `rewriteMatchedPattern`
  повторяется буквально пошагово (new Operator, setOpType, resultType,
  new SimpleFilter, new ParseTree).
- `makeIsNullFilter(col)` — отдельный sugar.
- `makeConstIntColumn<T>(value, tz)` — sugar над ConstantColumnUInt/Int.

### G. OJF / correlation helpers

- `collectFiltersInOuterJoinOn(root, predicate) -> vector<ParseTree*>`
  (decorrelate). Обобщённо: «найти в ParseTree все листья OJF, чьи data
  удовлетворяют предикату».
- `walkSubqueries(csep, visitor)` — рекурсивно по subSelectList /
  derivedTableList / unionVec (decorrelate делает это в
  `outerJoinOnContainsScalarSubselect`, parallelCES — руками,
  `Rule::walk` — тоже, но в другом контексте).

### H. Alias / ID generation

- Уже есть `getRewrittenSubTableAlias` + `ctx.incrementUniqueId`.
- Нужен `makeUniqueAlias(prefix, ctx)` и `makeUniqueColumnAlias(prefix,
  n, ctx)` — то, что decorrelate собирает вручную (`"$dec_sub" +
  uniqSuffix`, `"dec_kN" + uniqSuffix`, `"dec_agg" + uniqSuffix`).

### I. Aggregate wrapping

- `wrapIntoSelectSomeAgg(rc, exprIdAllocator)` — из `groupby_wrap`.
  Этот же приём нужен в `rewrite_distinct` для ORDER BY не в
  projection.
- `AggExprDedup` — структура с вектором `(AggregateColumn*, exprId)` и
  методом lookup/insert (сейчас живёт внутри `ColumnWrapperContext`).

### J. TableAliasToNewAliasAndSCPositionsMap helpers

Используется ТОЛЬКО parallelCES, но API (working/accumulated map,
`findOrInsertColumnPosition`) естественно живёт в общей либе, если в
будущем появятся ещё правила, переписывающие табличные alias’ы.

### K. Debug dump

- `dumpTreeFiles(tree, name)` из common_leaf_conjunctions — можно
  поднять в общую debug-библиотеку RBO, чтобы любое правило могло
  писать before/after .dot.

---

# Ответ на вопрос «можно ли сделать `rbo collections`?»

**Короткий ответ: да, и это имеет смысл.**

Подробнее:

- ~70% «низкоуровневого» кода в правилах — это *манипуляции с
  ParseTree* и *фабрики SimpleFilter/SimpleColumn/LogicOperator*. Эти
  вещи максимально переиспользуемы (группы A, F, D). Это даст самый
  большой выигрыш по строкам и по количеству багов (сейчас в каждом
  правиле свой `new ParseTree(new LogicOperator("and"))` с риском
  double-free).
- ~20% — «шаблонные» операции над CSEP: promote-to-derived, очистка
  полей, привязка к внешнему плану (группа C). Повторяется в трёх
  правилах буквально построчно.
- ~10% — специфичная бизнес-логика каждого правила (pattern match,
  классификация колонок, работа со статистикой). Эту часть унифицировать
  не получится и не нужно — каждое правило действительно решает свою
  задачу.

Правила **достаточно однородны** по форме (`mayApply` + `applyRule`
над `CalpontSelectExecutionPlan`), несмотря на разное назначение, и в
низкоуровневой части явно переиспользуют одни и те же операции над
деревом. Поэтому общая библиотека имеет смысл — правила станут
значительно короче и безопаснее.

Предлагаемая структура (под `dbcon/rbo/lib/`):

- `lib/parse_tree_ops.{h,cpp}` — A, B, K.
- `lib/filter_builders.{h,cpp}` — F.
- `lib/derived_table.{h,cpp}` — C, D, H.
- `lib/column_classify.{h,cpp}` — E.
- `lib/csep_walk.{h,cpp}` — G (walkSubqueries + OJF walkers).
- `lib/agg_wrap.{h,cpp}` — I.
- `lib/alias_map.{h,cpp}` — J (если зрелый).

В первую очередь я бы унифицировал **A + C + D + F** — минимальный
refactor, который удалит самую болезненную копипасту и заметно
упростит добавление новых правил.
