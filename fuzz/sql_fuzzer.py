#!/usr/bin/env python3
"""
SQL Mutation Fuzzer for MariaDB Columnstore.

Extracts SQL from MTR .test files, applies weighted mutations, and runs the
resulting statements against a live MariaDB Columnstore instance to surface
crashes, hangs, and internal errors.

Preferred shared entry point, prerequisites, and examples:
  bash fuzz/run_sql_fuzzer.sh --help

Post-run review is still expected: deduplicate findings, ignore known
Columnstore limitations, check Jira, and only promote reproduced
HIGH/MEDIUM-realism bugs.
"""

import argparse
import glob
import hashlib
import json
import logging
import os
import queue
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sql_fuzzer")

# ---------------------------------------------------------------------------
# SQL extraction from .test files
# ---------------------------------------------------------------------------

# MTR directives to skip (not SQL)
MTR_DIRECTIVES = re.compile(
    r"^\s*("
    r"--|source|let|if|while|echo|exec|enable_|disable_|connect|disconnect|"
    r"error|replace_|reap|send|sleep|dec|inc|exit|skip|require|result|"
    r"append_file|cat_file|copy_file|diff_files|file_exists|list_files|"
    r"move_file|remove_file|write_file|perl|end|delimiter|"
    r"sorted_result|vertical_results|horizontal_results|query|"
    r"character_set|eval"
    r")",
    re.IGNORECASE,
)


def extract_sql_from_test(filepath):
    """Extract SQL statements from an MTR .test file."""
    statements = []
    current = []

    with open(filepath, "r", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()

            # Skip empty, comments, MTR directives
            if not line or line.startswith("#") or line.startswith("--"):
                continue
            if MTR_DIRECTIVES.match(line):
                continue
            # Skip lines with MTR variables
            if "$" in line or ("@" in line and "@@" not in line):
                continue

            current.append(raw_line.rstrip())

            if line.endswith(";"):
                stmt = " ".join(current).strip()
                if stmt and len(stmt) > 5:
                    # Remove trailing ;
                    stmt = stmt.rstrip(";").strip()
                    statements.append(stmt)
                current = []

    return statements


def load_seed_corpus(test_dirs):
    """Load SQL from all .test files in given directories."""
    all_sql = []
    files_count = 0
    for d in test_dirs:
        for f in sorted(glob.glob(os.path.join(d, "**", "*.test"), recursive=True)):
            stmts = extract_sql_from_test(f)
            if stmts:
                all_sql.extend(stmts)
                files_count += 1
    log.info(f"Loaded {len(all_sql)} SQL statements from {files_count} test files")
    return all_sql


# ---------------------------------------------------------------------------
# Mutation strategies
# ---------------------------------------------------------------------------

# Interesting values for each type category
INTERESTING_INTS = [
    "0", "1", "-1", "127", "128", "255", "256", "-128", "-129",
    "32767", "32768", "65535", "65536", "-32768", "-32769",
    "2147483647", "2147483648", "-2147483648", "-2147483649",
    "9223372036854775807", "-9223372036854775808",
    "18446744073709551615", "NULL",
]

INTERESTING_STRINGS = [
    "''", "' '", "'a'",
    "REPEAT('x', 8000)",
    "REPEAT(CHAR(0), 100)",
    "'\\\\'",
    "'\\n\\r\\t'",
    "X'DEADBEEF'",
    "X'00'",
    "NULL",
    "'1970-01-01'",
    "'9999-12-31'",
    "'not-a-date'",
    "CONCAT(CHAR(0), CHAR(1), CHAR(255))",
    "REPEAT(CHAR(39), 1000)",
    "'äöü中文日本語'",
]

INTERESTING_DATES = [
    "'0000-00-00'", "'0000-00-00 00:00:00'",
    "'1970-01-01'", "'2038-01-19 03:14:07'",
    "'9999-12-31 23:59:59'", "'1000-01-01'",
    "'2024-02-29'", "'2023-02-29'",  # leap year vs not
    "NULL", "'not-a-date'", "'2024-13-32'",
]

INTERESTING_FLOATS = [
    "0.0", "-0.0", "1.0", "-1.0",
    "1e308", "-1e308", "1e-324",
    "1e999", "-1e999",
    "999999999999999999999.999999999",
    "NULL",
]

# SQL operators/functions to inject
EXTRA_PREDICATES = [
    "IS NULL", "IS NOT NULL",
    "= 0", "!= 0", "> 2147483647", "< -2147483648",
    "LIKE '%'", "LIKE ''", "REGEXP '.*'",
    "IN (NULL, 0, '', -1)",
    "BETWEEN -9999999 AND 9999999",
]

AGGREGATE_WRAPPERS = [
    "COUNT({})", "SUM({})", "AVG({})", "MIN({})", "MAX({})",
    "GROUP_CONCAT({})", "COUNT(DISTINCT {})",
]

# Column type mutations for ALTER — Columnstore supported types only
# CS max VARCHAR/CHAR is 8000, no VARBINARY, BLOB stored via dictionary
TYPE_MUTATIONS = [
    "TINYINT", "BIGINT", "DOUBLE", "DECIMAL(38,10)",
    "VARCHAR(1)", "VARCHAR(8000)", "TEXT",
    "DATE", "DATETIME", "TIMESTAMP",
    "CHAR(255)", "CHAR(1)", "INT UNSIGNED",
    "SMALLINT", "MEDIUMINT", "FLOAT",
    "DECIMAL(18,2)", "DECIMAL(5,0)",
]


def mutate_value(val):
    """Mutate a SQL literal value."""
    val = val.strip()
    r = random.random()

    # Detect type and mutate accordingly
    if val.upper() == "NULL":
        return random.choice(INTERESTING_INTS[:5] + INTERESTING_STRINGS[:3])
    if val.startswith("'"):
        if r < 0.5:
            return random.choice(INTERESTING_STRINGS)
        if r < 0.7:
            return random.choice(INTERESTING_DATES)
        return random.choice(INTERESTING_INTS)
    if re.match(r"^-?\d+$", val):
        if r < 0.7:
            return random.choice(INTERESTING_INTS)
        return random.choice(INTERESTING_STRINGS)
    if re.match(r"^-?\d+\.\d+", val):
        if r < 0.7:
            return random.choice(INTERESTING_FLOATS)
        return random.choice(INTERESTING_INTS)
    return val


_LITERAL_RE = re.compile(
    r"(?<![\w'])NULL(?![\w'])|"   
    r"(?<![\w'])-?\d+\.\d+(?:[eE][+-]?\d+)?(?![\w'])|"
    r"(?<![\w'])-?\d+(?![\w'])|"
    r"'(?:[^'\\]|\\.)*'",
    re.IGNORECASE,
)


def mutate_values_in_sql(sql):
    """Find and mutate literal values in a SQL statement."""
    matches = list(_LITERAL_RE.finditer(sql))
    if not matches:
        return sql

    mutate_indexes = {
        i for i in range(len(matches))
        if random.random() < 0.3
    }
    if not mutate_indexes:
        mutate_indexes.add(random.randrange(len(matches)))

    result = []
    last = 0
    for i, match in enumerate(matches):
        result.append(sql[last:match.start()])
        literal = match.group(0)
        result.append(mutate_value(literal) if i in mutate_indexes else literal)
        last = match.end()
    result.append(sql[last:])
    return "".join(result)


# SQL keywords to exclude when picking "column" identifiers
_SQL_KEYWORDS = frozenset({
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS",
    "NULL", "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
    "CROSS", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "DROP", "ALTER", "TABLE", "DATABASE", "INDEX", "IF",
    "EXISTS", "ENGINE", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
    "UNION", "ALL", "DISTINCT", "BETWEEN", "LIKE", "CASE", "WHEN",
    "THEN", "ELSE", "END", "ASC", "DESC", "PRIMARY", "KEY", "AUTO_INCREMENT",
    "DEFAULT", "UNSIGNED", "SIGNED", "INT", "BIGINT", "TINYINT",
    "SMALLINT", "MEDIUMINT", "VARCHAR", "CHAR", "TEXT", "BLOB",
    "DATE", "DATETIME", "TIMESTAMP", "FLOAT", "DOUBLE", "DECIMAL",
    "REPLACE", "IGNORE", "WITH", "RECURSIVE", "TEMPORARY", "VIEW",
    "COLUMNSTORE", "INNODB", "MYISAM", "TRUE", "FALSE", "USE",
    "REPEAT", "COUNT", "SUM", "AVG", "MIN", "MAX", "CONCAT",
})

_KNOWN_TABLE_COLUMNS = {
    "t1": ("c_tinyint", "c_smallint", "c_int", "c_bigint", "c_decimal", "c_float", "c_double", "c_char", "c_varchar", "c_text", "c_date", "c_datetime", "c_timestamp"),
    "t2": ("c_int", "c_bigint", "c_decimal", "c_varchar", "c_date"),
    "t3": ("id", "val"),
    "t4": ("c_int", "c_char", "c_decimal"),
    "datatypetestm": ("cidx", "CTINYINT", "CSMALLINT", "CINTEGER", "CBIGINT", "CDECIMAL1", "CDECIMAL5", "CDECIMAL9", "CDECIMAL18", "CDECIMAL18_2", "CFLOAT", "CDOUBLE", "CCHAR3", "CCHAR5", "CCHAR6", "CCHAR7", "CCHAR8", "CCHAR9", "CCHAR255", "CVCHAR6", "CVCHAR255", "CTEXT", "CDATE", "CDATETIME"),
    "region": ("r_regionkey", "r_name", "r_comment"),
    "nation": ("n_nationkey", "n_name", "n_regionkey", "n_comment"),
    "orders": ("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"),
    "customer": ("c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"),
    "lineitem": ("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"),
    "cs1": ("a", "b", "c", "d"),
    "cs2": ("a", "b", "c"),
    "cs3": ("a", "b"),
    "cs4": ("d1", "d2"),
    "cs5": ("a", "b"),
    "cs6": ("d1", "d2"),
    "utest1": ("ukey", "c1", "c2", "c3"),
    "utest3": ("a", "b"),
    "test_cs": ("a", "b"),
    "test_innodb": ("a", "b"),
    "test_mult": ("indemnity_paid", "n_clms"),
    "bug5096": ("id", "c1"),
    "mcol979": ("b", "h"),
    "t": ("x",),
    "zu": ("hu",),
    "three_cols": ("a", "b", "c"),
    "emp": ("id", "name", "dept", "salary"),
}

_KNOWN_TABLES = frozenset(name.upper() for name in _KNOWN_TABLE_COLUMNS)
_KNOWN_COLUMNS = frozenset(col.upper() for cols in _KNOWN_TABLE_COLUMNS.values() for col in cols)
_KNOWN_NUMERIC_COLUMNS = frozenset({
    "C_TINYINT", "C_SMALLINT", "C_INT", "C_BIGINT", "C_DECIMAL", "C_FLOAT", "C_DOUBLE",
    "CIDX", "CTINYINT", "CSMALLINT", "CINTEGER", "CBIGINT", "CDECIMAL1", "CDECIMAL5", "CDECIMAL9", "CDECIMAL18", "CDECIMAL18_2", "CFLOAT", "CDOUBLE",
    "R_REGIONKEY", "N_NATIONKEY", "N_REGIONKEY", "O_ORDERKEY", "O_CUSTKEY", "O_TOTALPRICE", "O_SHIPPRIORITY",
    "C_CUSTKEY", "C_NATIONKEY", "C_ACCTBAL", "L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX",
    "A", "D1", "D2", "UKEY", "C1", "C3", "INDEMNITY_PAID", "N_CLMS", "ID", "H", "HU", "DEPT", "SALARY",
})

def _find_top_level_clauses(sql, keywords):
    upper = sql.upper()
    wanted = tuple(sorted((kw.upper() for kw in keywords), key=len, reverse=True))
    matches = []
    depth = 0
    in_single = False
    in_double = False
    in_backtick = False
    i = 0

    while i < len(upper):
        ch = upper[i]
        if in_single:
            if ch == "'" and (i == 0 or upper[i - 1] != "\\"):
                in_single = False
            i += 1
            continue
        if in_double:
            if ch == '"' and (i == 0 or upper[i - 1] != "\\"):
                in_double = False
            i += 1
            continue
        if in_backtick:
            if ch == "`":
                in_backtick = False
            i += 1
            continue
        if ch == "'":
            in_single = True
            i += 1
            continue
        if ch == '"':
            in_double = True
            i += 1
            continue
        if ch == "`":
            in_backtick = True
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth == 0:
            matched = False
            for keyword in wanted:
                end = i + len(keyword)
                if not upper.startswith(keyword, i):
                    continue
                if i > 0 and (upper[i - 1].isalnum() or upper[i - 1] == "_"):
                    continue
                if end < len(upper) and (upper[end].isalnum() or upper[end] == "_"):
                    continue
                matches.append((keyword, i))
                i = end
                matched = True
                break
            if matched:
                continue
        i += 1
    return matches


def _find_first_top_level(sql, keywords, start=0):
    for keyword, pos in _find_top_level_clauses(sql, keywords):
        if pos >= start:
            return keyword, pos
    return None, None


def _insert_before_tail(sql, fragment, keywords):
    _, pos = _find_first_top_level(sql, keywords)
    if pos is None:
        return sql + fragment
    return sql[:pos].rstrip() + fragment + " " + sql[pos:].lstrip()


def _extract_table_aliases(sql):
    aliases = set()
    for table, alias in re.findall(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_]\w*)(?:\s+(?:AS\s+)?([a-zA-Z_]\w*))?",
        sql,
        re.IGNORECASE,
    ):
        aliases.add(table.upper())
        if alias and alias.upper() not in _SQL_KEYWORDS:
            aliases.add(alias.upper())
    return aliases


def _base_column_name(identifier):
    return identifier.rsplit(".", 1)[-1]


def _is_likely_numeric_column(column_name):
    return column_name.upper() in _KNOWN_NUMERIC_COLUMNS


def _numeric_column_family(column_name):
    base = _base_column_name(column_name).upper()
    if not _is_likely_numeric_column(base):
        return None
    if any(token in base for token in [
        "DECIMAL", "FLOAT", "DOUBLE", "ACCTBAL", "TOTALPRICE",
        "EXTENDEDPRICE", "DISCOUNT", "TAX", "PAID",
    ]):
        return "fractional"
    return "integer"


def _collect_identifier_candidates(sql, prefer_numeric=False, aliases=None):
    aliases = set(aliases or ())
    qualified_candidates = []
    known_candidates = []
    fallback_candidates = []

    for prefix, column in re.findall(r"\b([a-zA-Z_]\w*)\.([a-zA-Z_]\w*)\b", sql):
        if column.upper() in _SQL_KEYWORDS:
            continue
        if prefix.upper() not in aliases and prefix.upper() not in _KNOWN_TABLES:
            continue
        if prefer_numeric and not _is_likely_numeric_column(column):
            continue
        qualified_candidates.append(f"{prefix}.{column}")

    for match in re.finditer(r"\b([a-zA-Z_]\w*)\b", sql):
        token = match.group(1)
        upper_token = token.upper()
        if upper_token in _SQL_KEYWORDS or upper_token in aliases or upper_token in _KNOWN_TABLES:
            continue
        if match.start() > 0 and sql[match.start() - 1] == ".":
            continue
        if match.end() < len(sql) and sql[match.end()] == ".":
            continue
        if sql[match.end():].lstrip().startswith("("):
            continue
        if upper_token in _KNOWN_COLUMNS:
            if prefer_numeric and not _is_likely_numeric_column(token):
                continue
            known_candidates.append(token)
        elif not prefer_numeric:
            fallback_candidates.append(token)

    return qualified_candidates or known_candidates or fallback_candidates


def _pick_column(sql, prefer_numeric=False, aliases=None):
    """Extract likely column identifiers from SQL, excluding keywords and table names."""
    if aliases is None:
        aliases = _extract_table_aliases(sql)
    candidates = _collect_identifier_candidates(sql, prefer_numeric=prefer_numeric, aliases=aliases)
    return random.choice(candidates) if candidates else None


def mutate_add_predicate(sql):
    """Add a random predicate to WHERE clause or append one."""
    pred = random.choice(EXTRA_PREDICATES)
    col = _pick_column(sql)
    if not col:
        return sql
    _, where_pos = _find_first_top_level(sql, ["WHERE"])
    if where_pos is not None:
        idx = where_pos + len("WHERE")
        return sql[:idx] + f" {col} {pred} AND" + sql[idx:]
    return _insert_before_tail(
        sql,
        f" WHERE {col} {pred}",
        ["GROUP BY", "HAVING", "ORDER BY", "LIMIT", "UNION"],
    )


def mutate_wrap_aggregate(sql):
    """Wrap a column reference in an aggregate function."""
    upper = sql.upper()
    if "SELECT" not in upper:
        return sql
    # Find columns between SELECT and FROM
    m = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return sql
    select_list = m.group(1)
    cols = _collect_identifier_candidates(select_list, aliases=_extract_table_aliases(sql))
    if not cols:
        return sql
    col = random.choice(cols)
    wrapper = random.choice(AGGREGATE_WRAPPERS)
    new_expr = wrapper.format(col)
    new_select = select_list.replace(col, new_expr, 1)
    return sql[: m.start(1)] + new_select + sql[m.end(1) :]


def mutate_inject_subquery(sql):
    """Replace a value with a subquery."""
    subqueries = [
        "(SELECT NULL)",
        "(SELECT 1 UNION ALL SELECT 2)",
        "(SELECT MAX(1))",
        "(SELECT REPEAT('x', 10000))",
        "(SELECT COUNT(*) FROM t1)",
        "(SELECT MAX(c_int) FROM t2)",
        "(SELECT c_int FROM t1 LIMIT 1)",
        "(SELECT SUM(c_bigint) FROM t2 WHERE c_int > 500)",
        "(SELECT GROUP_CONCAT(c_varchar) FROM t1)",
    ]
    # Find a numeric value and replace with subquery
    pattern = r"(?<!=)\b(\d+)\b"
    matches = list(re.finditer(pattern, sql))
    if matches:
        m = random.choice(matches)
        return sql[: m.start()] + random.choice(subqueries) + sql[m.end() :]
    return sql


def mutate_alter_type(sql):
    """If it's a CREATE TABLE, mutate a column type."""
    if not re.match(r"\s*CREATE\s+TABLE", sql, re.IGNORECASE):
        return sql
    # Find type keywords and replace
    types_pattern = r"\b(INT|BIGINT|TINYINT|SMALLINT|MEDIUMINT|VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT|BLOB|DOUBLE|FLOAT|DECIMAL\(\d+,\d+\)|DATE|DATETIME|TIMESTAMP)\b"
    matches = list(re.finditer(types_pattern, sql, re.IGNORECASE))
    if matches:
        m = random.choice(matches)
        new_type = random.choice(TYPE_MUTATIONS)
        return sql[: m.start()] + new_type + sql[m.end() :]
    return sql


def mutate_remove_clause(sql):
    """Randomly remove GROUP BY, ORDER BY, HAVING, or LIMIT."""
    removable = ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]
    random.shuffle(removable)
    for target in removable:
        kw, pos = _find_first_top_level(sql, [target])
        if pos is None:
            continue
        tail_kws = [k for k in removable if k != target]
        _, next_pos = _find_first_top_level(sql, tail_kws, start=pos + len(kw))
        if next_pos is not None:
            return sql[:pos].rstrip() + " " + sql[next_pos:].lstrip()
        return sql[:pos].rstrip()
    return sql


def mutate_add_union(sql):
    """Append UNION ALL with same query but mutated values."""
    if "SELECT" in sql.upper() and "FROM" in sql.upper():
        return sql + " UNION ALL " + mutate_values_in_sql(sql)
    return sql


def mutate_cs_specific(sql):
    """Inject Columnstore-specific operations."""
    upper = sql.upper()
    r = random.random()
    # Cross-engine join: wrap in a subquery joining InnoDB
    if "SELECT" in upper and "FROM" in upper and r < 0.3:
        return f"SELECT * FROM ({sql}) derived_cs JOIN t3 ON 1=1 LIMIT 10"
    # Prepend a CS internal call before the original query (multi-statement)
    if "SELECT" in upper and r < 0.6:
        cs_call = random.choice([
            "SELECT calGetStats()",
            "SELECT calFlushCache()",
        ])
        return cs_call + ";\n" + sql
    return sql


# --- New mutations for overnight diversification ---

WINDOW_FUNCTIONS = [
    "ROW_NUMBER()", "RANK()", "DENSE_RANK()", "NTILE(4)",
    "SUM({col}) OVER (ORDER BY {col})",
    "AVG({col}) OVER (PARTITION BY {col})",
    "COUNT({col}) OVER ()",
    "MIN({col}) OVER (ORDER BY {col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
    "MAX({col}) OVER (ORDER BY {col} ROWS 5 PRECEDING)",
    "LEAD({col},1) OVER (ORDER BY {col})",
    "LAG({col},1) OVER (ORDER BY {col})",
    "FIRST_VALUE({col}) OVER (ORDER BY {col})",
    "LAST_VALUE({col}) OVER (ORDER BY {col} ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)",
    "NTH_VALUE({col},2) OVER (ORDER BY {col})",
    "STD({col}) OVER (PARTITION BY {col} ORDER BY {col})",
    "VARIANCE({col}) OVER (PARTITION BY abs({col}) ORDER BY {col})",
    "STDDEV_POP({col}) OVER (PARTITION BY {col}+1 ORDER BY {col})",
]


def mutate_add_window_function(sql):
    """Add a window function to a SELECT query."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    col = _pick_column(sql)
    if not col:
        return sql
    wf = random.choice(WINDOW_FUNCTIONS).replace("{col}", col)
    m = re.search(r"(SELECT\s+)(.*?)(\s+FROM)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return sql
    return m.group(1) + m.group(2) + ", " + wf + m.group(3) + sql[m.end():]


JOIN_TYPES = ["JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN", "STRAIGHT_JOIN"]

def mutate_add_join(sql):
    """Add a random JOIN to a SELECT query."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    join_columns = {
        "t1": ["c_tinyint", "c_smallint", "c_int", "c_bigint", "c_decimal"],
        "t2": ["c_int", "c_bigint", "c_decimal"],
        "t3": ["id"],
    }
    alias = f"jt{random.randint(1,99)}"
    join_type = random.choice(JOIN_TYPES)
    compatible_pairs = []
    for col in _collect_identifier_candidates(sql, prefer_numeric=True, aliases=_extract_table_aliases(sql)):
        family = _numeric_column_family(col)
        if family is None:
            continue
        for join_table, columns in join_columns.items():
            matches = [join_col for join_col in columns if _numeric_column_family(join_col) == family]
            if matches:
                compatible_pairs.append((col, join_table, matches))
    if not compatible_pairs and join_type != "CROSS JOIN":
        join_type = "CROSS JOIN"
    if join_type == "CROSS JOIN":
        join_table = random.choice(list(join_columns))
        join_clause = f" {join_type} {join_table} {alias}"
    else:
        col, join_table, matches = random.choice(compatible_pairs)
        join_col = random.choice(matches)
        join_clause = f" {join_type} {join_table} {alias} ON {alias}.{join_col} = {col}"
    _, from_pos = _find_first_top_level(sql, ["FROM"])
    if from_pos is None:
        return sql
    _, insert_pos = _find_first_top_level(
        sql,
        ["WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "UNION"],
        start=from_pos + len("FROM"),
    )
    if insert_pos is None:
        insert_pos = len(sql)
    return sql[:insert_pos].rstrip() + join_clause + " " + sql[insert_pos:].lstrip()


def mutate_wrap_cte(sql):
    """Wrap a SELECT in a CTE."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    cte_name = f"cte_{random.randint(1,99)}"
    return f"WITH {cte_name} AS ({sql}) SELECT * FROM {cte_name} LIMIT 100"


def mutate_transaction(sql):
    """Wrap DML in a transaction with rollback."""
    upper = sql.upper()
    if any(kw in upper for kw in ["INSERT", "UPDATE", "DELETE"]):
        if random.random() < 0.5:
            return f"BEGIN;\n{sql};\nROLLBACK"
        else:
            return f"BEGIN;\n{sql};\nCOMMIT"
    return sql


def mutate_alter_operations(sql):
    """Generate ALTER TABLE operations on a scratch table (never protected ones)."""
    table = "fuzz_scratch"
    col_name = f"fuzz_col_{random.randint(1,999)}"
    new_type = random.choice(TYPE_MUTATIONS)
    ops = [
        f"CREATE TABLE IF NOT EXISTS {table} (id INT) ENGINE=Columnstore",
        f"ALTER TABLE {table} ADD COLUMN {col_name} {new_type}",
        f"ALTER TABLE {table} ADD COLUMN {col_name} {new_type} DEFAULT NULL",
        f"DROP TABLE IF EXISTS {table}",
    ]
    return random.choice(ops)


def mutate_deep_nesting(sql):
    """Create deeply nested subqueries."""
    upper = sql.upper()
    if "SELECT" not in upper:
        return sql
    depth = random.randint(2, 5)
    result = sql
    for i in range(depth):
        result = f"SELECT * FROM ({result}) nested_{i} LIMIT 1000"
    return result


def mutate_combo_agg_window(sql):
    """Specifically combine aggregates with window functions — known crash area."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    col = _pick_column(sql)
    if not col:
        return sql
    agg = random.choice(["COUNT({c})", "SUM({c})", "AVG({c})", "MIN({c})",
                          "MAX({c})", "COUNT(DISTINCT {c})", "GROUP_CONCAT({c})"])
    wf = random.choice([
        "STD({c}) OVER (PARTITION BY abs({c}) ORDER BY {c})",
        "VARIANCE({c}) OVER (ORDER BY {c})",
        "DENSE_RANK() OVER (PARTITION BY {c}+1 ORDER BY {c})",
        "FIRST_VALUE({c}) OVER (ORDER BY {c} ROWS UNBOUNDED PRECEDING)",
        "SUM({c}) OVER (PARTITION BY {c} ORDER BY {c})",
        "LEAD({c},1) OVER (ORDER BY {c})",
    ])
    agg_expr = agg.format(c=col)
    wf_expr = wf.format(c=col)
    m = re.search(r"(SELECT\s+)(.*?)(\s+FROM)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return sql
    return m.group(1) + agg_expr + ", " + wf_expr + m.group(3) + sql[m.end():]


def mutate_set_variable(sql):
    """Prepend SET with interesting session variables."""
    variables = [
        "SET @@columnstore_decimal_scale=10",
        "SET @@columnstore_double_for_decimal_math=1",
        "SET @@columnstore_ordered_only=1",
        "SET @@columnstore_string_scan_threshold=1",
        "SET @@max_length_for_sort_data=4",
        "SET @@group_concat_max_len=10",
        "SET @@max_join_size=1",
        "SET @@sql_mode='STRICT_ALL_TABLES'",
        "SET @@sql_mode=''",
        "SET @@columnstore_use_import_for_batchinsert=0",
    ]
    return random.choice(variables) + ";\n" + sql


def mutate_having_without_groupby(sql):
    """Add HAVING clause without GROUP BY — known to confuse planners."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    if _find_first_top_level(sql, ["GROUP BY"])[1] is not None:
        return sql
    if _find_first_top_level(sql, ["HAVING"])[1] is not None:
        return sql
    col = _pick_column(sql)
    if not col:
        return sql
    agg = random.choice([f"COUNT({col})", f"SUM({col})", f"MAX({col})"])
    cond = random.choice([">", "<", "=", "!="])
    val = random.choice(["0", "1", "-1", "NULL", "100"])
    return _insert_before_tail(
        sql,
        f" HAVING {agg} {cond} {val}",
        ["ORDER BY", "LIMIT", "UNION"],
    )


def mutate_exists_subquery(sql):
    """Add EXISTS / NOT EXISTS with correlated subquery."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    col = _pick_column(sql, prefer_numeric=True)
    if not col:
        return sql
    tables = ["t1", "t2", "t4"]
    tbl = random.choice(tables)
    exist_type = random.choice(["EXISTS", "NOT EXISTS"])
    corr = random.choice([
        f"SELECT 1 FROM {tbl} x WHERE x.c_int = {col}",
        f"SELECT * FROM {tbl} x WHERE x.c_int IS NULL",
        f"SELECT 1 FROM {tbl} x WHERE x.c_int > {col} LIMIT 1",
        f"SELECT {col} FROM {tbl} x GROUP BY 1 HAVING COUNT(*) > 1",
    ])
    _, where_pos = _find_first_top_level(sql, ["WHERE"])
    if where_pos is not None:
        idx = where_pos + len("WHERE")
        return sql[:idx] + f" {exist_type} ({corr}) AND" + sql[idx:]
    return _insert_before_tail(
        sql,
        f" WHERE {exist_type} ({corr})",
        ["GROUP BY", "HAVING", "ORDER BY", "LIMIT", "UNION"],
    )


def mutate_nested_aggregate(sql):
    """Nest aggregates — e.g. COUNT(SUM(x)), known to crash."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    col = _pick_column(sql)
    if not col:
        return sql
    outer = random.choice(["COUNT", "SUM", "AVG", "MAX", "MIN"])
    inner = random.choice(["SUM", "COUNT", "AVG", "MAX", "MIN"])
    m = re.search(r"(SELECT\s+)(.*?)(\s+FROM)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return sql
    return m.group(1) + f"{outer}({inner}({col}))" + m.group(3) + sql[m.end():]


def mutate_prepare_execute(sql):
    """Wrap in PREPARE/EXECUTE — stresses different code paths."""
    upper = sql.upper()
    if not any(kw in upper for kw in ["SELECT", "INSERT", "UPDATE", "DELETE"]):
        return sql
    stmt_name = f"fuzz_stmt_{random.randint(1,999)}"
    escaped = sql.replace("'", "\\'")
    return f"PREPARE {stmt_name} FROM '{escaped}';\nEXECUTE {stmt_name};\nDEALLOCATE PREPARE {stmt_name}"


def mutate_division_by_zero(sql):
    """Inject division by zero or modulo zero into expressions."""
    upper = sql.upper()
    if "SELECT" not in upper:
        return sql
    col = _pick_column(sql, prefer_numeric=True)
    if not col:
        return sql
    expr = random.choice([
        f"{col}/0", f"{col}%0", f"{col}/NULL",
        f"1/({col}-{col})", f"IFNULL({col},0)/0",
        f"POW({col}, 99999)", f"EXP({col}*1000)",
        f"LOG(-1)", f"LOG(0)", f"SQRT(-{col})",
    ])
    m = re.search(r"(SELECT\s+)(.*?)(\s+FROM)", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return sql
    return m.group(1) + m.group(2) + ", " + expr + m.group(3) + sql[m.end():]


def mutate_group_by_rollup(sql):
    """Add GROUP BY ... WITH ROLLUP."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    _, group_pos = _find_first_top_level(sql, ["GROUP BY"])
    if group_pos is not None:
        _, insert_pos = _find_first_top_level(
            sql,
            ["HAVING", "ORDER BY", "LIMIT", "UNION"],
            start=group_pos + len("GROUP BY"),
        )
        group_end = insert_pos if insert_pos is not None else len(sql)
        if "WITH ROLLUP" in sql[group_pos:group_end].upper():
            return sql
        if insert_pos is None:
            return sql.rstrip() + " WITH ROLLUP"
        return sql[:insert_pos].rstrip() + " WITH ROLLUP " + sql[insert_pos:].lstrip()
    col = _pick_column(sql)
    if col:
        return _insert_before_tail(
            sql,
            f" GROUP BY {col} WITH ROLLUP",
            ["HAVING", "ORDER BY", "LIMIT", "UNION"],
        )
    return sql


def mutate_distinct_with_window(sql):
    """Add DISTINCT to SELECT that has window functions — known edge case."""
    upper = sql.upper()
    if "OVER" not in upper and "SELECT" in upper:
        col = _pick_column(sql)
        if col:
            wf = random.choice([
                f"ROW_NUMBER() OVER (ORDER BY {col})",
                f"RANK() OVER (PARTITION BY {col} ORDER BY {col})",
            ])
            m = re.search(r"(SELECT\s+)(.*?)(\s+FROM)", sql, re.IGNORECASE | re.DOTALL)
            if m:
                return m.group(1) + "DISTINCT " + m.group(2) + ", " + wf + m.group(3) + sql[m.end():]
    elif "OVER" in upper and "SELECT" in upper:
        return re.sub(r"SELECT\s+", "SELECT DISTINCT ", sql, count=1, flags=re.IGNORECASE)
    return sql


def mutate_large_in_list(sql):
    """Generate large IN(...) list to stress memory allocation."""
    upper = sql.upper()
    if "SELECT" not in upper or "FROM" not in upper:
        return sql
    col = _pick_column(sql, prefer_numeric=True)
    if not col:
        return sql
    n = random.choice([100, 500, 1000])
    vals = ",".join(str(random.randint(-2**31, 2**31)) for _ in range(n))
    _, where_pos = _find_first_top_level(sql, ["WHERE"])
    if where_pos is not None:
        idx = where_pos + len("WHERE")
        return sql[:idx] + f" {col} IN ({vals}) AND" + sql[idx:]
    return _insert_before_tail(
        sql,
        f" WHERE {col} IN ({vals})",
        ["GROUP BY", "HAVING", "ORDER BY", "LIMIT", "UNION"],
    )


# Weighted mutation strategies
MUTATIONS = [
    (mutate_values_in_sql, 20),       # Mutate literal values
    (mutate_add_predicate, 12),       # Add WHERE conditions
    (mutate_alter_type, 4),           # Mutate column types in DDL
    (mutate_inject_subquery, 8),      # Replace values with subqueries
    (mutate_wrap_aggregate, 6),       # Wrap columns in aggregates
    (mutate_remove_clause, 4),        # Remove GROUP BY/ORDER BY/etc
    (mutate_add_union, 3),            # Add UNION ALL
    (mutate_cs_specific, 3),          # CS-specific ops (trace, flush, cross-engine)
    (mutate_add_window_function, 6),  # Window functions
    (mutate_add_join, 4),             # Random JOINs
    (mutate_wrap_cte, 3),             # CTEs
    (mutate_transaction, 2),          # Transactions
    (mutate_alter_operations, 2),     # ALTER TABLE ops
    (mutate_deep_nesting, 2),         # Deep nesting
    (mutate_combo_agg_window, 5),     # Aggregate + window combos (crash area)
    (mutate_set_variable, 2),         # Session variables
    (mutate_having_without_groupby, 3), # HAVING without GROUP BY
    (mutate_exists_subquery, 4),      # EXISTS/NOT EXISTS correlated
    (mutate_nested_aggregate, 3),     # Nested aggregates COUNT(SUM(x))
    (mutate_prepare_execute, 2),      # PREPARE/EXECUTE
    (mutate_division_by_zero, 3),     # div/0, overflow, math errors
    (mutate_group_by_rollup, 2),      # WITH ROLLUP
    (mutate_distinct_with_window, 2), # DISTINCT + window functions
    (mutate_large_in_list, 2),        # Large IN() lists
]


def pick_mutation():
    """Pick a mutation strategy weighted by probability."""
    total = sum(w for _, w in MUTATIONS)
    r = random.randint(1, total)
    cumulative = 0
    for fn, weight in MUTATIONS:
        cumulative += weight
        if r <= cumulative:
            return fn
    return MUTATIONS[0][0]


# Max query length to prevent OOM from stacked mutations
_MAX_QUERY_LEN = 64000


def mutate_sql(sql, return_metadata=False):
    """Apply 1-5 random mutations to a SQL statement."""
    n_mutations = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5])[0]
    result = sql
    applied_mutations = []
    for _ in range(n_mutations):
        fn = pick_mutation()
        try:
            new_result = fn(result)
        except Exception:
            continue
        if new_result != result:
            applied_mutations.append(fn.__name__)
        result = new_result
        if len(result) > _MAX_QUERY_LEN:
            break
    if return_metadata:
        return result, applied_mutations
    return result


# ---------------------------------------------------------------------------
# Server interaction
# ---------------------------------------------------------------------------

# Pre-compiled regex for known CS limitations — not bugs, skip these
_KNOWN_LIMITATION_RE = re.compile(
    r"MCS-1000|MCS-3022|MCS-3013|MCS-1010|MCS-9022|"
    r"MCS-3008|"                                        # aggregate in EXISTS subquery
    r"MCS-4012|"                                        # autoincrement max exceeded
    r"CrossEngineStep.*credentials|"
    r"value is not numerical|"
    r"default value is out of range|"
    r"AlterTableProcessor|"
    r"varchar and varbinary length may not exceed|"
    r"maximum precision|"
    r"currently not supported in Columnstore|"
    r"is not supported with Columnstore|"
    r"VARBINARY/BLOB in filter|"                        # CS type limitation
    r"GROUP_CONCAT.*ROLLUP|JSONARRAYAGG.*ROLLUP|"       # unsupported combo
    r"Altertable: Error in the action type",            # ALTER limitation
    re.IGNORECASE,
)


# Tables that must NEVER be dropped/truncated/altered-to-different-engine by mutations
_PROTECTED_TABLES = {
    "t1", "t2", "t3", "t4",
    "datatypetestm", "orders", "lineitem", "customer", "region", "nation",
    "cs1", "cs2", "cs3", "cs4", "cs5", "cs6",
    "utest1", "utest3", "test_cs", "test_innodb", "test_mult",
    "bug5096", "mcol979",
}
_PROTECTED_TABLE_RE = re.compile(
    r'\b(?:' + '|'.join(re.escape(t.upper()) for t in _PROTECTED_TABLES) + r')\b'
)


def _is_destructive_to_protected(sql):
    """Return True if SQL would drop/truncate a protected table."""
    upper = sql.upper()
    if not any(kw in upper for kw in ["DROP TABLE", "TRUNCATE", "DROP DATABASE"]):
        return False
    if "DROP DATABASE" in upper and "FUZZ_DB" in upper:
        return True
    return bool(_PROTECTED_TABLE_RE.search(upper))


def _is_blacklisted(sql):
    """Return True if the query matches known hang patterns."""
    upper = sql.upper()
    if "SLEEP(" in upper or "BENCHMARK(" in upper:
        return True
    # Self-referencing INSERT: INSERT INTO x SELECT ... FROM x
    m = re.search(r"INSERT\s+INTO\s+(\w+).*SELECT.*FROM\s+\1\b", upper)
    if m:
        return True
    # REPEAT with huge numbers — OOM risk
    m = re.search(r"REPEAT\s*\(.+?,\s*(\d+)", sql, re.IGNORECASE)
    if m and int(m.group(1)) > 100000:
        return True
    return False


def _is_slow_ddl(sql):
    """Return True if this is DDL that's expected to be slow on CS."""
    upper = sql.upper()
    return any(kw in upper for kw in [
        "ALTER TABLE", "DROP TABLE", "CREATE TABLE",
        "TRUNCATE", "RENAME TABLE",
    ])


class ServerConnection:
    """Manages persistent mysql client connection and crash detection."""

    # Error patterns that indicate the server may have died (check liveness)
    _CONNECTION_ERRORS = re.compile(
        r"ERROR (2002|2003|2006|2013|2026)|Lost connection|"
        r"server has gone away|Can't connect",
        re.IGNORECASE,
    )

    def __init__(self, socket_path=None, host=None, port=None,
                 user="root", password=None, database="mysql"):
        self.socket_path = socket_path
        self.host = host or "127.0.0.1"
        self.port = port or 3306
        self.user = user
        self.password = password
        self.database = database
        self.query_timeout = 10  # seconds
        self._proc = None
        self._stderr_queue = queue.Queue()
        self._stderr_thread = None

    def close(self):
        """Kill persistent mysql client process."""
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.kill()
                self._proc.wait(timeout=2)
            except Exception:
                pass
        self._proc = None

    def _build_cmd(self, force=False):
        # Prefer mariadb client over mysql
        client = "mariadb" if os.path.exists("/usr/bin/mariadb") else "mysql"
        cmd = [client, f"--user={self.user}", f"--database={self.database}",
               "--batch", "--quick", "--silent", "--unbuffered",
               "--connect-timeout=5"]
        if force:
            cmd.append("--force")
        if self.password:
            cmd.append(f"--password={self.password}")
        if self.socket_path:
            cmd.append(f"--socket={self.socket_path}")
        else:
            cmd.extend([f"--host={self.host}", f"--port={self.port}"])
        return cmd

    def _start_persistent(self):
        """Start or restart the persistent mysql client subprocess."""
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.kill()
                self._proc.wait(timeout=2)
            except Exception:
                pass
        cmd = self._build_cmd(force=True)
        # Use binary unbuffered I/O (bufsize=0).  With text=True / bufsize=1
        # Python's TextIOWrapper buffers internally, so select() on the raw
        # fd cannot see data already buffered → false timeouts on large result
        # sets.  Binary mode with bufsize=0 avoids this entirely.
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._stderr_queue = queue.Queue()
        self._stderr_thread = threading.Thread(
            target=self._stderr_reader, daemon=True,
        )
        self._stderr_thread.start()
        # Tell the server to cancel queries that exceed the timeout.
        # Without this, kill()'ing the client leaves ghost queries running
        # in ExeMgr, blocking subsequent connections.
        try:
            self._proc.stdin.write(
                f"SET @@max_statement_time = {self.query_timeout};\n".encode()
            )
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError):
            pass

    def _stderr_reader(self):
        """Background thread that drains stderr from the persistent process."""
        proc = self._proc
        try:
            for raw in proc.stderr:
                if raw:
                    self._stderr_queue.put(raw.decode("utf-8", errors="replace"))
                else:
                    break
        except Exception:
            pass

    def _drain_stderr(self):
        """Collect all pending stderr lines."""
        lines = []
        while True:
            try:
                lines.append(self._stderr_queue.get_nowait())
            except queue.Empty:
                break
        return "".join(lines)

    def _ensure_persistent(self):
        """Start persistent connection if not running."""
        if self._proc is None or self._proc.poll() is not None:
            self._start_persistent()

    def execute(self, sql):
        """Execute SQL via persistent subprocess with marker-based completion.
        Returns (success, output, error, timed_out)."""
        self._ensure_persistent()
        proc = self._proc

        marker = f"__FUZZ_{os.getpid()}_{time.monotonic_ns()}__"
        input_sql = sql.rstrip().rstrip(";")
        full_input = f"{input_sql};\nSELECT '{marker}';\n"

        # Clear pending stderr from previous queries
        self._drain_stderr()

        try:
            proc.stdin.write(full_input.encode())
            proc.stdin.flush()
        except (BrokenPipeError, OSError):
            self._proc = None
            return (False, "", "connection lost (broken pipe)", False)

        # Read stdout in a background thread so we can enforce a timeout
        # without relying on select() (which doesn't see Python-buffered data).
        result_q = queue.Queue()

        def _reader():
            lines = []
            try:
                for raw in proc.stdout:
                    line = raw.decode("utf-8", errors="replace")
                    if marker in line:
                        result_q.put(("ok", lines))
                        return
                    lines.append(line)
                    if len(lines) > 200:
                        lines = lines[-100:]
            except Exception:
                pass
            result_q.put(("eof", lines))

        reader_t = threading.Thread(target=_reader, daemon=True)
        reader_t.start()
        reader_t.join(timeout=self.query_timeout + 2)

        # Drain result_q (small timeout avoids TOCTOU race with _reader).
        try:
            status, stdout_lines = result_q.get(timeout=0.05)
        except queue.Empty:
            status, stdout_lines = "timeout", []

        if status == "ok":
            stderr = self._drain_stderr()[:2000]
            stdout = "".join(stdout_lines)[:2000]
            has_error = bool(stderr and "ERROR" in stderr)
            return (not has_error, stdout, stderr, False)

        # Timeout or process died — kill and force-restart on next call.
        timed_out = proc.poll() is None
        if timed_out:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass
        stderr = self._drain_stderr()[:2000]
        stdout = "".join(stdout_lines)[:2000]
        self._proc = None
        if timed_out:
            return (False, stdout, f"TIMEOUT after {self.query_timeout}s", True)
        return (False, stdout, stderr or "connection lost", False)

    def is_alive(self):
        """Check if mysqld is still responding (fresh one-shot connection)."""
        cmd = self._build_cmd()
        try:
            result = subprocess.run(
                cmd,
                input="SELECT 1;\n",
                capture_output=True, text=True, timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    def looks_like_crash(self, error):
        """Return True if the error suggests a connection/server failure."""
        return bool(self._CONNECTION_ERRORS.search(error))

    def restart_server(self):
        """Force restart mariadb via systemctl (uses sudo). Returns True if successful."""
        attempts = []
        if shutil.which("mcsadmin"):
            attempts.append(("mcsadmin restartSystem", ["mcsadmin", "restartSystem"], 90))
        attempts.append(("sudo -n systemctl restart mariadb", ["sudo", "-n", "systemctl", "restart", "mariadb"], 60))

        for label, cmd, timeout in attempts:
            log.warning(f"Attempting server restart via {label}...")
            try:
                result = subprocess.run(cmd, timeout=timeout, capture_output=True, text=True)
            except Exception as e:
                log.warning(f"{label} failed: {e}")
                continue
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip()
                log.warning(f"{label} failed: {detail[:200]}")
                continue
            for i in range(45):
                time.sleep(1)
                if self.is_alive():
                    log.info(f"Server restarted successfully after {i+1}s via {label}")
                    return True
        log.error("Server did not come up after recovery attempts")
        return False

    def _detect_fuzz_engine(self):
        ok, out, _, _ = self.execute("SELECT ENGINE FROM information_schema.ENGINES WHERE ENGINE='Columnstore'")
        engine = "Columnstore" if ok and "Columnstore" in out else "InnoDB"
        log.info(f"Using engine: {engine}")
        return engine

    def _reuse_existing_fuzz_db(self):
        ok, out, _, _ = self.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema='fuzz_db' AND table_name IN "
            "('t1','t2','datatypetestm','orders','cs1')")
        if ok and "5" in out:
            log.info("fuzz_db already populated, reusing existing tables")
            self.database = "fuzz_db"
            self.execute("USE fuzz_db")
            return True
        return False

    def _initialize_fuzz_db(self):
        setup_ok = True
        ok_drop, _, err_drop, _ = self.execute("DROP DATABASE IF EXISTS fuzz_db")
        ok_create, _, err_create, _ = self.execute("CREATE DATABASE fuzz_db")
        if not ok_drop and "Unknown database" not in str(err_drop):
            setup_ok = False
            log.warning(f"Setup stmt failed: {str(err_drop)[:200]}")
        if not ok_create:
            setup_ok = False
            log.warning(f"Setup stmt failed: {str(err_create)[:200]}")
        self.database = "fuzz_db"
        # Persistent connection must switch context explicitly
        self.execute("USE fuzz_db")
        return setup_ok

    def _run_setup_statements(self, stmt_groups):
        setup_ok = True
        for stmts in stmt_groups:
            for stmt in stmts:
                stmt = stmt.strip()
                if not stmt:
                    continue
                ok_s, _, err_s, _ = self.execute(stmt)
                if not ok_s and "already exists" not in str(err_s):
                    setup_ok = False
                    log.warning(f"Setup stmt failed: {str(err_s)[:200]}")
        return setup_ok

    def _fuzz_db_statement_groups(self, engine):
        return [
            [
                f"""CREATE TABLE t1 (
                  c_tinyint TINYINT, c_smallint SMALLINT, c_int INT, c_bigint BIGINT,
                  c_decimal DECIMAL(18,2), c_float FLOAT, c_double DOUBLE,
                  c_char CHAR(50), c_varchar VARCHAR(255), c_text TEXT,
                  c_date DATE, c_datetime DATETIME, c_timestamp TIMESTAMP NULL
                ) ENGINE={engine}""",
                f"""INSERT INTO t1 SELECT
                  CAST(seq AS SIGNED) % 255 - 127,
                  CAST(seq AS SIGNED) % 60000 - 30000,
                  CAST(seq AS SIGNED) * 7 - 35000,
                  CAST(seq AS SIGNED) * 100000 - 500000000,
                  CAST(seq AS SIGNED) * 3.14 - 15000,
                  seq * 0.001, seq * 1.23456,
                  CONCAT('str_', seq % 100), CONCAT('varchar_val_', seq),
                  CONCAT('text_data_', REPEAT('x', seq % 50)),
                  DATE_ADD('2020-01-01', INTERVAL (seq % 3000) DAY),
                  DATE_ADD('2020-01-01 08:00:00', INTERVAL seq * 37 SECOND),
                  DATE_ADD('2020-06-15 12:00:00', INTERVAL seq * 13 SECOND)
                FROM seq_1_to_5000""",
                """INSERT INTO t1 VALUES
                  (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
                  (-128,-32768,-2147483648,-9223372036854775807,-99999.99,-1.5,-2.5,
                   '','','','1000-01-01','1000-01-01 00:00:00','1970-01-02 00:00:01'),
                  (127,32767,2147483647,9223372036854775807,99999.99,3.4e38,1.7e100,
                   REPEAT('x',50),REPEAT('y',255),REPEAT('z',1000),
                   '9999-12-31','9999-12-31 23:59:59','2038-01-19 03:14:07')""",
            ],
            [
                f"""CREATE TABLE t2 (c_int INT, c_bigint BIGINT, c_decimal DECIMAL(18,2),
                  c_varchar VARCHAR(255), c_date DATE) ENGINE={engine}""",
                "INSERT INTO t2 SELECT seq, seq*1000, seq*1.5, CONCAT('row_',seq), DATE_ADD('2020-01-01', INTERVAL (seq%3000) DAY) FROM seq_1_to_5000",
            ],
            [
                "CREATE TABLE t3 (id INT, val VARCHAR(100)) ENGINE=InnoDB",
                "INSERT INTO t3 SELECT seq, CONCAT('innodb_row_',seq) FROM seq_1_to_1000",
                f"CREATE TABLE t4 (c_int INT, c_char CHAR(20), c_decimal DECIMAL(10,2)) ENGINE={engine}",
                "INSERT INTO t4 SELECT seq, CONCAT('c4_',seq), seq*2.5 FROM seq_1_to_100",
            ],
            [
                f"""CREATE TABLE datatypetestm (
                  cidx INT, CTINYINT TINYINT, CSMALLINT SMALLINT, CINTEGER INT, CBIGINT BIGINT,
                  CDECIMAL1 DECIMAL(18,2), CDECIMAL5 DECIMAL(5,2), CDECIMAL9 DECIMAL(9,4),
                  CDECIMAL18 DECIMAL(18,8), CDECIMAL18_2 DECIMAL(18,2),
                  CFLOAT FLOAT, CDOUBLE DOUBLE,
                  CCHAR3 CHAR(3), CCHAR5 CHAR(5), CCHAR6 CHAR(6), CCHAR7 CHAR(7),
                  CCHAR8 CHAR(8), CCHAR9 CHAR(9), CCHAR255 CHAR(255),
                  CVCHAR6 VARCHAR(6), CVCHAR255 VARCHAR(255), CTEXT TEXT,
                  CDATE DATE, CDATETIME DATETIME
                ) ENGINE={engine}""",
                """INSERT INTO datatypetestm SELECT seq,
                  CAST(seq AS SIGNED) % 255 - 127,
                  CAST(seq AS SIGNED) % 60000 - 30000,
                  CAST(seq AS SIGNED) * 13 - 65000,
                  CAST(seq AS SIGNED) * 7919 - 3959500,
                  seq*1.23, (seq%999)*0.01, (seq%9999)*0.0001,
                  seq*0.12345678, seq*2.34, seq*0.5, seq*1.7,
                  SUBSTR(CONCAT('ABC',seq),1,3), SUBSTR(CONCAT('ABCDE',seq),1,5),
                  SUBSTR(CONCAT('FGHIJK',seq),1,6), SUBSTR(CONCAT('LMNOPQR',seq),1,7),
                  SUBSTR(CONCAT('STUVWXYZ',seq),1,8), SUBSTR(CONCAT('123456789',seq),1,9),
                  CONCAT('lc_', REPEAT(CHAR(65+(seq%26)), seq%50)),
                  SUBSTR(CONCAT('vc_',seq),1,6), CONCAT('varchar255_val_',seq),
                  CONCAT('text_', REPEAT('t',seq%100)),
                  DATE_ADD('2010-01-01', INTERVAL (seq%3000) DAY),
                  DATE_ADD('2010-01-01', INTERVAL seq*3600 SECOND)
                FROM seq_1_to_5000""",
            ],
            [
                f"CREATE TABLE region (r_regionkey INT, r_name CHAR(25), r_comment VARCHAR(152)) ENGINE={engine}",
                "INSERT INTO region VALUES (0,'AFRICA','...'),(1,'AMERICA','...'),(2,'ASIA','...'),(3,'EUROPE','...'),(4,'MIDDLE EAST','...')",
                f"CREATE TABLE nation (n_nationkey INT, n_name CHAR(25), n_regionkey INT, n_comment VARCHAR(152)) ENGINE={engine}",
                "INSERT INTO nation SELECT seq-1, CONCAT('NATION_',seq-1), (seq-1)%5, 'comment' FROM seq_1_to_25",
            ],
            [
                f"""CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHAR(1),
                  o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority CHAR(15),
                  o_clerk CHAR(15), o_shippriority INT, o_comment VARCHAR(79)) ENGINE={engine}""",
                """INSERT INTO orders SELECT seq, (seq%5000)+1,
                  ELT(1+(seq%3),'F','O','P'), seq*12.34,
                  DATE_ADD('1992-01-01', INTERVAL (seq%2500) DAY),
                  ELT(1+(seq%5),'1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'),
                  CONCAT('Clerk#',LPAD(1+(seq%1000),9,'0')), 0, CONCAT('order_comment_',seq)
                FROM seq_1_to_5000""",
                f"""CREATE TABLE customer (c_custkey INT, c_name VARCHAR(25), c_address VARCHAR(40),
                  c_nationkey INT, c_phone CHAR(15), c_acctbal DECIMAL(15,2),
                  c_mktsegment CHAR(10), c_comment VARCHAR(117)) ENGINE={engine}""",
                """INSERT INTO customer SELECT seq, CONCAT('Customer#',LPAD(seq,9,'0')),
                  CONCAT('addr_',seq), seq%25,
                  CONCAT(10+(seq%80),'-',LPAD(seq%10000000,7,'0')),
                  CAST(seq AS SIGNED)*7.77-5000,
                  ELT(1+(seq%5),'AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'),
                  CONCAT('comment_',seq)
                FROM seq_1_to_5000""",
            ],
            [
                f"""CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT,
                  l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2),
                  l_discount DECIMAL(15,2), l_tax DECIMAL(15,2),
                  l_returnflag CHAR(1), l_linestatus CHAR(1),
                  l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
                  l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment VARCHAR(44)) ENGINE={engine}""",
                """INSERT INTO lineitem SELECT (seq%50000)+1, (seq%2000)+1, (seq%100)+1,
                  (seq%7)+1, 1+(seq%50), seq*1.23, (seq%10)*0.01, (seq%8)*0.01,
                  ELT(1+(seq%3),'A','N','R'), ELT(1+(seq%2),'F','O'),
                  DATE_ADD('1992-01-01', INTERVAL (seq%2500) DAY),
                  DATE_ADD('1992-02-01', INTERVAL (seq%2500) DAY),
                  DATE_ADD('1992-03-01', INTERVAL (seq%2500) DAY),
                  ELT(1+(seq%4),'DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'),
                  ELT(1+(seq%7),'TRUCK','MAIL','REG AIR','AIR','RAIL','SHIP','FOB'),
                  CONCAT('lineitem_',seq)
                FROM seq_1_to_10000""",
            ],
            [
                f"CREATE TABLE cs1 (a INT, b CHAR(10), c CHAR(10), d CHAR(10)) ENGINE={engine}",
                "INSERT INTO cs1 SELECT seq, CONCAT('b',seq), CONCAT('c',seq), CONCAT('d',seq) FROM seq_1_to_1000",
                f"CREATE TABLE cs2 (a INT, b INT, c INT) ENGINE={engine}",
                "INSERT INTO cs2 SELECT seq, seq*2, seq*3 FROM seq_1_to_1000",
                f"CREATE TABLE cs3 (a INT, b INT) ENGINE={engine}",
                "INSERT INTO cs3 SELECT seq, seq*10 FROM seq_1_to_500",
                f"CREATE TABLE cs4 (d1 DECIMAL(18,2), d2 DECIMAL(18,2)) ENGINE={engine}",
                "INSERT INTO cs4 SELECT seq*1.5, seq*2.5 FROM seq_1_to_500",
                f"CREATE TABLE cs5 (a INT, b VARCHAR(50)) ENGINE={engine}",
                "INSERT INTO cs5 SELECT seq, CONCAT('cs5_',seq) FROM seq_1_to_500",
                f"CREATE TABLE cs6 (d1 DECIMAL(10,2), d2 DECIMAL(10,2)) ENGINE={engine}",
                "INSERT INTO cs6 SELECT seq*0.7, seq*3.14 FROM seq_1_to_500",
                f"CREATE TABLE utest1 (ukey INT, c1 INT, c2 VARCHAR(50), c3 BIGINT) ENGINE={engine}",
                "INSERT INTO utest1 SELECT seq, seq*10, CONCAT('u_',seq), seq*100 FROM seq_1_to_200",
                f"CREATE TABLE utest3 (a INT, b INT) ENGINE={engine}",
                "INSERT INTO utest3 SELECT seq, seq*5 FROM seq_1_to_200",
                f"CREATE TABLE test_cs (a INT, b VARCHAR(50)) ENGINE={engine}",
                "INSERT INTO test_cs SELECT seq, CONCAT('cs_',seq) FROM seq_1_to_500",
                "CREATE TABLE test_innodb (a INT, b VARCHAR(50)) ENGINE=InnoDB",
                "INSERT INTO test_innodb SELECT seq, CONCAT('inn_',seq) FROM seq_1_to_500",
                f"CREATE TABLE test_mult (indemnity_paid DECIMAL(10,2), n_clms INT) ENGINE={engine}",
                "INSERT INTO test_mult VALUES (-10.00,1),(100.50,5),(0.00,0),(-500.25,10),(999.99,100)",
                f"CREATE TABLE bug5096 (id INT, c1 INT) ENGINE={engine}",
                "INSERT INTO bug5096 SELECT seq, seq%5 FROM seq_1_to_100",
                f"CREATE TABLE mcol979 (b INT, h INT) ENGINE={engine}",
                "INSERT INTO mcol979 SELECT seq, seq*3 FROM seq_1_to_100",
                f"CREATE TABLE t (x VARCHAR(100)) ENGINE={engine}",
                "INSERT INTO t SELECT CONCAT('val_',seq) FROM seq_1_to_50",
                f"CREATE TABLE zu (hu INT) ENGINE={engine}",
                "INSERT INTO zu SELECT seq FROM seq_1_to_100",
                f"CREATE TABLE three_cols (a INT, b INT, c INT) ENGINE={engine}",
                "INSERT INTO three_cols SELECT seq, seq*2, seq*3 FROM seq_1_to_500",
                f"CREATE TABLE emp (id INT, name VARCHAR(50), dept INT, salary DECIMAL(10,2)) ENGINE={engine}",
                "INSERT INTO emp SELECT seq, CONCAT('emp_',seq), seq%10, 30000+seq*100 FROM seq_1_to_200",
            ],
        ]

    def setup_fuzz_db(self):
        """Create fuzzing database and base tables with substantial data."""
        engine = self._detect_fuzz_engine()
        if self._reuse_existing_fuzz_db():
            return True

        log.info("Creating fuzz_db with full dataset (this takes ~60s)...")
        old_timeout = self.query_timeout
        self.query_timeout = 120  # Setup needs time for big INSERTs
        try:
            setup_ok = self._initialize_fuzz_db()
            if not self._run_setup_statements(self._fuzz_db_statement_groups(engine)):
                setup_ok = False
        finally:
            self.query_timeout = old_timeout

        if setup_ok:
            log.info("fuzz_db setup complete")
        else:
            log.warning("fuzz_db setup completed with failures")
        return setup_ok


# ---------------------------------------------------------------------------
# Crash reporting
# ---------------------------------------------------------------------------

class FuzzReport:
    """Collects and deduplicates crash reports."""

    def __init__(self, output_dir, max_artifacts_per_pattern=5):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.crashes = []
        self.hangs = []
        self.errors_of_interest = []
        self.seen_hashes = set()
        self._coarse_seen = {}  # coarse_hash -> hit count
        self._max_per_pattern = max_artifacts_per_pattern
        self.total_queries = 0
        self.total_errors = 0

    def _normalize_error(self, error_msg):
        normalized = re.sub(r"0x[0-9a-fA-F]+", "ADDR", error_msg)
        normalized = re.sub(r":\d+", ":N", normalized)
        normalized = re.sub(r"\d+", "N", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _normalize_sql(self, sql):
        normalized = _LITERAL_RE.sub("?", sql.upper())
        normalized = re.sub(r"\b(JT|CTE|FUZZ_STMT|FUZZ_COL|NESTED)_\d+\b", r"\1_N", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _extract_error_key(self, error_msg):
        match = re.search(r"\b(MCS-\d+|CAL\d+|ERROR \d+)\b", error_msg, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        first_line = re.sub(r"\s+", " ", error_msg.strip()).split("\n", 1)[0]
        return first_line[:120]

    def _hash_issue(self, issue_type, error_msg, sql):
        payload = "\n".join([
            issue_type,
            self._normalize_error(error_msg)[:300],
            self._normalize_sql(sql)[:220],
        ])
        return hashlib.md5(payload.encode()).hexdigest()[:12]

    def _coarse_hash(self, issue_type, error_msg, context=None):
        """Coarse dedup key: same error pattern regardless of exact SQL.

        Groups by issue_type + error_key only — intentionally ignores
        which mutations or seed SQL produced the finding so that many
        roads to the same MCS-XXXX error collapse into one bucket.
        """
        error_key = self._extract_error_key(error_msg)
        payload = f"{issue_type}|{error_key}"
        return hashlib.md5(payload.encode()).hexdigest()[:12]

    def _build_entry(self, issue_type, sql, error, context=None):
        context = context or {}
        return {
            "type": issue_type,
            "sql": sql,
            "error": error,
            "time": datetime.now().isoformat(),
            "hash": self._hash_issue(issue_type, error, sql),
            "error_key": self._extract_error_key(error),
            "sql_shape": self._normalize_sql(sql)[:300],
            "seed_sql": context.get("seed_sql"),
            "mutations": context.get("mutations", []),
        }

    def _record_entry(self, entry, entries_list, label, context, log_fn):
        """Record a finding with exact + coarse dedup."""
        if entry["hash"] in self.seen_hashes:
            return
        self.seen_hashes.add(entry["hash"])

        ch = self._coarse_hash(entry["type"], entry["error"], context)
        count = self._coarse_seen.get(ch, 0) + 1
        self._coarse_seen[ch] = count

        if count <= self._max_per_pattern:
            entries_list.append(entry)
            self._save_crash(entry)
            log_fn(f"{label} #{len(entries_list)}: {entry['sql'][:100]}...")
        elif count == self._max_per_pattern + 1:
            entries_list.append(entry)
            self._save_crash(entry)
            log.info(f"Suppressing further artifacts for pattern "
                     f"{entry['error_key']} (>{self._max_per_pattern} variants, "
                     f"coarse={ch})")
        # Beyond max_per_pattern+1: counted in seen_hashes and coarse_seen
        # but not appended to in-memory list to avoid unbounded growth.

    def record(self, sql, success, output, error, timed_out, server_alive, context=None):
        self.total_queries += 1
        if success:
            return

        self.total_errors += 1

        # Server crash — most critical
        if not server_alive:
            entry = self._build_entry("CRASH", sql, error, context)
            self._record_entry(entry, self.crashes, "SERVER CRASH", context, log.error)
            return

        # Timeout / hang
        if timed_out:
            entry = self._build_entry("HANG", sql, error, context)
            self._record_entry(entry, self.hangs, "HANG", context, log.warning)
            return

        # Skip known CS limitations — not bugs, skip these
        if _KNOWN_LIMITATION_RE.search(error):
            return

        # Assertion failure, signal, segfault in error message
        if any(kw in error.lower() for kw in [
            "assertion", "signal 11", "signal 6", "segfault",
            "stack trace", "asan", "internal error",
            "mariadbd got signal",
        ]):
            entry = self._build_entry("ASSERTION/SIGNAL", sql, error, context)
            self._record_entry(entry, self.errors_of_interest, "INTERESTING ERROR",
                               context, log.warning)

    def _save_crash(self, entry):
        """Save individual crash to file."""
        prefix = entry["type"].lower().replace("/", "_")
        fname = f"{prefix}_{entry['hash']}.sql"
        fpath = self.output_dir / fname
        with open(fpath, "w") as f:
            f.write(f"-- Type: {entry['type']}\n")
            f.write(f"-- Time: {entry['time']}\n")
            f.write(f"-- Hash: {entry['hash']}\n")
            f.write(f"-- ErrorKey: {entry['error_key']}\n")
            f.write(f"-- SQLShape: {entry['sql_shape']}\n")
            if entry.get("mutations"):
                f.write(f"-- Mutations: {', '.join(entry['mutations'])}\n")
            if entry.get("seed_sql"):
                f.write(f"-- SeedSQL: {entry['seed_sql'].replace(chr(10), ' ')[:500]}\n")
            f.write(f"-- Error: {entry['error'][:500]}\n")
            f.write(f"--\n")
            f.write(f"USE fuzz_db;\n")
            f.write(f"{entry['sql']};\n")

    def collect_crash_artifacts(self, crash_hash):
        """Collect server error log, core dumps, dmesg after a crash."""
        artifacts_dir = self.output_dir / "artifacts" / crash_hash
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        def _capture(cmd, timeout):
            try:
                return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            except Exception:
                return None

        def _tail_log(log_path, lines):
            for cmd in (["tail", f"-{lines}", log_path], ["sudo", "-n", "tail", f"-{lines}", log_path]):
                result = _capture(cmd, 5)
                if result and result.returncode == 0 and result.stdout is not None:
                    return result.stdout
            return None

        # MariaDB error log tail
        for log_path in ["/var/log/mariadb/mariadb.log",
                         "/var/log/mysql/error.log",
                         "/var/log/syslog"]:
            if os.path.exists(log_path):
                log_data = _tail_log(log_path, 200)
                if log_data is not None:
                    out_name = os.path.basename(log_path) + ".tail"
                    (artifacts_dir / out_name).write_text(log_data)
        # Columnstore debug log
        for cs_log in ["/var/log/mariadb/columnstore/debug.log",
                       "/var/log/mariadb/columnstore/info.log",
                       "/var/log/mariadb/columnstore/warning.log",
                       "/var/log/mariadb/columnstore/err.log"]:
            if os.path.exists(cs_log):
                log_data = _tail_log(cs_log, 100)
                if log_data is not None:
                    out_name = os.path.basename(cs_log) + ".tail"
                    (artifacts_dir / out_name).write_text(log_data)
        # Core dumps — search known directories only (no full-FS scan)
        core_dirs = ["/core", "/var/lib/mysql", "/tmp"]
        try:
            cp_result = _capture(["cat", "/proc/sys/kernel/core_pattern"], 2)
            if cp_result and cp_result.returncode == 0:
                pdir = os.path.dirname(cp_result.stdout.strip())
                if pdir and pdir not in core_dirs:
                    core_dirs.insert(0, pdir)
        except Exception:
            pass
        core_found = []
        for cdir in core_dirs:
            if not os.path.isdir(cdir):
                continue
            result = _capture(
                ["find", cdir, "-maxdepth", "2", "-name", "core*",
                 "-mmin", "-5", "-ls"],
                5,
            )
            if result and result.returncode == 0 and result.stdout.strip():
                core_found.append(result.stdout)
        if core_found:
            (artifacts_dir / "core_files.txt").write_text("\n".join(core_found))
            log.warning(f"Core files found! See {artifacts_dir}/core_files.txt")
        # dmesg (OOM killer, segfaults)
        try:
            result = subprocess.run(
                ["sudo", "-n", "dmesg", "--time-format=iso", "-T"],
                capture_output=True, text=True, timeout=5,
            )
            # Only last 50 lines
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")[-50:]
                (artifacts_dir / "dmesg.tail").write_text("\n".join(lines))
        except Exception:
            pass

    def save_checkpoint(self):
        """Save periodic checkpoint with stats as JSON."""
        checkpoint = {
            "timestamp": datetime.now().isoformat(),
            "total_queries": self.total_queries,
            "total_errors": self.total_errors,
            "crashes": len(self.crashes),
            "hangs": len(self.hangs),
            "assertions": len(self.errors_of_interest),
            "unique_issues": len(self.seen_hashes),
            "crash_hashes": [c["hash"] for c in self.crashes],
            "hang_hashes": [h["hash"] for h in self.hangs],
            "assertion_hashes": [e["hash"] for e in self.errors_of_interest],
        }
        cp_path = self.output_dir / "checkpoint.json"
        with open(cp_path, "w") as f:
            json.dump(checkpoint, f, indent=2)
        return cp_path

    def write_summary(self):
        """Write final summary report."""
        summary_path = self.output_dir / "summary.txt"
        with open(summary_path, "w") as f:
            f.write("=" * 70 + "\n")
            f.write("SQL FUZZER REPORT\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Total queries executed: {self.total_queries}\n")
            f.write(f"Total errors:           {self.total_errors}\n")
            f.write(f"Server crashes:         {len(self.crashes)}\n")
            f.write(f"Hangs (timeout):        {len(self.hangs)}\n")
            f.write(f"Assertion/signal:       {len(self.errors_of_interest)}\n")
            f.write(f"Unique issues:          {len(self.seen_hashes)}\n")
            suppressed = sum(1 for v in self._coarse_seen.values() if v > self._max_per_pattern)
            f.write(f"Coarse patterns:        {len(self._coarse_seen)}\n")
            f.write(f"Patterns suppressed:    {suppressed} (>{self._max_per_pattern} variants)\n\n")

            if self.crashes:
                f.write("-" * 70 + "\n")
                f.write("CRASHES (server died)\n")
                f.write("-" * 70 + "\n")
                for c in self.crashes:
                    f.write(f"\n[{c['hash']}] {c['time']}\n")
                    f.write(f"Key: {c['error_key']}\n")
                    f.write(f"SQL: {c['sql'][:200]}\n")
                    if c.get("mutations"):
                        f.write(f"Mutations: {', '.join(c['mutations'])[:200]}\n")
                    f.write(f"Error: {c['error'][:300]}\n")

            if self.hangs:
                f.write("\n" + "-" * 70 + "\n")
                f.write("HANGS (query timeout)\n")
                f.write("-" * 70 + "\n")
                for h in self.hangs:
                    f.write(f"\n[{h['hash']}] {h['time']}\n")
                    f.write(f"Key: {h['error_key']}\n")
                    f.write(f"SQL: {h['sql'][:200]}\n")
                    if h.get("mutations"):
                        f.write(f"Mutations: {', '.join(h['mutations'])[:200]}\n")

            if self.errors_of_interest:
                f.write("\n" + "-" * 70 + "\n")
                f.write("ASSERTION / SIGNAL ERRORS\n")
                f.write("-" * 70 + "\n")
                for e in self.errors_of_interest:
                    f.write(f"\n[{e['hash']}] {e['time']}\n")
                    f.write(f"Key: {e['error_key']}\n")
                    f.write(f"SQL: {e['sql'][:200]}\n")
                    if e.get("mutations"):
                        f.write(f"Mutations: {', '.join(e['mutations'])[:200]}\n")
                    f.write(f"Error: {e['error'][:300]}\n")

        log.info(f"Summary written to {summary_path}")
        return summary_path


# ---------------------------------------------------------------------------
# Main fuzzing loop
# ---------------------------------------------------------------------------

def fuzz_loop(conn, seed_sql, report, duration_sec, max_queries=None, query_timeout=10):
    """Main fuzzing loop with overnight resilience."""
    start = time.time()
    iteration = 0
    crash_count = 0
    consecutive_failures = 0
    variants_per_seed = 3
    args_timeout = query_timeout

    # Always set to fuzz_db
    conn.execute("USE fuzz_db")

    # Shuffle corpus and iterate in order — guarantees full coverage per pass
    corpus = list(seed_sql)
    random.shuffle(corpus)
    corpus_idx = 0
    pass_number = 1

    log.info(f"Corpus: {len(corpus)} seeds, {variants_per_seed} variants/seed, "
             f"duration: {duration_sec}s ({duration_sec/3600:.1f}h)")

    while True:
        elapsed = time.time() - start
        if elapsed >= duration_sec:
            break
        if max_queries and iteration >= max_queries:
            break

        # Pick next seed SQL; reshuffle when exhausted
        original = corpus[corpus_idx // variants_per_seed]
        corpus_idx += 1
        if corpus_idx >= len(corpus) * variants_per_seed:
            log.info(f"Corpus pass #{pass_number} complete "
                     f"({len(corpus)} seeds x{variants_per_seed} variants), reshuffling...")
            random.shuffle(corpus)
            corpus_idx = 0
            pass_number += 1

        mutated, mutation_names = mutate_sql(original, return_metadata=True)

        # Protect base tables from destructive DDL
        if _is_destructive_to_protected(mutated):
            continue
        # Skip known hang patterns
        if _is_blacklisted(mutated):
            continue

        iteration += 1

        # Use higher timeout for DDL (slow on CS), lower for queries
        if _is_slow_ddl(mutated):
            conn.query_timeout = 30
        else:
            conn.query_timeout = args_timeout

        # Execute
        ok, output, error, timed_out = conn.execute(mutated)
        server_alive = True

        if timed_out:
            server_alive = conn.is_alive()

        # DDL timeouts are expected, don't record as hangs
        if timed_out and _is_slow_ddl(mutated) and server_alive:
            continue

        # Check server liveness only on connection-related errors (not every error)
        if not ok and not timed_out and conn.looks_like_crash(error):
            server_alive = conn.is_alive()

        report.record(
            mutated,
            ok,
            output,
            error,
            timed_out,
            server_alive,
            context={"seed_sql": original, "mutations": mutation_names},
        )

        if not server_alive:
            crash_count += 1
            crash_hash = report._hash_issue("CRASH", error, mutated)
            log.error(f"Server crash #{crash_count}! Collecting artifacts...")

            # Collect crash artifacts (logs, cores, dmesg)
            report.collect_crash_artifacts(crash_hash)

            # Wait for auto-restart first
            recovered = False
            for attempt in range(15):
                time.sleep(1)
                if conn.is_alive():
                    log.info(f"Server auto-recovered after {attempt+1}s")
                    recovered = True
                    break

            # If auto-restart didn't work, force restart
            if not recovered:
                log.warning("Auto-restart failed, forcing systemctl restart...")
                recovered = conn.restart_server()

            if recovered:
                conn.execute("USE fuzz_db")
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    log.error("Server failed to restart 3 times in a row, stopping.")
                    break
                log.warning(f"Restart failed ({consecutive_failures}/3), retrying in 30s...")
                time.sleep(30)
                if conn.restart_server():
                    conn.execute("USE fuzz_db")
                    consecutive_failures = 0
                else:
                    continue
        else:
            if ok:
                consecutive_failures = 0

        # Periodically recreate tables in case mutations broke them
        if iteration % 2000 == 0 and iteration > 0:
            log.info("Recreating fuzz_db tables...")
            conn.setup_fuzz_db()
            conn.execute("USE fuzz_db")

        # Checkpoint every 5000 queries
        if iteration % 5000 == 0 and iteration > 0:
            report.save_checkpoint()

        # Progress
        if iteration % 500 == 0:
            rate = iteration / (time.time() - start)
            log.info(
                f"Progress: {iteration} queries, {rate:.0f} q/s, "
                f"{len(report.crashes)} crashes, {len(report.hangs)} hangs, "
                f"{len(report.errors_of_interest)} assertions, "
                f"{elapsed:.0f}s/{duration_sec}s "
                f"(pass #{pass_number}, {elapsed/duration_sec*100:.0f}%)"
            )

    # Final checkpoint
    report.save_checkpoint()

    log.info(
        f"Done: {iteration} queries in {time.time() - start:.0f}s, "
        f"{pass_number} corpus passes, "
        f"{len(report.crashes)} crashes, {len(report.hangs)} hangs, "
        f"{len(report.errors_of_interest)} assertions"
    )


def main():
    parser = argparse.ArgumentParser(
        description="SQL Mutation Fuzzer for MariaDB Columnstore"
    )
    parser.add_argument(
        "--duration", type=int, default=60,
        help="Fuzzing duration in seconds (default: 60)"
    )
    parser.add_argument(
        "--max-queries", type=int, default=None,
        help="Maximum number of queries to execute"
    )
    parser.add_argument(
        "--socket", type=str, default=None,
        help="MySQL socket path (e.g. /var/lib/mysql/mysql.sock)"
    )
    parser.add_argument(
        "--host", type=str, default="127.0.0.1",
        help="MySQL host"
    )
    parser.add_argument(
        "--port", type=int, default=3306,
        help="MySQL port"
    )
    parser.add_argument(
        "--user", type=str, default="root",
        help="MySQL user"
    )
    parser.add_argument(
        "--password", type=str, default=None,
        help="MySQL password"
    )
    parser.add_argument(
        "--output", type=str,
        default=os.path.join(os.path.dirname(__file__), "fuzz_results", "sql_fuzzer"),
        help="Output directory for crash reports"
    )
    parser.add_argument(
        "--test-dirs", type=str, nargs="+",
        default=None,
        help="Directories containing .test files (default: auto-detect)"
    )
    parser.add_argument(
        "--query-timeout", type=int, default=10,
        help="Per-query timeout in seconds (default: 10)"
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility"
    )
    parser.add_argument(
        "--max-artifacts-per-pattern", type=int, default=5,
        help="Max artifact files per coarse error pattern (default: 5)"
    )

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # Add file logging for overnight runs
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.output
    if args.duration >= 3600:
        out_dir = os.path.join(os.path.dirname(args.output), f"sql_fuzzer_{run_ts}")
    os.makedirs(out_dir, exist_ok=True)
    args.output = out_dir
    fh = logging.FileHandler(os.path.join(out_dir, "fuzzer.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    log.addHandler(fh)

    # Auto-detect test directories
    if args.test_dirs is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        cs_root = os.path.dirname(script_dir)
        mtr_base = os.path.join(cs_root, "mysql-test", "columnstore")
        args.test_dirs = []
        for sub in ["basic/t", "bugfixes", "autopilot/t", "devregression/t"]:
            d = os.path.join(mtr_base, sub)
            if os.path.isdir(d):
                args.test_dirs.append(d)
        if not args.test_dirs:
            log.error(f"No test directories found under {mtr_base}")
            sys.exit(1)

    log.info(f"Test directories: {args.test_dirs}")

    # Load seed corpus
    seed_sql = load_seed_corpus(args.test_dirs)
    if not seed_sql:
        log.error("No SQL statements extracted from test files")
        sys.exit(1)

    # Connect
    conn = ServerConnection(
        socket_path=args.socket,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )
    conn.query_timeout = args.query_timeout

    # Check connection
    if not conn.is_alive():
        log.error("Cannot connect to MySQL server")
        sys.exit(1)
    log.info("Connected to MySQL server")

    # Setup
    log.info("Setting up fuzz database with Columnstore tables...")
    conn.setup_fuzz_db()

    # Fuzz
    report = FuzzReport(args.output, max_artifacts_per_pattern=args.max_artifacts_per_pattern)
    log.info(f"Starting fuzzing for {args.duration}s (max {args.max_queries or 'unlimited'} queries)...")
    log.info(f"Output directory: {args.output}")

    try:
        fuzz_loop(conn, seed_sql, report, args.duration, args.max_queries, args.query_timeout)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        conn.close()

    # Report
    summary = report.write_summary()
    print(f"\n{'=' * 60}")
    print(f"Fuzzing complete.")
    print(f"  Queries:    {report.total_queries}")
    print(f"  Crashes:    {len(report.crashes)}")
    print(f"  Hangs:      {len(report.hangs)}")
    print(f"  Assertions: {len(report.errors_of_interest)}")
    print(f"  Report:     {summary}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
