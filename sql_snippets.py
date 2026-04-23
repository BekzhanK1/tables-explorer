"""Генерация SQL-шаблонов и подсказок JOIN по схеме из schema_compact."""

from __future__ import annotations

import re
from typing import Any

from search_schema import parse_columns


def sql_double_quoted_ident(table_key: str) -> str:
    """PostgreSQL: "schema"."table" или "table" для public."""
    esc = lambda s: s.replace('"', '""')
    if "." in table_key:
        sch, name = table_key.split(".", 1)
        return f'"{esc(sch)}"."{esc(name)}"'
    return f'"{esc(table_key)}"'


def _parse_col_line(col_text: str, desc_map: dict[str, Any]) -> dict[str, Any]:
    m = re.match(
        r"^(?P<name>\S+)\s+(?P<ctype>[^\[]+?)(?:\s+\[(?P<flags>.+)\])?$", col_text
    )
    if not m:
        name = col_text.strip() or col_text
        return {"name": name, "type": "", "pk": False, "not_null": False}
    name = m.group("name")
    flags_raw = m.group("flags") or ""
    flags = [f.strip() for f in flags_raw.split(",")] if flags_raw else []
    desc = " ".join(str(desc_map.get(name, "")).split())
    return {
        "name": name,
        "type": (m.group("ctype") or "").strip(),
        "pk": "PK" in flags,
        "not_null": "NOT NULL" in flags,
        "desc": desc,
    }


def column_rows_from_item(item: dict[str, Any]) -> list[dict[str, Any]]:
    desc_map = item.get("columns_description", {})
    return [
        _parse_col_line(c, desc_map) for c in parse_columns(item.get("text", ""))
    ]


def generate_select_star(table_key: str, item: dict[str, Any]) -> str:
    ident = sql_double_quoted_ident(table_key)
    return f"SELECT *\nFROM {ident};"


def generate_select_columns(table_key: str, item: dict[str, Any]) -> str:
    rows = column_rows_from_item(item)
    if not rows:
        return generate_select_star(table_key, item)
    ident = sql_double_quoted_ident(table_key)
    cols = ",\n  ".join(f'"{r["name"].replace(chr(34), chr(34) + chr(34))}"' for r in rows)
    return f"SELECT\n  {cols}\nFROM {ident};"


def generate_insert_stub(table_key: str, item: dict[str, Any]) -> str:
    rows = column_rows_from_item(item)
    if not rows:
        return f"-- нет колонок в схеме для {table_key}"
    ident = sql_double_quoted_ident(table_key)
    col_idents = ", ".join(f'"{r["name"].replace(chr(34), chr(34) + chr(34))}"' for r in rows)
    values = ", ".join("NULL" for _ in rows)
    return (
        f"INSERT INTO {ident} ({col_idents})\n"
        f"VALUES ({values});\n"
        f"-- Замените NULL на значения (учитывайте NOT NULL / PK)."
    )


def generate_update_stub(table_key: str, item: dict[str, Any]) -> str:
    rows = column_rows_from_item(item)
    ident = sql_double_quoted_ident(table_key)
    if not rows:
        return f"-- нет колонок для {table_key}"
    pk_cols = [r["name"] for r in rows if r["pk"]]
    non_pk = [r for r in rows if not r["pk"]]
    if not non_pk:
        return f"-- только PK, UPDATE не сгенерирован для {table_key}"
    set_lines = ",\n  ".join(
        f'"{r["name"].replace(chr(34), chr(34) + chr(34))}" = NULL  -- TODO'
        for r in non_pk[:40]
    )
    if len(non_pk) > 40:
        set_lines += "\n  -- … остальные колонки"
    if pk_cols:
        wh = " AND ".join(
            f'"{c.replace(chr(34), chr(34) + chr(34))}" = NULL' for c in pk_cols
        )
        where_clause = f"WHERE {wh}  -- подставьте ключ"
    else:
        where_clause = "WHERE true  -- нет PK в схеме: укажите своё условие"
    return f"UPDATE {ident}\nSET\n  {set_lines}\n{where_clause};"


def fk_join_suggestions(table_a: str, table_b: str, schema: dict[str, Any]) -> list[str]:
    """Условия соединения для пары соседних таблиц на пути FK (оба направления)."""
    lines: list[str] = []
    qa = sql_double_quoted_ident(table_a)
    qb = sql_double_quoted_ident(table_b)

    item_a = schema.get(table_a)
    if item_a:
        for fk in item_a.get("fk_out", []):
            if "→" not in fk:
                continue
            col_a, ref = fk.split("→", 1)
            ref_tbl = ref.rsplit(".", 1)[0]
            if ref_tbl != table_b:
                continue
            col_b = ref.rsplit(".", 1)[1]
            lines.append(f"{qa}.{col_a} = {qb}.{col_b}")

    item_b = schema.get(table_b)
    if item_b:
        for fk in item_b.get("fk_out", []):
            if "→" not in fk:
                continue
            col_b, ref = fk.split("→", 1)
            ref_tbl = ref.rsplit(".", 1)[0]
            if ref_tbl != table_a:
                continue
            col_a = ref.rsplit(".", 1)[1]
            lines.append(f"{qa}.{col_a} = {qb}.{col_b}")

    return sorted(set(lines))


def join_hints_along_path(path: list[str], schema: dict[str, Any]) -> str:
    if len(path) < 2:
        return ""
    blocks: list[str] = []
    for i in range(len(path) - 1):
        a, b = path[i], path[i + 1]
        conds = fk_join_suggestions(a, b, schema)
        if conds:
            blocks.append(f"-- {a} ↔ {b}\n" + "\n".join(conds))
        else:
            blocks.append(f"-- {a} ↔ {b} (FK-колонки не найдены в компактной схеме)")
    return "\n\n".join(blocks)
