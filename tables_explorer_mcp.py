#!/usr/bin/env python3
"""
MCP (stdio) для Cursor: чтение schema_compact.json, поиск таблиц, путь по FK, SQL-шаблоны.

Регистрация: Cursor → Settings → MCP (или .cursor/mcp.json).

Ошибка spawn ... wsl.exe ENOENT: процесс MCP не видит C:\\Windows\\... Чаще всего Cursor
открыт через WSL (Remote-WSL) — тогда используйте вариант 1, без wsl.exe.

--- Вариант 1: окно Cursor подключено к WSL (путь вроде /home/.../tables-explorer) ---

{
  "mcpServers": {
    "tables-explorer": {
      "command": "/home/bekzhan/tables-explorer/.venv/bin/python",
      "args": ["/home/bekzhan/tables-explorer/tables_explorer_mcp.py"]
    }
  }
}

--- Вариант 2: Cursor только на Windows, код внутри WSL ---

{
  "mcpServers": {
    "tables-explorer": {
      "command": "wsl.exe",
      "args": [
        "-d", "Ubuntu",
        "-e", "/home/bekzhan/tables-explorer/.venv/bin/python",
        "/home/bekzhan/tables-explorer/tables_explorer_mcp.py"
      ]
    }
  }
}

Имя дистрибутива: в PowerShell `wsl -l -v` → NAME. Один дистрибутив — уберите "-d", "Ubuntu".
На чистом Windows при сбое попробуйте "command": "C:\\\\Windows\\\\System32\\\\wsl.exe"
(двойной обратный слэш в JSON).
"""

from __future__ import annotations
from sql_snippets import (generate_insert_stub, generate_select_columns,
                          generate_select_star, generate_update_stub,
                          join_hints_along_path)
from search_schema import (load_schema, resolve_single_table,
                           search_and_format, shortest_fk_path)
from mcp.server.fastmcp import FastMCP

import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


mcp = FastMCP(
    "tables-explorer",
    instructions=(
        "Доступ к локальному каталогу таблиц PostgreSQL (schema_compact.json): "
        "колонки, FK, SQL-шаблоны. Без подключения к живой БД."
    ),
    log_level="ERROR",
)

_schema_cache: dict | None = None


def _schema() -> dict:
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = load_schema()
    return _schema_cache


def _schema_filter(name: str | None) -> str | None:
    if not name or str(name).strip().lower() in ("", "all"):
        return None
    return str(name).strip()


@mcp.tool()
def get_table(table: str, schema_name: str | None = None) -> str:
    """
    Одна таблица: компактная строка схемы, fk_in/fk_out, комментарии колонок, шаблоны SELECT/INSERT/UPDATE.
    schema_name: имя схемы PostgreSQL или 'all' (по умолчанию — без фильтра = вся схема JSON).
    """
    sch = _schema_filter(schema_name)
    key, err = resolve_single_table(table, _schema(), sch)
    if err:
        return err
    item = _schema()[key]
    fk_out = item.get("fk_out") or []
    fk_in = item.get("fk_in") or []
    desc = item.get("columns_description") or {}
    parts = [
        f"# {key}",
        "",
        "## compact",
        item.get("text", ""),
        "",
        "## fk_out",
        "\n".join(f"- {x}" for x in fk_out) or "(none)",
        "",
        "## fk_in",
        "\n".join(f"- {x}" for x in fk_in) or "(none)",
        "",
        "## column_descriptions",
        json.dumps(desc, ensure_ascii=False, indent=2),
        "",
        "## SQL stubs",
        "### SELECT *",
        generate_select_star(key, item),
        "",
        "### SELECT columns",
        generate_select_columns(key, item),
        "",
        "### INSERT",
        generate_insert_stub(key, item),
        "",
        "### UPDATE",
        generate_update_stub(key, item),
    ]
    return "\n".join(parts)


@mcp.tool()
def search_tables(query: str, fuzzy: bool = False, schema_name: str | None = None) -> str:
    """
    Поиск таблиц по имени/колонкам (как CLI search_schema.py без --fk).
    fuzzy: подстроки по имени и колонкам; иначе точное совпадение имени таблицы.
    """
    sch = _schema_filter(schema_name)
    return search_and_format(
        query,
        _schema(),
        fuzzy=fuzzy,
        fk=False,
        depth=1,
        pretty=False,
        schema_filter=sch,
    )


if __name__ == "__main__":
    mcp.run()
