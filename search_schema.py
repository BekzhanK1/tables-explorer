"""
python search_schema.py "payment client"
python search_schema.py "remont" --fk
python search_schema.py "remont payment client" --fk --depth 2
python search_schema.py "orders" --schema sale --fk
"""

import argparse
import json
from pathlib import Path

SCHEMA_PATH = Path("output/schema_compact.json")


def load_schema() -> dict:
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        data = json.load(f)
    # ключ — qualified name: "table" для public, "schema.table" для остальных
    return {item["table"]: item for item in data}


def _schema_names(schema: dict) -> list[str]:
    """Список уникальных схем, отсортированных (public первая)."""
    schemas = sorted({item["schema"] for item in schema.values()})
    if "public" in schemas:
        schemas.remove("public")
        schemas.insert(0, "public")
    return schemas


def search(query: str, schema: dict, schema_filter: str | None = None) -> set:
    keywords = query.lower().split()
    found = set()
    for table_key, item in schema.items():
        if schema_filter and item.get("schema") != schema_filter:
            continue
        name = item.get("name", table_key)
        if any(kw in name.lower() for kw in keywords):
            found.add(table_key)
            continue
        if any(any(kw in col.lower() for kw in keywords) for col in item["columns"]):
            found.add(table_key)
    return found


def search_exact_table(
    query: str, schema: dict, schema_filter: str | None = None
) -> set:
    q = query.strip()
    if not q:
        return set()
    q_lower = q.lower()
    for table_key, item in schema.items():
        if schema_filter and item.get("schema") != schema_filter:
            continue
        name = item.get("name", table_key)
        if table_key == q or name == q or table_key.lower() == q_lower or name.lower() == q_lower:
            return {table_key}
    return set()


def expand_fk(tables: set, schema: dict, depth: int) -> dict:
    via_map: dict[str, set] = {t: set() for t in tables}
    current = set(tables)

    for _ in range(depth):
        to_add: dict[str, set] = {}
        for table_key in current:
            item = schema.get(table_key, {})
            for fk in item.get("fk_out", []):
                # формат: "col→[schema.]to_table.to_col"
                # берём часть после →, отрезаем последний сегмент (.to_col)
                ref_full = fk.split("→")[1]          # "[schema.]to_table.to_col"
                ref = ref_full.rsplit(".", 1)[0]      # "[schema.]to_table"
                if ref not in via_map:
                    to_add.setdefault(ref, set()).add(table_key)

        for t, parents in to_add.items():
            via_map[t] = parents
        current = set(to_add.keys())

    return via_map


def format_output(via_map: dict, schema: dict, found_direct: set) -> str:
    lines = []
    all_tables = set(via_map.keys())
    lines.append(f"Найдено таблиц: {len(all_tables)}")
    lines.append("=" * 60)
    lines.append("")

    for table_name in sorted(all_tables):
        item = schema.get(table_name)
        if not item:
            continue
        parents = via_map.get(table_name, set())
        if table_name in found_direct:
            tag = ""
        else:
            tag = f" [via FK from: {', '.join(sorted(parents))}]"
        lines.append(f"-- {table_name}{tag}")
        lines.append(item["text"])
        lines.append("")

    total = sum(len(schema[t]["text"]) for t in all_tables if t in schema)
    lines.append("=" * 60)
    lines.append(f"Символов: {total:,}")
    return "\n".join(lines)


def parse_columns(text: str) -> list[str]:
    """Парсит колонки корректно, игнорируя запятые внутри скобок []"""
    inner = text[text.find("(") + 1: text.rfind(")")]
    cols = []
    depth = 0
    current = []
    for ch in inner:
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        if ch == "," and depth == 0:
            cols.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        cols.append("".join(current).strip())
    return [c for c in cols if c]


def format_output_pretty(via_map: dict, schema: dict, found_direct: set) -> str:
    lines = []
    all_tables = set(via_map.keys())
    direct_count = len(found_direct)
    via_fk_count = len(all_tables) - direct_count

    lines.append("=" * 80)
    lines.append(
        f"RESULTS | tables: {len(all_tables)} | direct: {direct_count} | via_fk: {via_fk_count}"
    )
    lines.append("=" * 80)

    children: dict[str, list] = {}
    for table_name in all_tables - found_direct:
        for p in via_map.get(table_name, set()):
            children.setdefault(p, []).append(table_name)

    def print_table(table_name: str, indent: str, has_children: bool):
        item = schema.get(table_name)
        if not item:
            # таблица из другой схемы, может не быть в словаре (edge case)
            lines.append(f"{indent}┌─ {table_name}  [external]")
            return

        cols = parse_columns(item.get("text", ""))
        lines.append(f"{indent}┌─ {table_name}")
        for i, col in enumerate(cols):
            branch = "└── " if i == len(cols) - 1 else "├── "
            lines.append(f"{indent}│   {branch}{col}")

        if has_children:
            lines.append(f"{indent}│")

    def print_tree(table_name: str, indent: str, visited: set):
        if table_name in visited:
            return
        visited.add(table_name)

        kids = sorted(children.get(table_name, []))
        print_table(table_name, indent, has_children=bool(kids))

        for child in kids:
            lines.append(f"{indent}│")
            lines.append(f"{indent}└──► {child} (via FK)")
            print_tree(child, indent + "    ", visited)

    lines.append("")
    visited = set()
    for table_name in sorted(found_direct):
        print_tree(table_name, "", visited)

    total = sum(len(schema[t]["text"]) for t in all_tables if t in schema)
    lines.append("=" * 80)
    lines.append(f"Chars total: {total:,}")
    return "\n".join(lines)


def search_and_format(
    query: str,
    schema: dict,
    *,
    fuzzy: bool = False,
    fk: bool = False,
    depth: int = 1,
    pretty: bool = False,
    schema_filter: str | None = "public",
) -> str:
    found_direct = (
        search(query, schema, schema_filter)
        if fuzzy
        else search_exact_table(query, schema, schema_filter)
    )
    if not found_direct:
        return "Ничего не найдено"

    if fk:
        via_map = expand_fk(found_direct, schema, depth=depth)
    else:
        via_map = {t: set() for t in found_direct}

    formatter = format_output_pretty if pretty else format_output
    return formatter(via_map, schema, found_direct)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="?", help="Имя таблицы (по умолчанию точный поиск)")
    parser.add_argument("--fk",     action="store_true", help="Расширить по FK")
    parser.add_argument("--fuzzy",  action="store_true", help="Поиск по подстрокам")
    parser.add_argument("--depth",  type=int, default=1, help="Глубина FK (default: 1)")
    parser.add_argument("--pretty", action="store_true", help="Читаемый формат вывода")
    parser.add_argument(
        "--schema",
        default="public",
        help="Схема для поиска (default: public). Используй 'all' для поиска везде",
    )
    parser.add_argument("--interactive", "-i", action="store_true")
    args = parser.parse_args()

    schema_filter = None if args.schema == "all" else args.schema
    schema = load_schema()

    def run(query: str) -> None:
        print(
            search_and_format(
                query,
                schema,
                fuzzy=args.fuzzy,
                fk=args.fk,
                depth=args.depth,
                pretty=args.pretty,
                schema_filter=schema_filter,
            )
        )

    if args.interactive:
        print(f"Интерактивный режим. Схема: {args.schema}. 'q' — выход\n")
        while True:
            try:
                line = input("Поиск> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if line.lower() in ("q", "quit", "exit"):
                break
            if line:
                run(line)
    elif args.query:
        run(args.query)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
