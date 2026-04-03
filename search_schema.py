"""
python search_schema.py "payment client"
python search_schema.py "remont" --fk
python search_schema.py "remont payment client" --fk --depth 2
"""

import argparse
import json
from pathlib import Path

SCHEMA_PATH = Path("output/schema_compact.json")


def load_schema():
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        data = json.load(f)
    return {item["table"]: item for item in data}


def search(query: str, schema: dict) -> set:
    keywords = query.lower().split()
    found = set()
    for table_name, item in schema.items():
        if any(kw in table_name.lower() for kw in keywords):
            found.add(table_name)
            continue
        if any(any(kw in col.lower() for kw in keywords) for col in item["columns"]):
            found.add(table_name)
    return found


def search_exact_table(query: str, schema: dict) -> set:
    q = query.strip()
    if not q:
        return set()
    if q in schema:
        return {q}
    q_lower = q.lower()
    for table_name in schema:
        if table_name.lower() == q_lower:
            return {table_name}
    return set()


def expand_fk(tables: set, schema: dict, depth: int) -> dict:
    via_map: dict[str, set] = {t: set() for t in tables}
    current = set(tables)

    for _ in range(depth):
        to_add: dict[str, set] = {}
        for table_name in current:
            item = schema.get(table_name, {})
            # только fk_out раскрываем как дерево
            for fk in item.get("fk_out", []):
                ref = fk.split("→")[1].split(".")[0]
                if ref not in via_map:
                    to_add.setdefault(ref, set()).add(table_name)
            # fk_in больше не раскрываем

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
        f"RESULTS | tables: {len(all_tables)} | direct: {direct_count} | via_fk: {via_fk_count}")
    lines.append("=" * 80)

    # parent -> [children]
    children: dict[str, list] = {}
    for table_name in all_tables - found_direct:
        for p in via_map.get(table_name, set()):
            children.setdefault(p, []).append(table_name)

    def print_table(table_name: str, indent: str, has_children: bool):
        item = schema.get(table_name)
        if not item:
            return

        cols = parse_columns(item.get("text", ""))
        # fk_in = item.get("fk_in", [])

        lines.append(f"{indent}┌─ {table_name}")
        for i, col in enumerate(cols):
            branch = "└── " if i == len(cols) - 1 else "├── "
            # and not fk_in else "├── "
            lines.append(f"{indent}│   {branch}{col}")

        # if fk_in:
        #     lines.append(f"{indent}│   │")
        #     lines.append(
        #         f"{indent}│   └── referenced by: {', '.join(sorted(fk_in))}")

        if has_children:
            lines.append(f"{indent}│")

    def print_tree(table_name: str, indent: str, visited: set):
        if table_name in visited:
            return
        visited.add(table_name)

        kids = sorted(children.get(table_name, []))
        # <-- передаём флаг
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
) -> str:
    found_direct = (
        search(query, schema) if fuzzy else search_exact_table(query, schema)
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
    parser.add_argument("query", nargs="?",
                        help="Имя таблицы (по умолчанию точный поиск)")
    parser.add_argument("--fk",    action="store_true", help="Расширить по FK")
    parser.add_argument("--fuzzy", action="store_true",
                        help="Поиск по подстрокам в именах таблиц и колонок")
    parser.add_argument("--depth", type=int, default=1,
                        help="Глубина FK (default: 1)")
    parser.add_argument("--pretty", action="store_true",
                        help="Читаемый формат вывода")
    parser.add_argument("--interactive", "-i", action="store_true")
    args = parser.parse_args()

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
            )
        )

    if args.interactive:
        print("Интерактивный режим. 'q' — выход\n")
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
