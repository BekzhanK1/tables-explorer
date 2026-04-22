import json
from pathlib import Path

import pandas as pd

RAW = Path("raw")
OUT = Path("output")
OUT.mkdir(exist_ok=True)

PUBLIC = "public"

# --- Загрузка ---
cols_df = pd.read_csv(RAW / "columns.csv", sep=";")
fk_df = pd.read_csv(RAW / "foreign_keys.csv", sep=";")


def is_pk(default):
    return str(default).startswith("nextval")


def short_type(data_type, max_len):
    m = {
        "integer": "int",
        "bigint": "bigint",
        "double precision": "float",
        "boolean": "bool",
        "text": "text",
        "date": "date",
        "uuid": "uuid",
        "jsonb": "jsonb",
        "numeric": "numeric",
        "timestamp without time zone": "timestamp",
        "timestamp with time zone": "timestamptz",
    }
    if data_type == "character varying":
        if pd.notna(max_len):
            max_len_int = int(str(max_len).replace("\xa0", "").replace(" ", ""))
            return f"varchar({max_len_int})"
        else:
            return "varchar"
    return m.get(data_type, data_type)


def qualified(schema: str, table: str) -> str:
    """Returns table for public schema, schema.table for others."""
    return table if schema == PUBLIC else f"{schema}.{table}"


# --- FK индекс ---
# ключ: (from_schema, from_table, from_col)
# значение: "[schema.]to_table.to_col"  (schema опускается для public)
fk_index: dict[tuple, str] = {}
fk_in: dict[str, list[str]] = {}  # qualified_to → [qualified_from, ...]

for _, row in fk_df.iterrows():
    from_key = (row["from_schema"], row["from_table"], row["from_col"])
    to_q = qualified(row["to_schema"], row["to_table"])
    fk_index[from_key] = f"{to_q}.{row['to_col']}"

    from_q = qualified(row["from_schema"], row["from_table"])
    fk_in.setdefault(to_q, []).append(from_q)

# --- Строим схему ---
schema = []

for (table_schema, table_name), group in cols_df.groupby(
    ["table_schema", "table_name"], sort=True
):
    q_key = qualified(table_schema, table_name)
    columns = []
    col_names = []
    col_descriptions = {}

    for _, row in group.iterrows():
        col = row["column_name"]
        ctype = short_type(row["data_type"], row["character_maximum_length"])
        flags = []

        if is_pk(row["column_default"]):
            flags.append("PK")
        fk_ref = fk_index.get((table_schema, table_name, col))
        if fk_ref:
            flags.append(f"FK→{fk_ref}")
        if row["is_nullable"] == "NO" and not is_pk(row["column_default"]):
            flags.append("NOT NULL")

        flag_str = f" [{', '.join(flags)}]" if flags else ""
        columns.append(f"{col} {ctype}{flag_str}")
        col_names.append(col)
        desc = row.get("columns_description")
        if pd.isna(desc):
            desc = row.get("column_description")
        if pd.notna(desc):
            desc_str = str(desc).strip()
            if desc_str and desc_str.lower() != "null":
                col_descriptions[col] = desc_str

    fk_out_list = [
        f"{col}→{ref}"
        for (s, t, col), ref in fk_index.items()
        if s == table_schema and t == table_name
    ]
    fk_in_list = fk_in.get(q_key, [])

    text = f"{q_key}({', '.join(columns)})"

    schema.append({
        "schema": table_schema,
        "table":  q_key,        # qualified key — используется как ключ словаря
        "name":   table_name,   # короткое имя без схемы
        "text":   text,
        "columns": col_names,
        "columns_description": col_descriptions,
        "fk_out": fk_out_list,
        "fk_in":  fk_in_list,
    })

# --- Сохраняем ---
with open(OUT / "schema_compact.json", "w", encoding="utf-8") as f:
    json.dump(schema, f, ensure_ascii=False, indent=2)

print(f"✓ Готово: {len(schema)} таблиц → output/schema_compact.json")
