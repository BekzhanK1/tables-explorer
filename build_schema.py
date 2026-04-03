import json
from pathlib import Path

import pandas as pd

RAW = Path("raw")
OUT = Path("output")
OUT.mkdir(exist_ok=True)

# --- Загрузка ---
cols_df = pd.read_csv(RAW / "columns.csv", sep=";")
fk_df = pd.read_csv(RAW / "foreign_keys.csv", sep=";")

# --- PK определяем по column_default (nextval = serial = PK) ---


def is_pk(default):
    return str(default).startswith("nextval")

# --- Короткий тип ---


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
            # убираем пробелы и конвертируем в int
            max_len_int = int(str(max_len).replace(
                "\xa0", "").replace(" ", ""))
            return f"varchar({max_len_int})"
        else:
            return "varchar"
    return m.get(data_type, data_type)


# --- FK индекс ---
fk_index = {}  # (from_table, from_col) -> "to_table.to_col"
fk_in = {}  # to_table -> ["from_table.from_col", ...]

for _, row in fk_df.iterrows():
    fk_index[(row["from_table"], row["from_col"])
             ] = f"{row['to_table']}.{row['to_col']}"
    fk_in.setdefault(row["to_table"], []).append(row["from_table"])

# --- Строим схему ---
schema = []

for table_name, group in cols_df.groupby("table_name", sort=True):
    columns = []
    col_names = []

    for _, row in group.iterrows():
        col = row["column_name"]
        ctype = short_type(row["data_type"], row["character_maximum_length"])
        flags = []

        if is_pk(row["column_default"]):
            flags.append("PK")
        if (table_name, col) in fk_index:
            flags.append(f"FK→{fk_index[(table_name, col)]}")
        if row["is_nullable"] == "NO" and not is_pk(row["column_default"]):
            flags.append("NOT NULL")

        flag_str = f" [{', '.join(flags)}]" if flags else ""
        columns.append(f"{col} {ctype}{flag_str}")
        col_names.append(col)

    fk_out_list = [
        f"{col}→{ref}"
        for (t, col), ref in fk_index.items()
        if t == table_name
    ]
    fk_in_list = fk_in.get(table_name, [])

    # Компактная строка для контекста
    text = f"{table_name}({', '.join(columns)})"

    schema.append({
        "table":    table_name,
        "text":     text,
        "columns":  col_names,
        "fk_out":   fk_out_list,
        "fk_in":    fk_in_list,
    })

# --- Сохраняем ---
with open(OUT / "schema_compact.json", "w", encoding="utf-8") as f:
    json.dump(schema, f, ensure_ascii=False, indent=2)

print(f"✓ Готово: {len(schema)} таблиц → output/schema_compact.json")
