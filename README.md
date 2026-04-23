# Tables Explorer

## Установка

```bash
pip install -r requirements.txt
```

## Пересборка схемы из CSV

```bash
python build_schema.py
```

## CLI-поиск по схеме

```bash
python search_schema.py "client_request_tab"
python search_schema.py "payment" --fuzzy
python search_schema.py "payment" --fuzzy --pretty
python search_schema.py "remont client" --fuzzy --fk
python search_schema.py "remont client" --fuzzy --fk --depth 2
python search_schema.py --interactive
python search_schema.py --interactive --fk --pretty
python search_schema.py "audit_phone_tab" --schema admin --fk --pretty
python search_schema.py "client_request_tab" --schema all --fuzzy
```

Кратчайший путь по FK (неориентированный граф, как во вкладке Tables в UI):

```bash
python search_schema.py --path-from admin.audit_phone_tab --path-to client_request_tab --schema all
```

## MCP для Cursor

Локальный stdio-сервер `tables_explorer_mcp.py`: инструменты `get_table`, `search_tables`, `fk_shortest_path` (плюс SQL-шаблоны внутри `get_table`).

Пример конфигурации (подставьте абсолютные пути):

```json
{
  "mcpServers": {
    "tables-explorer": {
      "command": "/ABS/PATH/tables-explorer/.venv/bin/python",
      "args": ["/ABS/PATH/tables-explorer/tables_explorer_mcp.py"]
    }
  }
}
```

Если в логах MCP `spawn ... wsl.exe ENOENT`, вы почти наверняка в **Remote-WSL**: не вызывайте `wsl.exe`, укажите в `command` путь к `python` внутри Linux (как в примере выше). Подробнее — докстринг в `tables_explorer_mcp.py`.

Перед использованием выполните `python build_schema.py`, чтобы существовал `output/schema_compact.json`.

## Streamlit UI

```bash
streamlit run app.py --server.port 9234
```

Вкладки в UI:

- `Tables` — поиск по `output/schema_compact.json`, карточки, Mermaid, копирование для LLM, **SQL-шаблоны** (SELECT/INSERT/UPDATE), **кратчайший путь по FK** (expander над поиском)
- `Функции` — live-поиск по `version_tab` в PostgreSQL
- `Таблицы из функции`, `Timeline функций` — см. `app.py`

## Настройка .env для вкладки `Функции`

Создай `.env` в корне проекта:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=your_database
PGUSER=your_user
PGPASSWORD=your_password
PGSSLMODE=prefer
PGCONNECT_TIMEOUT=5
```

Обязательные переменные:

- `PGHOST`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Необязательные:

- `PGPORT` — по умолчанию `5432`
- `PGSSLMODE`
- `PGCONNECT_TIMEOUT` — по умолчанию `5`

## Поиск функций

Вкладка `Функции`:

- ищет по `function_name` и `source_code`
- выполняет поиск только если запрос длиной не меньше `6` символов
- показывает последнюю версию функции по паре `schema_name + function_name`
- открывает полный `source_code` выбранной функции
- подсвечивает найденный текст в коде

Используемый SQL:

```sql
SELECT DISTINCT ON (t.schema_name, t.function_name)
       t.version_id,
       t.function_name,
       t.schema_name,
       t.source_code,
       t.rowversion,
       t.employee_id,
       t.pg_user,
       t.is_from_compare
FROM version_tab t
WHERE t.function_name ILIKE %(query)s
   OR t.source_code ILIKE %(query)s
ORDER BY t.schema_name, t.function_name, t.rowversion DESC
LIMIT %(limit)s;
```