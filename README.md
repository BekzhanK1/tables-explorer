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

## Streamlit UI

```bash
streamlit run app.py --server.port 9234
```

В приложении есть 2 вкладки:

- `Tables` — поиск по собранному `output/schema_compact.json`
- `Функции` — live-поиск по `version_tab` в PostgreSQL

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