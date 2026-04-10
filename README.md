# 1. Установить зависимости
pip install pandas sentence-transformers numpy

# 2. Собрать схему
python build_schema.py

# 3. Поиск по схеме (без torch, без эмбеддингов, просто Python + JSON)

# По умолчанию: точный поиск таблицы по имени (без подстрок и без поиска по колонкам).
python search_schema.py "client_request_tab"

# Поиск по сходству (подстроки в названиях таблиц и колонок).
python search_schema.py "payment" --fuzzy

# Красивый, более читаемый формат вывода.
python search_schema.py "payment" --fuzzy --pretty

# То же, но дополнительно показывает связанные таблицы по внешним ключам (FK).
# Удобно, когда нужно понять контекст вокруг найденной сущности.
python search_schema.py "remont client" --fuzzy --fk

# То же с FK, но глубже по графу связей:
# depth 2 = прямые связи + связи следующего уровня.
python search_schema.py "remont client" --fuzzy --fk --depth 2

# Запускает интерактивный режим:
# можно вводить несколько запросов подряд без перезапуска скрипта.
python search_schema.py --interactive

# Интерактивный режим с FK и красивым выводом.
python search_schema.py --interactive --fk --pretty




SELECT
    tc.table_schema AS from_schema,
    tc.table_name AS from_table,
    kcu.column_name AS from_col,
    ccu.table_schema AS to_schema,
    ccu.table_name AS to_table,
    ccu.column_name AS to_col,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_schema = kcu.constraint_schema
    AND tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_schema = tc.constraint_schema
    AND ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema IN (
    'admin', 'backend', 'backup', 'buh', 'client', 'constructor', 'contractor',
    'create_sys', 'critical_ops', 'crm', 'ds', 'ff', 'gpr', 'innermaster', 'knb',
    'landing', 'log', 'mng_report', 'mobile', 'nca', 'notify', 'okk', 'partner',
    'power_bi', 'provider', 'public', 'react_new', 'rem_logic', 'remarket',
    'report', 'request', 'rest', 'rm', 'sale', 'sbr', 'sbs', 'smarthome',
    'techproject', 'trash', 'utils', 'wcheck', 'wh'
  )
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;





SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.is_nullable,
    c.column_default
FROM information_schema.columns c
JOIN information_schema.tables t
    ON c.table_schema = t.table_schema
    AND c.table_name = t.table_name
WHERE c.table_schema IN (
    'admin', 'backend', 'backup', 'buh', 'client', 'constructor', 'contractor',
    'create_sys', 'critical_ops', 'crm', 'ds', 'ff', 'gpr', 'innermaster', 'knb',
    'landing', 'log', 'mng_report', 'mobile', 'nca', 'notify', 'okk', 'partner',
    'power_bi', 'provider', 'public', 'react_new', 'rem_logic', 'remarket',
    'report', 'request', 'rest', 'rm', 'sale', 'sbr', 'sbs', 'smarthome',
    'techproject', 'trash', 'utils', 'wcheck', 'wh'
)
  AND t.table_type = 'BASE TABLE'
ORDER BY c.table_schema, c.table_name, c.ordinal_position;