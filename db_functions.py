from __future__ import annotations

import difflib
import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row


MIN_QUERY_LEN = 6
DEFAULT_LIMIT = 200

# Один источник правды для fetch и для предпросмотра на фронте.
FUNCTIONS_SEARCH_SQL = """
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
"""


def functions_search_sql_preview(query: str, limit: int = DEFAULT_LIMIT) -> str:
    """
    Текст запроса для UI (копипаст в psql).
    Параметры подставлены как литералы; апострофы в шаблоне экранированы.
    Реальное выполнение в fetch_functions по-прежнему через bind-параметры.
    """
    clean = query.strip()
    pattern = f"%{clean}%"
    lit = pattern.replace("'", "''")
    lim = int(limit)
    return (
        "-- ILIKE pattern (same as bound %(query)s in app)\n"
        f"SELECT DISTINCT ON (t.schema_name, t.function_name)\n"
        f"       t.version_id,\n"
        f"       t.function_name,\n"
        f"       t.schema_name,\n"
        f"       t.source_code,\n"
        f"       t.rowversion,\n"
        f"       t.employee_id,\n"
        f"       t.pg_user,\n"
        f"       t.is_from_compare\n"
        f"FROM version_tab t\n"
        f"WHERE t.function_name ILIKE '{lit}'\n"
        f"   OR t.source_code ILIKE '{lit}'\n"
        f"ORDER BY t.schema_name, t.function_name, t.rowversion DESC\n"
        f"LIMIT {lim};"
    )


@dataclass(frozen=True)
class FunctionRecord:
    version_id: int
    function_name: str
    schema_name: str
    source_code: str
    rowversion: str | None
    employee_id: int | None
    pg_user: str | None
    is_from_compare: bool | None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.function_name}"


@dataclass(frozen=True)
class FunctionVersion:
    version_id: int
    function_name: str
    schema_name: str
    source_code: str
    rowversion: str | None
    employee_id: int | None
    pg_user: str | None
    source_db: str | None = None
    source_dbs: tuple[str, ...] | None = None  # Список всех источников для этой версии


def _load_db_settings() -> dict[str, Any]:
    load_dotenv()

    required_keys = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [key for key in required_keys if not os.getenv(key)]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(
            f"Missing database settings in .env: {missing_list}"
        )

    settings: dict[str, Any] = {
        "host": os.getenv("PGHOST"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
    }

    sslmode = os.getenv("PGSSLMODE")
    if sslmode:
        settings["sslmode"] = sslmode

    return settings


def _discover_databases() -> list[dict[str, Any]]:
    """
    Автоматически обнаруживает все настроенные базы данных из .env
    Формат: DB_{NAME}_HOST, DB_{NAME}_DATABASE, и т.д.
    """
    load_dotenv()
    databases = []
    
    # 1. Основная база (обязательная)
    main_db = {
        "name": "main",
        "host": os.getenv("PGHOST"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": os.getenv("PGDATABASE"),
        "user": os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
        "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
    }
    
    sslmode = os.getenv("PGSSLMODE")
    if sslmode:
        main_db["sslmode"] = sslmode
    
    if all([main_db["host"], main_db["dbname"], main_db["user"], main_db["password"]]):
        databases.append(main_db)
    
    # 2. Поиск дополнительных баз по паттерну DB_{NAME}_HOST
    env_vars = os.environ
    db_names = set()
    
    for key in env_vars.keys():
        if key.startswith("DB_") and "_HOST" in key:
            db_name = key.replace("DB_", "").replace("_HOST", "")
            db_names.add(db_name)
    
    # 3. Сборка конфигурации для каждой найденной базы
    for db_name in sorted(db_names):
        prefix = f"DB_{db_name}_"
        
        db_config = {
            "name": db_name.lower(),
            "host": os.getenv(f"{prefix}HOST"),
            "port": int(os.getenv(f"{prefix}PORT", "5432")),
            "dbname": os.getenv(f"{prefix}DATABASE"),
            "user": os.getenv(f"{prefix}USER"),
            "password": os.getenv(f"{prefix}PASSWORD"),
            "connect_timeout": int(os.getenv(f"{prefix}CONNECT_TIMEOUT", "5")),
        }
        
        sslmode = os.getenv(f"{prefix}SSLMODE")
        if sslmode:
            db_config["sslmode"] = sslmode
        
        if all([db_config["host"], db_config["dbname"], 
                db_config["user"], db_config["password"]]):
            databases.append(db_config)
    
    return databases


def fetch_functions(query: str, limit: int = DEFAULT_LIMIT) -> list[FunctionRecord]:
    clean_query = query.strip()
    if len(clean_query) < MIN_QUERY_LEN:
        raise ValueError(f"Search query must be at least {MIN_QUERY_LEN} characters")

    sql = FUNCTIONS_SEARCH_SQL

    params = {
        "query": f"%{clean_query}%",
        "limit": limit,
    }

    with psycopg.connect(**_load_db_settings(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        FunctionRecord(
            version_id=row["version_id"],
            function_name=row["function_name"],
            schema_name=row["schema_name"],
            source_code=row["source_code"] or "",
            rowversion=str(row["rowversion"]) if row["rowversion"] is not None else None,
            employee_id=row["employee_id"],
            pg_user=row["pg_user"],
            is_from_compare=row["is_from_compare"],
        )
        for row in rows
    ]


def extract_tables_from_function(function_name: str) -> list[str]:
    """
    Извлекает список таблиц, используемых в функции.
    Ищет функцию по имени и парсит её код.
    Возвращает только таблицы, заканчивающиеся на 'tab'.
    """
    import re
    
    clean_name = function_name.strip()
    if not clean_name:
        raise ValueError("Function name cannot be empty")
    
    # Ищем функцию по точному имени
    sql = """
    SELECT DISTINCT ON (t.schema_name, t.function_name)
           t.source_code
    FROM version_tab t
    WHERE t.function_name ILIKE %(name)s
    ORDER BY t.schema_name, t.function_name, t.rowversion DESC
    LIMIT 1;
    """
    
    with psycopg.connect(**_load_db_settings(), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"name": clean_name})
            row = cur.fetchone()
    
    if not row or not row.get("source_code"):
        return []
    
    source_code = row["source_code"]
    tables = set()
    
    # Паттерны для поиска таблиц в SQL-коде
    # FROM/JOIN table_name или schema.table_name
    patterns = [
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bINSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
        r'\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, source_code, re.IGNORECASE)
        for match in matches:
            table_name = match.group(1).strip()
            # Убираем алиасы и лишние пробелы
            table_name = table_name.split()[0] if ' ' in table_name else table_name
            
            # Извлекаем имя таблицы без схемы для проверки
            if '.' in table_name:
                schema_part, table_part = table_name.rsplit('.', 1)
            else:
                schema_part = 'public'
                table_part = table_name
            
            # Фильтруем только таблицы, заканчивающиеся на 'tab'
            if not table_part.lower().endswith('tab'):
                continue
            
            # Формируем ключ для поиска в схеме
            if schema_part.lower() == 'public':
                tables.add(table_part)
            else:
                tables.add(f"{schema_part}.{table_part}")
    
    return sorted(list(tables))


def fetch_function_timeline(
    function_name: str, 
    schema_name: str | None = None,
    source_db_filter: str | None = None
) -> list[FunctionVersion]:
    """Получает все версии функции из всех настроенных баз данных"""
    clean_name = function_name.strip()
    if not clean_name:
        raise ValueError("Function name cannot be empty")
    
    all_versions = []
    databases = _discover_databases()
    
    # Фильтруем базы данных если указан фильтр
    if source_db_filter and source_db_filter != "all":
        databases = [db for db in databases if db.get("name") == source_db_filter]
    
    # Формируем SQL запрос
    if schema_name:
        sql = """
        SELECT t.version_id,
               t.function_name,
               t.schema_name,
               t.source_code,
               t.rowversion,
               t.employee_id,
               t.pg_user
        FROM version_tab t
        WHERE t.function_name ILIKE %(function_name)s
          AND t.schema_name = %(schema_name)s
        ORDER BY t.rowversion DESC;
        """
        params = {"function_name": clean_name, "schema_name": schema_name}
    else:
        sql = """
        SELECT t.version_id,
               t.function_name,
               t.schema_name,
               t.source_code,
               t.rowversion,
               t.employee_id,
               t.pg_user
        FROM version_tab t
        WHERE t.function_name ILIKE %(function_name)s
        ORDER BY t.rowversion DESC;
        """
        params = {"function_name": clean_name}
    
    # Собираем данные из всех баз
    for db_config in databases:
        db_name = db_config.pop("name")
        
        try:
            with psycopg.connect(**db_config, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall()
                    
                    for row in rows:
                        all_versions.append(FunctionVersion(
                            version_id=row["version_id"],
                            function_name=row["function_name"],
                            schema_name=row["schema_name"],
                            source_code=row["source_code"] or "",
                            rowversion=str(row["rowversion"]) if row["rowversion"] is not None else None,
                            employee_id=row["employee_id"],
                            pg_user=row["pg_user"],
                            source_db=db_name,
                            source_dbs=None,  # Будет заполнено при группировке
                        ))
        except Exception as e:
            import sys
            print(f"Warning: Could not fetch from database '{db_name}': {e}", file=sys.stderr)
            continue
    
    # Сортировка по времени (от новых к старым) и version_id для стабильности
    all_versions.sort(key=lambda v: (v.rowversion or "0000-00-00", v.version_id), reverse=True)
    
    # Группировка дубликатов по version_id и объединение источников
    # Используем OrderedDict для сохранения порядка
    from collections import OrderedDict
    version_map = OrderedDict()
    
    for version in all_versions:
        if version.version_id not in version_map:
            version_map[version.version_id] = {
                "version": version,
                "sources": [version.source_db],
                "rowversion": version.rowversion  # Сохраняем для сортировки
            }
        else:
            # Добавляем источник к существующей версии
            version_map[version.version_id]["sources"].append(version.source_db)
    
    # Создаём финальный список с объединёнными источниками
    unique_versions = []
    for version_id, data in version_map.items():
        version = data["version"]
        sources = tuple(sorted(set(data["sources"])))  # Уникальные источники, отсортированные
        
        # Создаём новую версию с обновлённым списком источников
        unique_versions.append(FunctionVersion(
            version_id=version.version_id,
            function_name=version.function_name,
            schema_name=version.schema_name,
            source_code=version.source_code,
            rowversion=version.rowversion,
            employee_id=version.employee_id,
            pg_user=version.pg_user,
            source_db=sources[0] if len(sources) == 1 else None,  # Один источник
            source_dbs=sources if len(sources) > 1 else None,  # Множественные источники
        ))
    
    # Финальная сортировка уже не нужна, так как OrderedDict сохранил порядок
    # Но на всякий случай пересортируем для гарантии
    unique_versions.sort(key=lambda v: (v.rowversion or "0000-00-00", v.version_id), reverse=True)
    
    return unique_versions


def compute_diff(old_code: str, new_code: str) -> str:
    """Вычисляет unified diff между двумя версиями кода"""
    old_lines = old_code.splitlines(keepends=True)
    new_lines = new_code.splitlines(keepends=True)
    
    diff = difflib.unified_diff(
        old_lines, 
        new_lines,
        lineterm='',
        fromfile='предыдущая версия',
        tofile='текущая версия'
    )
    
    return ''.join(diff)


def compute_diff_stats(old_code: str, new_code: str) -> tuple[int, int]:
    """Возвращает (добавлено_строк, удалено_строк)"""
    diff_text = compute_diff(old_code, new_code)
    
    added = 0
    removed = 0
    
    for line in diff_text.split('\n'):
        if line.startswith('+') and not line.startswith('+++'):
            added += 1
        elif line.startswith('-') and not line.startswith('---'):
            removed += 1
    
    return (added, removed)
