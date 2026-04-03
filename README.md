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