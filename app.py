from __future__ import annotations

import html
import re

import streamlit as st

from db_functions import DEFAULT_LIMIT, MIN_QUERY_LEN, FunctionRecord, fetch_functions
from search_schema import _schema_names, load_schema, search_and_format


@st.cache_resource
def cached_schema() -> dict:
    return load_schema()


@st.cache_data(ttl=60, show_spinner=False)
def cached_fetch_functions(query: str, limit: int) -> list[FunctionRecord]:
    return fetch_functions(query, limit)


def highlight_code(code: str, query: str) -> str:
    pattern = re.compile(re.escape(query.strip()), re.IGNORECASE)
    parts: list[str] = []
    last = 0

    for match in pattern.finditer(code):
        parts.append(html.escape(code[last:match.start()]))
        parts.append(f"<mark>{html.escape(match.group(0))}</mark>")
        last = match.end()

    parts.append(html.escape(code[last:]))
    highlighted = "".join(parts)

    return (
        "<pre style='white-space: pre-wrap; overflow-x: auto; "
        "padding: 1rem; border: 1px solid rgba(128,128,128,0.3); "
        "border-radius: 0.5rem;'>"
        f"{highlighted}</pre>"
    )


def render_tables_tab(schema: dict, schemas: list[str]) -> None:
    with st.sidebar:
        st.header("Options")
        schema_filter_label = st.selectbox(
            "Schema", options=["all"] + schemas, index=1, key="tables_schema"
        )
        fuzzy = st.checkbox("Fuzzy", value=False, key="tables_fuzzy")
        fk = st.checkbox("Expand FK", value=True, key="tables_fk")
        pretty = st.checkbox("Pretty", value=True, key="tables_pretty")
        depth = st.number_input(
            "FK depth", min_value=1, max_value=20, value=1, step=1, key="tables_depth"
        )

    schema_filter = None if schema_filter_label == "all" else schema_filter_label

    with st.form("search_form"):
        query = st.text_input("Table name or query", "")
        submitted = st.form_submit_button("Search")

    if not submitted:
        return

    q = query.strip()
    if not q:
        st.warning("Enter a query.")
        return

    result = search_and_format(
        q,
        schema,
        fuzzy=fuzzy,
        fk=fk,
        depth=int(depth),
        pretty=pretty,
        schema_filter=schema_filter,
    )
    st.code(result, language=None)


def render_functions_tab() -> None:
    st.caption(
        f"Поиск по `version_tab` в PostgreSQL. Минимальная длина запроса: {MIN_QUERY_LEN} символов."
    )

    with st.form("functions_search_form"):
        query = st.text_input(
            "Имя функции или текст в исходнике",
            value=st.session_state.get("functions_query", ""),
            placeholder="remont_contragent_get_ostatok_sum",
        )
        submitted = st.form_submit_button("Search functions")

    if submitted:
        clean_query = query.strip()
        st.session_state["functions_query"] = clean_query

        if len(clean_query) < MIN_QUERY_LEN:
            st.session_state["functions_results"] = []
            st.session_state["functions_error"] = (
                f"Введите минимум {MIN_QUERY_LEN} символов."
            )
        else:
            try:
                records = cached_fetch_functions(clean_query, DEFAULT_LIMIT)
            except Exception as exc:
                st.session_state["functions_results"] = []
                st.session_state["functions_error"] = str(exc)
            else:
                st.session_state["functions_results"] = records
                st.session_state["functions_error"] = ""

    error_message = st.session_state.get("functions_error", "")
    if error_message:
        st.warning(error_message)

    records: list[FunctionRecord] = st.session_state.get("functions_results", [])
    current_query = st.session_state.get("functions_query", "")
    if not records:
        return

    st.write(f"Найдено функций: {len(records)}")
    if len(records) >= DEFAULT_LIMIT:
        st.info(f"Показаны первые {DEFAULT_LIMIT} результатов.")

    selected_label = st.selectbox(
        "Открыть функцию",
        options=[record.qualified_name for record in records],
        key="selected_function_label",
    )
    selected_record = next(
        record for record in records if record.qualified_name == selected_label
    )

    meta1, meta2, meta3, meta4 = st.columns(4)
    meta1.metric("Schema", selected_record.schema_name)
    meta2.metric("Function", selected_record.function_name)
    meta3.metric("Version ID", str(selected_record.version_id))
    meta4.metric("User", selected_record.pg_user or "-")

    rowversion = selected_record.rowversion or "-"
    employee_id = (
        str(selected_record.employee_id)
        if selected_record.employee_id is not None
        else "-"
    )
    compare_flag = (
        "yes" if selected_record.is_from_compare else "no"
        if selected_record.is_from_compare is not None
        else "-"
    )
    st.caption(
        f"rowversion: {rowversion} | employee_id: {employee_id} | is_from_compare: {compare_flag}"
    )
    st.markdown(highlight_code(selected_record.source_code, current_query), unsafe_allow_html=True)


def main() -> None:
    st.set_page_config(page_title="Tables explorer", layout="wide")
    st.title("Tables explorer")

    schema = cached_schema()
    schemas = _schema_names(schema)
    tables_tab, functions_tab = st.tabs(["Tables", "Функции"])

    with tables_tab:
        render_tables_tab(schema, schemas)

    with functions_tab:
        render_functions_tab()


if __name__ == "__main__":
    main()
