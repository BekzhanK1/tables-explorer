from __future__ import annotations

import html
import re

import streamlit as st
import streamlit.components.v1 as components
from pygments import highlight as pyg_highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name

from db_functions import (
    DEFAULT_LIMIT,
    MIN_QUERY_LEN,
    FunctionRecord,
    fetch_functions,
    functions_search_sql_preview,
)
from search_schema import _schema_names, load_schema, search_and_format


@st.cache_resource
def cached_schema() -> dict:
    return load_schema()


@st.cache_data(ttl=300, show_spinner=False)
def cached_fetch_functions(query: str, limit: int) -> list[FunctionRecord]:
    return fetch_functions(query, limit)


def _lexer_plpgsql():
    try:
        return get_lexer_by_name("plpgsql")
    except Exception:
        return get_lexer_by_name("postgresql")


def render_code(code: str, query: str) -> None:
    # noclasses + monokai: inline styles, без внешнего CSS — стабильнее в iframe
    formatter = HtmlFormatter(style="monokai", nowrap=False, noclasses=True)
    lexer = _lexer_plpgsql()

    code_html = pyg_highlight(code, lexer, formatter)

    clean_q = query.strip()
    if clean_q:
        escaped_q = html.escape(clean_q)
        pattern = re.compile(re.escape(escaped_q), re.IGNORECASE)
        code_html = pattern.sub(
            lambda m: (
                "<mark style='background:#ffe066;color:#1a1a1a;"
                "border-radius:2px;padding:0 2px'>"
                f"{m.group(0)}</mark>"
            ),
            code_html,
        )

    # st.markdown в @st.dialog ломает вложенный HTML (<span> внутри <pre>);
    # components.html рендерит сырой HTML в iframe.
    full_page = (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<style>"
        "body{margin:0;background:#272822;color:#f8f8f2;font-size:16px;"
        "font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}"
        ".highlight{border-radius:.5rem;overflow:hidden;font-size:16px;}"
        ".highlight pre span{font-size:inherit!important;}"
        ".highlight pre{margin:0;padding:1rem;font-size:16px;line-height:1.6;"
        "white-space:pre-wrap;word-break:break-word;overflow-x:auto;}"
        "</style></head><body>"
        f"{code_html}"
        "</body></html>"
    )
    components.html(full_page, height=560, scrolling=True)


@st.dialog("Source code", width="large")
def show_code_modal(record: FunctionRecord, query: str) -> None:
    st.caption(
        f"`{record.schema_name}.{record.function_name}`"
        f"  ·  v{record.version_id}"
        f"  ·  {record.pg_user or '—'}"
        f"  ·  {record.rowversion or '—'}"
    )
    render_code(record.source_code, query)


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
        q, schema, fuzzy=fuzzy, fk=fk, depth=int(depth), pretty=pretty,
        schema_filter=schema_filter,
    )
    st.code(result, language=None)


def render_functions_tab() -> None:
    with st.form("functions_search_form"):
        query_input = st.text_input(
            "",
            value=st.session_state.get("functions_query", ""),
            placeholder=f"Function name or code fragment (min {MIN_QUERY_LEN} chars)",
        )
        submitted = st.form_submit_button("Search", use_container_width=True)

    if submitted:
        clean_query = query_input.strip()
        st.session_state["functions_query"] = clean_query
        st.session_state["functions_error"] = ""

        if len(clean_query) < MIN_QUERY_LEN:
            st.session_state["functions_results"] = []
            st.session_state["functions_error"] = (
                f"Enter at least {MIN_QUERY_LEN} characters."
            )
        else:
            with st.spinner("Searching..."):
                try:
                    records = cached_fetch_functions(clean_query, DEFAULT_LIMIT)
                    st.session_state["functions_results"] = records
                except Exception as exc:
                    st.session_state["functions_results"] = []
                    st.session_state["functions_error"] = str(exc)

    preview_q = st.session_state.get("functions_query", "").strip()
    if len(preview_q) >= MIN_QUERY_LEN:
        with st.expander("SQL query", expanded=False):
            st.caption(
                "Тот же запрос, что уходит в БД (ниже — литералы для копипаста в psql; "
                "в коде используются bind-параметры)."
            )
            st.code(
                functions_search_sql_preview(preview_q, DEFAULT_LIMIT),
                language="sql",
            )

    error_message = st.session_state.get("functions_error", "")
    if error_message:
        st.warning(error_message)
        return

    records: list[FunctionRecord] = st.session_state.get("functions_results", [])
    current_query = st.session_state.get("functions_query", "")

    if "functions_results" not in st.session_state:
        return

    if not records:
        if current_query:
            st.caption("No functions found.")
        return

    n = len(records)
    suffix = f" · showing first {DEFAULT_LIMIT}" if n >= DEFAULT_LIMIT else ""
    st.caption(f"{n} function{'s' if n != 1 else ''} found{suffix}")
    st.divider()

    for record in records:
        col_name, col_meta, col_btn = st.columns([5, 3, 1])
        col_name.markdown(f"**{record.function_name}**")
        col_meta.caption(
            f"{record.schema_name}  ·  {record.pg_user or '—'}  ·  {record.rowversion or '—'}"
        )
        if col_btn.button("View", key=f"view_{record.version_id}"):
            show_code_modal(record, current_query)


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
