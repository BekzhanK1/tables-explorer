from __future__ import annotations

import html
import re
from typing import Any

import streamlit as st
import streamlit.components.v1 as components
from pygments import highlight as pyg_highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import get_lexer_by_name

from db_functions import (
    DEFAULT_LIMIT,
    MIN_QUERY_LEN,
    FunctionRecord,
    FunctionVersion,
    fetch_functions,
    functions_search_sql_preview,
    extract_tables_from_function,
    fetch_function_timeline,
    compute_diff,
    compute_diff_stats,
)
from search_schema import (
    _schema_names,
    expand_fk,
    load_schema,
    parse_columns,
    search,
    search_exact_table,
)


@st.cache_resource
def cached_schema() -> dict:
    return load_schema()


@st.cache_data(ttl=300, show_spinner=False)
def cached_fetch_functions(query: str, limit: int) -> list[FunctionRecord]:
    return fetch_functions(query, limit)


@st.cache_data(ttl=300, show_spinner=False)
def cached_fetch_timeline(function_name: str, schema_name: str | None, source_db_filter: str | None) -> list[FunctionVersion]:
    return fetch_function_timeline(function_name, schema_name, source_db_filter)


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


def _parse_col(col_text: str, item: dict[str, Any]) -> dict:
    m = re.match(r"^(?P<name>\S+)\s+(?P<ctype>[^\[]+?)(?:\s+\[(?P<flags>.+)\])?$", col_text)
    if not m:
        return {"name": col_text, "type": "", "pk": False, "not_null": False, "fk": "", "desc": ""}
    name = m.group("name")
    ctype = (m.group("ctype") or "").strip()
    flags_raw = m.group("flags") or ""
    flags = [f.strip() for f in flags_raw.split(",")] if flags_raw else []
    fk_ref = next((f.replace("FK→", "", 1) for f in flags if f.startswith("FK→")), "")
    desc_map = item.get("columns_description", {})
    desc = " ".join(str(desc_map.get(name, "")).split())
    return {
        "name": name,
        "type": ctype,
        "pk": "PK" in flags,
        "not_null": "NOT NULL" in flags,
        "fk": fk_ref,
        "desc": desc,
    }


def _build_llm_text(
    sorted_tables: list[str],
    found_direct: set[str],
    via_map: dict[str, set],
    schema: dict,
) -> str:
    blocks: list[str] = []
    for table_name in sorted_tables:
        item = schema.get(table_name)
        if not item:
            blocks.append(f"-- {table_name}  [external / not in schema]")
            continue

        header = f"TABLE {table_name}"
        if table_name not in found_direct:
            parents = ", ".join(sorted(via_map.get(table_name, set())))
            header += f"  [via FK from: {parents}]"
        blocks.append(header)

        cols = [_parse_col(c, item) for c in parse_columns(item.get("text", ""))]
        if cols:
            max_name = max(len(c["name"]) for c in cols)
            max_type = max(len(c["type"]) for c in cols)
            for c in cols:
                flags_parts = []
                if c["pk"]:
                    flags_parts.append("PK")
                if c["fk"]:
                    flags_parts.append(f"FK→{c['fk']}")
                if c["not_null"] and not c["pk"]:
                    flags_parts.append("NOT NULL")
                flag_str = ("  [" + ", ".join(flags_parts) + "]") if flags_parts else ""
                desc_str = f"  -- {c['desc']}" if c["desc"] else ""
                line = (
                    f"  {c['name']:<{max_name}}  "
                    f"{c['type']:<{max_type}}"
                    f"{flag_str}{desc_str}"
                )
                blocks.append(line)

        fk_out_raw = item.get("fk_out", [])
        fk_in_raw = item.get("fk_in", [])
        fk_out_tables = sorted({ref.split("→")[1].rsplit(".", 1)[0] for ref in fk_out_raw if "→" in ref})
        fk_in_tables = sorted(set(fk_in_raw))
        if fk_out_tables:
            blocks.append(f"  FK_OUT: {', '.join(fk_out_tables)}")
        if fk_in_tables:
            blocks.append(f"  FK_IN:  {', '.join(fk_in_tables)}")

        blocks.append("")

    return "\n".join(blocks).rstrip()


def _render_table_card(table_name: str, item: dict, is_direct: bool, parents: list[str]) -> None:
    cols = [_parse_col(c, item) for c in parse_columns(item.get("text", ""))]

    rows_html = ""
    for c in cols:
        badges = ""
        if c["pk"]:
            badges += "<span style='background:#854d0e;color:#fef3c7;border-radius:3px;padding:1px 5px;font-size:11px;margin-right:3px'>PK</span>"
        if c["fk"]:
            short_ref = c["fk"].rsplit(".", 1)[0] if "." in c["fk"] else c["fk"]
            fk_full = html.escape(c["fk"])
            fk_short = html.escape(short_ref)
            badges += (
                f"<span style='background:#1e3a5f;color:#93c5fd;border-radius:3px;"
                f"padding:1px 5px;font-size:11px;margin-right:3px' title='FK→{fk_full}'>"
                f"FK→{fk_short}</span>"
            )
        if c["not_null"] and not c["pk"]:
            badges += "<span style='background:#374151;color:#9ca3af;border-radius:3px;padding:1px 5px;font-size:11px;margin-right:3px'>NOT NULL</span>"
        desc_cell = f"<span style='color:#9ca3af;font-size:12px'>{html.escape(c['desc'])}</span>" if c["desc"] else ""
        rows_html += (
            f"<tr>"
            f"<td style='padding:5px 10px;font-family:monospace;color:#e2e8f0;white-space:nowrap'>{html.escape(c['name'])}</td>"
            f"<td style='padding:5px 10px;font-family:monospace;color:#7dd3fc;white-space:nowrap'>{html.escape(c['type'])}</td>"
            f"<td style='padding:5px 10px'>{badges}</td>"
            f"<td style='padding:5px 10px'>{desc_cell}</td>"
            f"</tr>"
        )

    # fk_out entries are "col→schema.table.col" — extract unique referenced tables
    fk_out_raw = item.get("fk_out", [])
    fk_in_raw = item.get("fk_in", [])
    fk_out_tables = sorted({ref.split("→")[1].rsplit(".", 1)[0] for ref in fk_out_raw if "→" in ref})
    fk_in_tables = sorted(set(fk_in_raw))

    footer_html = ""
    if fk_out_tables or fk_in_tables:
        def _pill(label: str) -> str:
            return (
                f"<span style='display:inline-block;background:#1e293b;border:1px solid #334155;"
                f"border-radius:4px;padding:1px 7px;margin:2px 3px 2px 0;"
                f"font-size:11px;color:#94a3b8;font-family:monospace'>{html.escape(label)}</span>"
            )
        rows = ""
        if fk_out_tables:
            pills = "".join(_pill(t) for t in fk_out_tables)
            rows += f"<tr><td style='padding:4px 10px;color:#6b7280;font-size:11px;white-space:nowrap;vertical-align:top'>FK out →</td><td style='padding:4px 6px'>{pills}</td></tr>"
        if fk_in_tables:
            pills = "".join(_pill(t) for t in fk_in_tables)
            rows += f"<tr><td style='padding:4px 10px;color:#6b7280;font-size:11px;white-space:nowrap;vertical-align:top'>← FK in</td><td style='padding:4px 6px'>{pills}</td></tr>"
        footer_html = (
            f"<div style='border-top:1px solid #2d3748;padding:4px 0'>"
            f"<table style='border-collapse:collapse;width:100%'>{rows}</table></div>"
        )

    via_html = ""
    if not is_direct:
        via_html = f"<div style='color:#6b7280;font-size:12px;margin-bottom:6px'>via FK from: {html.escape(', '.join(parents))}</div>"

    table_html = (
        "<div style='background:#1a202c;border:1px solid #2d3748;border-radius:8px;"
        "overflow:hidden;margin-bottom:4px'>"
        + via_html
        + "<table style='width:100%;border-collapse:collapse'>"
        "<thead><tr style='background:#2d3748'>"
        "<th style='padding:6px 10px;text-align:left;color:#94a3b8;font-size:12px;font-weight:600'>column</th>"
        "<th style='padding:6px 10px;text-align:left;color:#94a3b8;font-size:12px;font-weight:600'>type</th>"
        "<th style='padding:6px 10px;text-align:left;color:#94a3b8;font-size:12px;font-weight:600'>flags</th>"
        "<th style='padding:6px 10px;text-align:left;color:#94a3b8;font-size:12px;font-weight:600'>description</th>"
        "</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        "</table>"
        + footer_html
        + "</div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


def _mermaid_safe(name: str) -> str:
    return name.replace(".", "__").replace("-", "_").replace(" ", "_")


def _mermaid_type(t: str) -> str:
    return t.split("(")[0].replace(" ", "_") or "text"


def _mermaid_field_name(name: str) -> str:
    # Mermaid identifiers can't start with a digit
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return f"_{safe}" if safe and safe[0].isdigit() else safe


def _build_mermaid(
    sorted_tables: list[str],
    found_direct: set[str],
    schema: dict,
) -> str:
    in_scope = set(sorted_tables)
    lines = ["erDiagram"]

    for table_name in sorted_tables:
        item = schema.get(table_name)
        if not item:
            continue
        safe = _mermaid_safe(table_name)
        cols = [_parse_col(c, item) for c in parse_columns(item.get("text", ""))]
        # Only show PK and FK columns — non-key cols clutter the diagram
        field_lines: list[str] = []
        for c in cols:
            if c["pk"]:
                field_lines.append(f"        {_mermaid_type(c['type'])} {_mermaid_field_name(c['name'])} PK")
            elif c["fk"]:
                field_lines.append(f"        {_mermaid_type(c['type'])} {_mermaid_field_name(c['name'])} FK")
        if not field_lines:
            # Table has no PK/FK — show first col so the box isn't empty
            for c in cols[:1]:
                field_lines.append(f"        {_mermaid_type(c['type'])} {_mermaid_field_name(c['name'])}")
        lines.append(f"    {safe} {{")
        lines.extend(field_lines)
        lines.append("    }")

    seen_rels: set[tuple[str, str]] = set()
    for table_name in sorted_tables:
        item = schema.get(table_name)
        if not item:
            continue
        safe_from = _mermaid_safe(table_name)
        for fk_str in item.get("fk_out", []):
            if "→" not in fk_str:
                continue
            col_part, ref_part = fk_str.split("→", 1)
            ref_table = ref_part.rsplit(".", 1)[0]
            if ref_table not in in_scope:
                continue
            safe_to = _mermaid_safe(ref_table)
            key = (safe_from, safe_to)
            if key in seen_rels:
                continue
            seen_rels.add(key)
            lines.append(f'    {safe_from} }}o--|| {safe_to} : "{col_part}"')

    return "\n".join(lines)


def _render_mermaid(mermaid_code: str, n_tables: int) -> None:
    height = 720
    escaped = (
        mermaid_code
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("`", "&#96;")
        .replace("$", "&#36;")
    )
    page = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<script src="https://cdn.jsdelivr.net/npm/svg-pan-zoom@3.6.1/dist/svg-pan-zoom.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f172a; overflow: hidden; font-family: sans-serif; }}
  #controls {{
    position: fixed; top: 8px; right: 8px; z-index: 100;
    display: flex; gap: 4px;
  }}
  #controls button {{
    background: #1e293b; color: #94a3b8;
    border: 1px solid #334155; border-radius: 4px;
    padding: 4px 12px; cursor: pointer; font-size: 15px; line-height: 1.4;
  }}
  #controls button:hover {{ background: #334155; color: #e2e8f0; }}
  #hint {{
    position: fixed; bottom: 8px; left: 8px;
    color: #475569; font-size: 11px;
  }}
  #wrap {{ width: 100%; height: {height}px; }}
  #wrap svg {{ width: 100%; height: 100%; }}
  #status {{ color: #94a3b8; padding: 24px; font-size: 14px; }}
  #err {{ color: #f87171; padding: 16px; white-space: pre-wrap; font-size: 13px; font-family: monospace; }}
</style>
</head>
<body>
<div id="controls">
  <button id="btn-in" title="Zoom in">+</button>
  <button id="btn-out" title="Zoom out">−</button>
  <button id="btn-reset" title="Fit to screen">⊡</button>
</div>
<div id="hint">Scroll to zoom · Drag to pan</div>
<div id="wrap">
  <div id="status">Rendering diagram…</div>
  <div id="src" style="display:none">{escaped}</div>
</div>
<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
  mermaid.initialize({{ startOnLoad: false, theme: 'dark' }});

  const wrap = document.getElementById('wrap');
  let pz = null;

  try {{
    const source = document.getElementById('src').textContent;
    const {{ svg }} = await mermaid.render('mg', source);
    wrap.innerHTML = svg;

    const svgEl = wrap.querySelector('svg');
    svgEl.removeAttribute('width');
    svgEl.removeAttribute('height');
    svgEl.style.width  = '100%';
    svgEl.style.height = '100%';

    pz = svgPanZoom(svgEl, {{
      zoomEnabled: true,
      panEnabled: true,
      controlIconsEnabled: false,
      fit: true,
      center: true,
      minZoom: 0.02,
      maxZoom: 20,
      zoomScaleSensitivity: 0.35,
    }});

    window.addEventListener('resize', () => {{ pz.resize(); pz.fit(); pz.center(); }});
  }} catch (e) {{
    wrap.innerHTML = '<div id="err">Mermaid error:\\n' + e.message + '</div>';
  }}

  document.getElementById('btn-in').onclick    = () => pz && pz.zoomIn();
  document.getElementById('btn-out').onclick   = () => pz && pz.zoomOut();
  document.getElementById('btn-reset').onclick = () => {{ if (pz) {{ pz.resetZoom(); pz.fit(); pz.center(); }} }};
</script>
</body>
</html>"""
    components.html(page, height=height + 10, scrolling=False)


def render_tables_tab(schema: dict, schemas: list[str]) -> None:
    with st.sidebar:
        st.header("Options")
        schema_filter_label = st.selectbox(
            "Schema", options=["all"] + schemas, index=1, key="tables_schema"
        )
        fuzzy = st.checkbox("Fuzzy", value=False, key="tables_fuzzy")
        fk = st.checkbox("Expand FK", value=True, key="tables_fk")
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

    found_direct = (
        search(q, schema, schema_filter) if fuzzy else search_exact_table(q, schema, schema_filter)
    )
    if not found_direct:
        st.warning("Nothing found.")
        return

    via_map = (
        expand_fk(found_direct, schema, depth=int(depth))
        if fk
        else {t: set() for t in found_direct}
    )
    all_tables = set(via_map.keys())
    sorted_tables = sorted(all_tables, key=lambda t: (t not in found_direct, t))

    st.caption(
        f"Tables: {len(all_tables)} · Direct: {len(found_direct)} · Via FK: {len(all_tables) - len(found_direct)}"
    )

    llm_text = _build_llm_text(sorted_tables, found_direct, via_map, schema)
    with st.expander("📋 Copy for LLM", expanded=True):
        st.code(llm_text, language=None)

    cards_tab, diagram_tab = st.tabs(["Cards", "Diagram"])

    with cards_tab:
        for table_name in sorted_tables:
            item = schema.get(table_name)
            if not item:
                st.warning(f"{table_name} (external)")
                continue
            parents = sorted(via_map.get(table_name, set()))
            is_direct = table_name in found_direct
            with st.expander(table_name, expanded=is_direct):
                _render_table_card(table_name, item, is_direct, parents)

    with diagram_tab:
        mermaid_code = _build_mermaid(sorted_tables, found_direct, schema)
        _render_mermaid(mermaid_code, len(sorted_tables))
        with st.expander("Mermaid source", expanded=False):
            st.code(mermaid_code, language="text")


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


def render_diff_colored(diff_text: str) -> None:
    """Отображает diff с цветовым кодированием"""
    if not diff_text:
        st.info("Нет изменений")
        return
    
    lines = diff_text.split('\n')
    html_lines = []
    
    html_lines.append('<div style="font-family:monospace;font-size:14px;line-height:1.5;background:#1e1e1e;padding:1rem;border-radius:0.5rem;overflow-x:auto;">')
    
    for line in lines:
        escaped_line = html.escape(line)
        if line.startswith('+') and not line.startswith('+++'):
            html_lines.append(f'<div style="background:#1a4d1a;color:#7dff7d;padding:2px 4px;">{escaped_line}</div>')
        elif line.startswith('-') and not line.startswith('---'):
            html_lines.append(f'<div style="background:#4d1a1a;color:#ff7d7d;padding:2px 4px;">{escaped_line}</div>')
        elif line.startswith('@@'):
            html_lines.append(f'<div style="background:#1a3a4d;color:#7dc8ff;padding:2px 4px;font-weight:bold;">{escaped_line}</div>')
        elif line.startswith('---') or line.startswith('+++'):
            html_lines.append(f'<div style="color:#888;padding:2px 4px;">{escaped_line}</div>')
        else:
            html_lines.append(f'<div style="color:#d4d4d4;padding:2px 4px;">{escaped_line}</div>')
    
    html_lines.append('</div>')
    st.markdown(''.join(html_lines), unsafe_allow_html=True)


def render_code_simple(code: str) -> None:
    """Простое отображение кода с подсветкой синтаксиса для timeline"""
    if not code:
        st.warning("Код недоступен")
        return
    
    # Используем st.code вместо components.html для совместимости с expander
    st.code(code, language="sql", line_numbers=True)


def render_code_with_changes(current_code: str, previous_code: str) -> None:
    """Отображает полный код с подсветкой изменений, включая удалённые строки"""
    import difflib
    
    if not current_code:
        st.warning("Код недоступен")
        return
    
    # Разбиваем код на строки
    current_lines = current_code.splitlines()
    previous_lines = previous_code.splitlines() if previous_code else []
    
    # Используем SequenceMatcher для определения изменений
    matcher = difflib.SequenceMatcher(None, previous_lines, current_lines)
    
    # Создаём объединённое представление с информацией об изменениях
    display_lines = []
    current_line_num = 1
    
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            # Строки без изменений
            for i in range(j1, j2):
                display_lines.append({
                    'line_num': current_line_num,
                    'content': current_lines[i],
                    'type': 'equal'
                })
                current_line_num += 1
                
        elif tag == 'replace':
            # Строки были изменены - показываем удалённые и добавленные
            # Сначала удалённые (красным)
            for i in range(i1, i2):
                display_lines.append({
                    'line_num': '—',
                    'content': previous_lines[i],
                    'type': 'deleted'
                })
            # Потом добавленные (зелёным)
            for i in range(j1, j2):
                display_lines.append({
                    'line_num': current_line_num,
                    'content': current_lines[i],
                    'type': 'added'
                })
                current_line_num += 1
                
        elif tag == 'delete':
            # Строки были удалены
            for i in range(i1, i2):
                display_lines.append({
                    'line_num': '—',
                    'content': previous_lines[i],
                    'type': 'deleted'
                })
                
        elif tag == 'insert':
            # Строки были добавлены
            for i in range(j1, j2):
                display_lines.append({
                    'line_num': current_line_num,
                    'content': current_lines[i],
                    'type': 'added'
                })
                current_line_num += 1
    
    # Формируем HTML с подсветкой
    html_lines = []
    html_lines.append('<div style="font-family:monospace;font-size:14px;line-height:1.6;background:#1e1e1e;padding:1rem;border-radius:0.5rem;overflow-x:auto;max-height:600px;overflow-y:auto;">')
    
    for line_info in display_lines:
        escaped_line = html.escape(line_info['content'])
        line_num = line_info['line_num']
        line_type = line_info['type']
        
        if line_type == 'added':
            # Добавленная строка - зелёный фон
            html_lines.append(
                f'<div style="background:#1a4d1a;color:#7dff7d;padding:2px 8px;border-left:3px solid #4ade80;">'
                f'<span style="color:#888;margin-right:1em;user-select:none;display:inline-block;width:3em;text-align:right;">{line_num}</span>'
                f'<span style="color:#4ade80;margin-right:0.5em;">+</span>'
                f'{escaped_line}</div>'
            )
        elif line_type == 'deleted':
            # Удалённая строка - красный фон
            html_lines.append(
                f'<div style="background:#4d1a1a;color:#ff7d7d;padding:2px 8px;border-left:3px solid #f87171;">'
                f'<span style="color:#888;margin-right:1em;user-select:none;display:inline-block;width:3em;text-align:right;">{line_num}</span>'
                f'<span style="color:#f87171;margin-right:0.5em;">−</span>'
                f'{escaped_line}</div>'
            )
        else:
            # Обычная строка
            html_lines.append(
                f'<div style="color:#d4d4d4;padding:2px 8px;">'
                f'<span style="color:#888;margin-right:1em;user-select:none;display:inline-block;width:3em;text-align:right;">{line_num}</span>'
                f'<span style="margin-right:0.5em;opacity:0;"> </span>'
                f'{escaped_line}</div>'
            )
    
    html_lines.append('</div>')
    
    # Добавляем легенду
    st.caption("🟢 Зелёным выделены добавленные строки · 🔴 Красным выделены удалённые строки")
    st.markdown(''.join(html_lines), unsafe_allow_html=True)


def render_function_timeline_tab() -> None:
    """Вкладка для просмотра истории изменений функции"""
    st.markdown("### 📜 История изменений функции")
    st.caption("Просмотр всех версий функции с визуализацией изменений")
    
    # Получаем список доступных баз данных
    from db_functions import _discover_databases
    available_dbs = _discover_databases()
    db_options = ["all"] + [db["name"] for db in available_dbs]
    
    with st.form("timeline_search_form"):
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            function_name = st.text_input(
                "Название функции",
                value=st.session_state.get("timeline_function_name", ""),
                placeholder="Введите точное название функции",
            )
        
        with col2:
            schema_name = st.text_input(
                "Схема (опционально)",
                value=st.session_state.get("timeline_schema_name", ""),
                placeholder="public",
            )
        
        with col3:
            source_db_filter = st.selectbox(
                "База данных",
                options=db_options,
                index=0,
                key="timeline_db_filter_select"
            )
        
        submitted = st.form_submit_button("Показать историю", use_container_width=True)
    
    if submitted:
        clean_name = function_name.strip()
        clean_schema = schema_name.strip() if schema_name.strip() else None
        db_filter = source_db_filter if source_db_filter != "all" else None
        
        st.session_state["timeline_function_name"] = clean_name
        st.session_state["timeline_schema_name"] = clean_schema or ""
        st.session_state["timeline_db_filter"] = db_filter
        st.session_state["timeline_error"] = ""
        st.session_state["timeline_results"] = []
        
        if not clean_name:
            st.session_state["timeline_error"] = "Введите название функции."
        else:
            with st.spinner("Загрузка истории..."):
                try:
                    versions = cached_fetch_timeline(clean_name, clean_schema, db_filter)
                    st.session_state["timeline_results"] = versions
                    if not versions:
                        st.session_state["timeline_error"] = f"Функция '{clean_name}' не найдена."
                except Exception as exc:
                    st.session_state["timeline_results"] = []
                    st.session_state["timeline_error"] = f"Ошибка: {str(exc)}"
    
    error_message = st.session_state.get("timeline_error", "")
    if error_message:
        st.warning(error_message)
        return
    
    versions: list[FunctionVersion] = st.session_state.get("timeline_results", [])
    
    if "timeline_results" not in st.session_state:
        return
    
    if not versions:
        return
    
    st.success(f"Найдено версий: {len(versions)}")
    
    # Предупреждение при поиске по всем базам
    if not st.session_state.get("timeline_db_filter"):
        # Проверяем, есть ли версии из разных баз
        all_sources = set()
        for v in versions:
            if v.source_dbs:
                all_sources.update(v.source_dbs)
            elif v.source_db:
                all_sources.add(v.source_db)
        
        if len(all_sources) > 1:
            st.warning(
                "⚠️ **Внимание:** Поиск выполнен по всем базам данных. "
                "Сравнение версий из разных баз может быть некорректным. "
                "Для точного сравнения выберите конкретную базу данных в форме поиска."
            )
    
    # Фильтр по источнику данных (только если не выбрана конкретная база)
    if versions and not st.session_state.get("timeline_db_filter"):
        # Собираем все уникальные источники
        all_sources = set()
        for v in versions:
            if v.source_dbs:
                all_sources.update(v.source_dbs)
            elif v.source_db:
                all_sources.add(v.source_db)
        
        sources = sorted(all_sources)
        
        if len(sources) > 1:
            st.caption("Фильтр по источнику данных:")
            selected_sources = st.multiselect(
                "Выберите базы данных:",
                options=sources,
                default=sources,
                key="timeline_source_filter"
            )
            
            if selected_sources:
                # Фильтруем версии
                filtered_versions = []
                for v in versions:
                    if v.source_dbs:
                        # Проверяем, есть ли хотя бы один источник в выбранных
                        if any(src in selected_sources for src in v.source_dbs):
                            filtered_versions.append(v)
                    elif v.source_db and v.source_db in selected_sources:
                        filtered_versions.append(v)
                
                versions = filtered_versions
                st.info(f"Отфильтровано версий: {len(versions)}")
    
    st.divider()
    
    # Отображаем версии от новых к старым
    for idx, version in enumerate(versions):
        is_first = (idx == len(versions) - 1)
        version_num = len(versions) - idx
        
        # Формируем заголовок expander с источником данных
        if version.source_dbs:
            # Множественные источники
            source_label = f"[{', '.join(version.source_dbs)}]"
        elif version.source_db:
            # Один источник
            source_label = f"[{version.source_db}]"
        else:
            source_label = ""
        
        if is_first:
            title = f"Версия #{version_num} (первая версия) {source_label} · 📅 {version.rowversion or '—'} · 👤 {version.pg_user or '—'}"
        elif idx == 0:
            title = f"Версия #{version_num} (текущая) {source_label} · 📅 {version.rowversion or '—'} · 👤 {version.pg_user or '—'}"
        else:
            title = f"Версия #{version_num} {source_label} · 📅 {version.rowversion or '—'} · 👤 {version.pg_user or '—'}"
        
        with st.expander(title, expanded=False):
            # Метаданные
            col1, col2, col3, col4 = st.columns(4)
            col1.caption(f"**Schema:** {version.schema_name}")
            col2.caption(f"**Version ID:** {version.version_id}")
            col3.caption(f"**Employee ID:** {version.employee_id or '—'}")
            
            # Отображаем источники
            if version.source_dbs:
                sources_str = ", ".join(version.source_dbs)
                col4.caption(f"**Sources:** {sources_str}")
            else:
                col4.caption(f"**Source DB:** {version.source_db or 'main'}")
            
            st.divider()
            
            if is_first:
                # Первая версия - только показываем код
                st.info("🎉 Создание функции")
                
                # Используем radio для переключения вместо кнопок
                view_mode = st.radio(
                    "Выберите режим просмотра:",
                    ["Скрыть", "Показать код"],
                    key=f"view_mode_first_{version.version_id}",
                    horizontal=True
                )
                
                if view_mode == "Показать код":
                    render_code_simple(version.source_code)
            else:
                # Не первая версия - показываем diff и статистику
                prev_version = versions[idx + 1]
                
                # Проверяем, из разных ли баз данных эти версии
                current_sources = set(version.source_dbs) if version.source_dbs else {version.source_db}
                prev_sources = set(prev_version.source_dbs) if prev_version.source_dbs else {prev_version.source_db}
                
                # Если версии из разных баз - показываем предупреждение
                different_sources = current_sources != prev_sources
                if different_sources and not st.session_state.get("timeline_db_filter"):
                    st.error(
                        "⚠️ **ВНИМАНИЕ:** Сравнение версий из разных баз данных может быть некорректным! "
                        f"Текущая версия из: {', '.join(sorted(current_sources))} | "
                        f"Предыдущая версия из: {', '.join(sorted(prev_sources))}"
                    )
                
                # Вычисляем статистику
                added, removed = compute_diff_stats(prev_version.source_code, version.source_code)
                
                if added > 0 or removed > 0:
                    st.markdown(f"**Изменения:** ➕ {added} строк, ➖ {removed} строк")
                else:
                    st.info("Нет изменений в коде")
                
                # Используем radio для переключения между режимами
                view_mode = st.radio(
                    "Выберите режим просмотра:",
                    ["Скрыть", "Показать diff", "Показать полный код"],
                    key=f"view_mode_{version.version_id}",
                    horizontal=True
                )
                
                # Показываем diff
                if view_mode == "Показать diff":
                    diff_text = compute_diff(prev_version.source_code, version.source_code)
                    if diff_text:
                        render_diff_colored(diff_text)
                    else:
                        st.info("Нет изменений")
                
                # Показываем полный код
                elif view_mode == "Показать полный код":
                    render_code_with_changes(version.source_code, prev_version.source_code)


def render_function_tables_tab(schema: dict) -> None:
    """Вкладка для поиска таблиц, используемых в функции."""
    with st.form("function_tables_form"):
        function_name = st.text_input(
            "Название функции",
            value=st.session_state.get("function_tables_query", ""),
            placeholder="Введите точное название функции",
        )
        submitted = st.form_submit_button("Найти таблицы", use_container_width=True)

    if submitted:
        clean_name = function_name.strip()
        st.session_state["function_tables_query"] = clean_name
        st.session_state["function_tables_error"] = ""
        st.session_state["function_tables_results"] = []

        if not clean_name:
            st.session_state["function_tables_error"] = "Введите название функции."
        else:
            with st.spinner("Поиск таблиц в функции..."):
                try:
                    tables = extract_tables_from_function(clean_name)
                    st.session_state["function_tables_results"] = tables
                    if not tables:
                        st.session_state["function_tables_error"] = (
                            f"Функция '{clean_name}' не найдена или не использует таблицы (заканчивающиеся на 'tab')."
                        )
                except Exception as exc:
                    st.session_state["function_tables_results"] = []
                    st.session_state["function_tables_error"] = str(exc)

    error_message = st.session_state.get("function_tables_error", "")
    if error_message:
        st.warning(error_message)
        return

    tables: list[str] = st.session_state.get("function_tables_results", [])
    
    if "function_tables_results" not in st.session_state:
        return

    if not tables:
        return

    st.success(f"Найдено таблиц: {len(tables)}")
    
    # Собираем все структуры таблиц в один текст для копирования
    all_structures = []
    all_structures.append(f"Найдено таблиц: {len(tables)}")
    all_structures.append("=" * 60)
    all_structures.append("")
    
    for table_name in tables:
        table_info = schema.get(table_name)
        if table_info:
            all_structures.append(f"-- {table_name}")
            all_structures.append(table_info.get("text", "Структура недоступна"))
            all_structures.append("")
        else:
            all_structures.append(f"-- {table_name}")
            all_structures.append(f"Таблица '{table_name}' не найдена в схеме.")
            all_structures.append("")
    
    total_chars = sum(len(schema[t]["text"]) for t in tables if t in schema)
    all_structures.append("=" * 60)
    all_structures.append(f"Символов: {total_chars:,}")
    
    result_text = "\n".join(all_structures)
    
    # Выводим в виде кода для удобного копирования
    st.code(result_text, language=None)


def main() -> None:
    st.set_page_config(page_title="Tables explorer", layout="wide")
    st.title("Tables explorer")

    schema = cached_schema()
    schemas = _schema_names(schema)
    tables_tab, functions_tab, function_tables_tab, timeline_tab = st.tabs(
        ["Tables", "Функции", "Таблицы из функции", "Timeline функций"]
    )

    with tables_tab:
        render_tables_tab(schema, schemas)

    with functions_tab:
        render_functions_tab()

    with function_tables_tab:
        render_function_tables_tab(schema)
    
    with timeline_tab:
        render_function_timeline_tab()


if __name__ == "__main__":
    main()
