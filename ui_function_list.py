from __future__ import annotations

from typing import Literal

import pandas as pd
import streamlit as st

from db_functions import (
    FunctionCompareResult,
    FunctionRecord,
    compare_status_label,
    compute_diff,
    normalize_ddl,
)

StatusFilter = Literal["all", "new", "changed", "same"]

_STATUS_LABEL = {
    "new": "🆕 нет на PROD",
    "changed": "⚠️ ≠ PROD",
    "same": "✅ = PROD",
}

_STATUS_SEGMENT = {
    "all": "Все",
    "new": "Нет на PROD",
    "changed": "Отличается",
    "same": "Совпадает",
}


def _compare_by_name(
    compare_results: list[FunctionCompareResult] | None,
) -> dict[str, FunctionCompareResult]:
    if not compare_results:
        return {}
    return {item.qualified_name: item for item in compare_results}


def _filter_records(
    records: list[FunctionRecord],
    compare_map: dict[str, FunctionCompareResult],
    *,
    text: str,
    schema: str,
    status: StatusFilter,
) -> list[FunctionRecord]:
    text_l = text.strip().lower()
    filtered: list[FunctionRecord] = []
    for record in records:
        if schema != "(все)" and record.schema_name != schema:
            continue
        if text_l and text_l not in record.qualified_name.lower():
            continue
        if status != "all":
            item = compare_map.get(record.qualified_name)
            if item is None or item.status != status:
                continue
        filtered.append(record)
    return filtered


def _records_to_frame(
    records: list[FunctionRecord],
    compare_map: dict[str, FunctionCompareResult],
    *,
    has_compare: bool,
    picked_names: set[str] | None,
) -> pd.DataFrame:
    rows: list[dict] = []
    for record in records:
        row: dict = {}
        if picked_names is not None:
            row["export"] = record.qualified_name in picked_names
        row["function"] = record.qualified_name
        row["v"] = record.version_id
        row["updated"] = (record.rowversion or "—")[:16]
        row["user"] = record.pg_user or "—"
        if has_compare:
            item = compare_map.get(record.qualified_name)
            row["prod"] = _STATUS_LABEL.get(item.status, "—") if item else "—"
        rows.append(row)

    cols: list[str] = []
    if picked_names is not None:
        cols.append("export")
    cols += ["function", "v", "updated", "user"]
    if has_compare:
        cols.append("prod")
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows, columns=cols)


def _read_grid_rows(grid_key: str) -> list[int]:
    state = st.session_state.get(grid_key, {})
    selection = state.get("selection", {}) if isinstance(state, dict) else {}
    rows = selection.get("rows", [])
    return [int(i) for i in rows if isinstance(i, int)]


@st.dialog("DDL функции", width="large")
def _ddl_modal(
    record: FunctionRecord,
    compare_map: dict[str, FunctionCompareResult],
) -> None:
    item = compare_map.get(record.qualified_name)
    parts = [f"`{record.qualified_name}`", f"v{record.version_id}"]
    if record.rowversion:
        parts.append(record.rowversion[:16])
    if record.pg_user:
        parts.append(f"by {record.pg_user}")
    if item:
        parts.append(compare_status_label(item.status))
    st.caption(" · ".join(parts))

    show_diff = bool(item and item.prod and item.status == "changed")
    if compare_map and show_diff:
        tabs = st.tabs(["Diff с PROD", "DDL"])
        diff_tab, ddl_tab = tabs[0], tabs[1]
    elif compare_map:
        tabs = st.tabs(["DDL", "Diff с PROD"])
        ddl_tab, diff_tab = tabs[0], tabs[1]
    else:
        ddl_tab = st.container()
        diff_tab = None

    with ddl_tab:
        st.code(normalize_ddl(record.source_code), language="sql")

    if diff_tab is not None:
        with diff_tab:
            if show_diff:
                diff_text = compute_diff(item.prod.source_code, record.source_code)
                if diff_text:
                    st.code(diff_text, language="diff")
                else:
                    st.caption("Diff пустой (возможны отличия только в пробелах).")
            elif item and item.status == "same":
                st.success("Совпадает с PROD")
            elif item and item.status == "new":
                st.info("На PROD этой функции нет")
            else:
                st.caption("Сравнение с PROD недоступно.")


def render_function_list_panel(
    records: list[FunctionRecord],
    *,
    compare_results: list[FunctionCompareResult] | None = None,
    key_prefix: str = "fnlist",
    selectable: bool = False,
) -> list[FunctionRecord]:
    compare_map = _compare_by_name(compare_results)
    has_compare = bool(compare_map)
    schemas = sorted({r.schema_name for r in records})

    pick_key = f"{key_prefix}_export_pick"
    grid_key = f"{key_prefix}_grid"
    last_viewed_key = f"{key_prefix}_last_viewed"

    if has_compare:
        counts = {"new": 0, "changed": 0, "same": 0}
        for record in records:
            item = compare_map.get(record.qualified_name)
            if item:
                counts[item.status] += 1
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Всего", len(records))
        c2.metric("Нет на PROD", counts["new"])
        c3.metric("Отличается", counts["changed"])
        c4.metric("Совпадает", counts["same"])

    # --- Filters ---
    f1, f2 = st.columns([3, 1])
    with f1:
        text_filter = st.text_input(
            "Поиск",
            placeholder="schema.function или фрагмент имени",
            key=f"{key_prefix}_text",
            label_visibility="collapsed",
        )
    with f2:
        schema_filter = st.selectbox(
            "Схема",
            options=["(все)"] + schemas,
            key=f"{key_prefix}_schema",
            label_visibility="collapsed",
        )

    status_filter: StatusFilter = "all"
    if has_compare:
        status_filter = (
            st.segmented_control(
                "Статус",
                options=["all", "new", "changed", "same"],
                format_func=lambda s: _STATUS_SEGMENT[s],
                key=f"{key_prefix}_status",
                label_visibility="collapsed",
            )
            or "all"
        )

    filtered = _filter_records(
        records,
        compare_map,
        text=text_filter,
        schema=schema_filter,
        status=status_filter,
    )

    if not filtered:
        st.info("Нет функций по выбранным фильтрам.")
        return []

    # --- Export selection state (by name, survives filtering) ---
    all_names = {r.qualified_name for r in records}
    if pick_key not in st.session_state:
        st.session_state[pick_key] = [r.qualified_name for r in records]
    else:
        st.session_state[pick_key] = [
            n for n in st.session_state[pick_key] if n in all_names
        ]
    picked_set = set(st.session_state[pick_key])

    if not selectable:
        return _render_readonly_table(
            filtered, records, compare_map, has_compare, grid_key, last_viewed_key
        )

    # --- Bulk selection buttons ---
    visible_names = [r.qualified_name for r in filtered]
    b1, b2, b3, b4 = st.columns(4)
    if b1.button("✓ Все видимые", key=f"{key_prefix}_pick_all", use_container_width=True):
        st.session_state[pick_key] = sorted(picked_set | set(visible_names))
        st.rerun()
    if b2.button("✕ Снять видимые", key=f"{key_prefix}_pick_none", use_container_width=True):
        st.session_state[pick_key] = sorted(picked_set - set(visible_names))
        st.rerun()
    diff_disabled = not has_compare
    if b3.button(
        "⚡ Новые и ≠ PROD",
        key=f"{key_prefix}_pick_diff",
        use_container_width=True,
        disabled=diff_disabled,
        help="Отметить только функции, которых нет на PROD или которые отличаются",
    ):
        st.session_state[pick_key] = [
            r.qualified_name
            for r in filtered
            if (item := compare_map.get(r.qualified_name))
            and item.status in ("new", "changed")
        ]
        st.rerun()
    if b4.button("👁 Открыть DDL", key=f"{key_prefix}_open_ddl", use_container_width=True):
        view_name = st.session_state.get(f"{key_prefix}_view_select")
        target = next((r for r in filtered if r.qualified_name == view_name), filtered[0])
        _ddl_modal(target, compare_map)

    # --- Editable table (checkbox per row) ---
    df = _records_to_frame(
        filtered, compare_map, has_compare=has_compare, picked_names=picked_set
    )

    col_cfg: dict = {
        "export": st.column_config.CheckboxColumn("✓", width="small", help="В скрипт"),
        "function": st.column_config.TextColumn("Функция", width="large"),
        "v": st.column_config.NumberColumn("v", width="small"),
        "updated": st.column_config.TextColumn("Обновлено", width="medium"),
        "user": st.column_config.TextColumn("Автор", width="small"),
    }
    if has_compare:
        col_cfg["prod"] = st.column_config.TextColumn("PROD", width="medium")

    editor_key = f"{key_prefix}_editor_{abs(hash(tuple(visible_names)))}"
    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(52 + len(df) * 35, 520),
        column_config=col_cfg,
        disabled=["function", "v", "updated", "user", "prod"],
        num_rows="fixed",
        key=editor_key,
    )

    new_visible_picked = {
        name for name, flag in zip(edited["function"], edited["export"]) if flag
    }
    st.session_state[pick_key] = sorted(
        (picked_set - set(visible_names)) | new_visible_picked
    )
    picked_set = set(st.session_state[pick_key])
    visible_picked_count = len(new_visible_picked)

    # --- DDL preview control ---
    view_col, info_col = st.columns([3, 2])
    with view_col:
        st.selectbox(
            "Просмотр",
            options=visible_names,
            key=f"{key_prefix}_view_select",
            label_visibility="collapsed",
        )
    with info_col:
        st.markdown(
            f"<div style='padding-top:6px;color:#888'>"
            f"Отмечено: <b>{visible_picked_count}</b> из {len(filtered)} видимых · "
            f"всего <b>{len(picked_set)}</b></div>",
            unsafe_allow_html=True,
        )

    return [r for r in filtered if r.qualified_name in picked_set]


def _render_readonly_table(
    filtered: list[FunctionRecord],
    records: list[FunctionRecord],
    compare_map: dict[str, FunctionCompareResult],
    has_compare: bool,
    grid_key: str,
    last_viewed_key: str,
) -> list[FunctionRecord]:
    df = _records_to_frame(
        filtered, compare_map, has_compare=has_compare, picked_names=None
    )

    col_cfg: dict = {
        "function": st.column_config.TextColumn("Функция", width="large"),
        "v": st.column_config.NumberColumn("v", width="small"),
        "updated": st.column_config.TextColumn("Обновлено", width="medium"),
        "user": st.column_config.TextColumn("Автор", width="small"),
    }
    if has_compare:
        col_cfg["prod"] = st.column_config.TextColumn("PROD", width="medium")

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(52 + len(df) * 35, 520),
        column_config=col_cfg,
        key=grid_key,
        on_select="rerun",
        selection_mode="single-row",
    )
    st.caption(f"Показано **{len(filtered)}** из {len(records)} · нажмите строку для DDL")

    selected_rows = _read_grid_rows(grid_key)
    current_idx = selected_rows[0] if selected_rows else None
    last_idx = st.session_state.get(last_viewed_key)
    if current_idx is None:
        st.session_state.pop(last_viewed_key, None)
    elif current_idx != last_idx and 0 <= current_idx < len(filtered):
        st.session_state[last_viewed_key] = current_idx
        _ddl_modal(filtered[current_idx], compare_map)

    return []
