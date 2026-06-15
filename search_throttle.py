from __future__ import annotations

import time
from datetime import datetime

import streamlit as st

COOLDOWN_SECONDS = 5


def _mono_key(action: str) -> str:
    return f"search_last_mono_{action}"


def _wall_key(action: str) -> str:
    return f"search_last_wall_{action}"


def allow_search(action: str) -> bool:
    """False = cooldown active; shows warning in UI."""
    last = float(st.session_state.get(_mono_key(action), 0))
    if not last:
        return True
    elapsed = time.monotonic() - last
    if elapsed < COOLDOWN_SECONDS:
        wait = COOLDOWN_SECONDS - elapsed
        st.warning(
            f"Повторный запрос можно сделать через {wait:.0f} сек. "
            f"(защита от частых обращений к БД)."
        )
        return False
    return True


def record_search(action: str) -> int:
    """Mark search as executed; return cache-bust nonce."""
    st.session_state[_mono_key(action)] = time.monotonic()
    st.session_state[_wall_key(action)] = time.time()
    nonce = int(st.session_state.get("search_nonce", 0)) + 1
    st.session_state["search_nonce"] = nonce
    return nonce


def last_search_label(action: str) -> str | None:
    ts = st.session_state.get(_wall_key(action))
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts)).strftime("%d.%m.%Y %H:%M:%S")
