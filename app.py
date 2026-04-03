import streamlit as st

from search_schema import load_schema, search_and_format


@st.cache_resource
def cached_schema() -> dict:
    return load_schema()


def main() -> None:
    st.set_page_config(page_title="Tables explorer", layout="wide")
    st.title("Tables explorer")

    schema = cached_schema()

    with st.sidebar:
        st.header("Options")
        fuzzy = st.checkbox("Fuzzy", value=False)
        fk = st.checkbox("Expand FK", value=True)
        pretty = st.checkbox("Pretty", value=True)
        depth = st.number_input("FK depth", min_value=1, max_value=20, value=1, step=1)

    with st.form("search_form"):
        query = st.text_input("Table name or query", "")
        submitted = st.form_submit_button("Search")

    if submitted:
        q = query.strip()
        if not q:
            st.warning("Enter a query.")
        else:
            result = search_and_format(
                q,
                schema,
                fuzzy=fuzzy,
                fk=fk,
                depth=int(depth),
                pretty=pretty,
            )
            st.code(result, language=None)


if __name__ == "__main__":
    main()
