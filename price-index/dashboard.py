import streamlit as st
from ffi import load_data, get_symol_df, Symbols
from streamlit_autorefresh import st_autorefresh

shm_path = ".ticker.data"
data = load_data(shm_path)


symbol_to_value = {
    sym.name: sym.value
    for sym in
    Symbols
}
    

def main():
    refresh_interval = st.sidebar.number_input(
        "Auto-Refresh Interval (seconds/10)", min_value=5, max_value=600, value=10, step=1
    ) * 100  # Convert to milliseconds
    st.markdown("""
        <style>
        stStatusWidget{display:none!important;}
        </style>
    """,unsafe_allow_html=True)
    st_autorefresh(interval=refresh_interval, limit=None)

    st.title("Dashboard")

    # choose symbol index
    symbol_name = st.sidebar.selectbox("Select Symbol Index", options=[sym.name for sym in Symbols], index=0)
    st.subheader(f"{symbol_name}")

    df = get_symol_df(data, symbol_to_value[symbol_name])
    st.dataframe(df)


if __name__ == "__main__":
    main()
