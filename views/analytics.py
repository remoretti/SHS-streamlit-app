from pygwalker.api.streamlit import StreamlitRenderer
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import pygwalker as pyg
from dotenv import load_dotenv
import os


# Load environment variables
load_dotenv()

# Database connection URL
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def fetch_table_data(table_name):
    """Fetch data from a given table."""
    query = f"""
        SELECT *
        FROM {table_name}
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query), conn)
        return result
    except Exception as e:
        st.error(f"Error fetching data from {table_name} table: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def analytics_page():
    """Analytics Page Logic."""
    st.title("Analytics Dashboard")
    
    # Fetch data selection
    st.subheader("Data Selection")
    table_choice = st.radio(
        "Select the data source for your visualization:",
        options=["All", "Cygnus", "Logiquip", "Summit Medical", "QuickBooks"],
        index=1,
    )

    # Determine the table to fetch based on selection
    if table_choice == "All":
        data_df = fetch_table_data("harmonised_table")
    elif table_choice == "Cygnus":
        data_df = fetch_table_data("master_cygnus_sales")
    elif table_choice == "Summit Medical":
        data_df = fetch_table_data("master_summit_medical_sales")
    elif table_choice == "QuickBooks":
        data_df = fetch_table_data("master_quickbooks_sales")
    else:  # Logiquip
        data_df = fetch_table_data("master_logiquip_sales")

    if data_df.empty:
        st.warning(f"No data available in the {table_choice} table.")
        return

    # Allow user to download the data as CSV
    csv_data = data_df.to_csv(index=False)
    st.download_button(
        label=f"Use {table_choice} data",
        data=csv_data,
        file_name=f"{table_choice.lower()}_data.csv",
        mime="text/csv",
    )

    # Display data visualization using PyGWalker (default Cygnus for now)
    st.subheader("Data Visualization with PyGWalker")
    if table_choice == "Cygnus":
        pyg_app = StreamlitRenderer(data_df)
        pyg_app.explorer()
    elif table_choice == "Logiquip":
        pyg_app = StreamlitRenderer(data_df)
        pyg_app.explorer()
    elif table_choice == "Summit Medical":
        pyg_app = StreamlitRenderer(data_df)
        pyg_app.explorer()
    elif table_choice == "QuickBooks":
        pyg_app = StreamlitRenderer(data_df)
        pyg_app.explorer()
    elif table_choice == "All":
        pyg_app = StreamlitRenderer(data_df)
        pyg_app.explorer()
    else:
        st.info("Select the desired data source.")

# Render the analytics page
analytics_page()