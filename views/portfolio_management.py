import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def fetch_table_data(table_name):
    """Fetch data from a PostgreSQL table."""
    query = f"SELECT * FROM {table_name};"
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def update_table_data(table_name, df):
    """Update the PostgreSQL table with the modified DataFrame."""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            with conn.begin():
                # Clear the existing table
                conn.execute(text(f"DELETE FROM {table_name};"))
                
                # Insert the updated data
                df.to_sql(table_name, con=engine, if_exists="append", index=False)
        st.success(f"Changes successfully saved to the {table_name} table!")
    except Exception as e:
        st.error(f"Error updating the {table_name} table: {e}")
    finally:
        engine.dispose()

# Streamlit UI
tab1, tab2, tab3 = st.tabs(["Service to Product", "Sales Reps", "Sales Territory"])

with tab1:
    st.header("Service-to-Product Mapping - QuickBooks")
    try:
        # Fetch data from the `service_to_product` table
        df_service_to_product = fetch_table_data("service_to_product")

        # Display editable DataFrame
        st.subheader("Service-to-Product Data (Editable)")
        edited_df = st.data_editor(
            df_service_to_product,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            key="service_to_product_editor",
            height=600
        )

        # Check for save initiation
        if "service_save_initiated" not in st.session_state:
            st.session_state.service_save_initiated = False

        # Save button
        if st.button("Save Changes", key="service_save_button"):
            st.session_state.service_save_initiated = True
            st.warning("Are you sure you want to replace the current table with the new data?")

        # Confirmation button (appears only if save was initiated)
        if st.session_state.service_save_initiated:
            if st.button("Yes, Replace Table", key="service_confirm_button"):
                update_table_data("service_to_product", edited_df)
                st.session_state.service_save_initiated = False

    except Exception as e:
        st.error(f"Error: {e}")

with tab2:
    st.header("Sales Reps Commission Tiers")
    try:
        # Fetch data from the `sales_rep_commission_tier` table
        df_commission = fetch_table_data("sales_rep_commission_tier")

        # Ensure column types for editable DataFrame
        df_commission["Commission tier 1 rate"] = df_commission["Commission tier 1 rate"].astype(float)
        df_commission["Commission tier 2 rate"] = df_commission["Commission tier 2 rate"].astype(float)

        # Display editable DataFrame
        st.subheader("Sales Reps Commission Data (Editable)")
        edited_df = st.data_editor(
            df_commission,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            key="commission_editor"
        )

        # Check for save initiation
        if "save_initiated" not in st.session_state:
            st.session_state.save_initiated = False

        # Save button
        if st.button("Save Commission Changes"):
            # Validate percentage values before proceeding
            if edited_df["Commission tier 1 rate"].between(0, 100).all() and edited_df["Commission tier 2 rate"].between(0, 100).all():
                st.session_state.save_initiated = True
                st.warning("Are you sure you want to replace the current table with the new data?")
            else:
                st.error("Commission rates must be valid percentages (0 to 100).")

        # Confirmation button (appears only if save was initiated)
        if st.session_state.save_initiated:
            if st.button("Yes, Replace Table"):
                update_table_data("sales_rep_commission_tier", edited_df)
                st.session_state.save_initiated = False

    except Exception as e:
        st.error(f"Error: {e}")

with tab3:
    st.header("Sales Territory")
    try:
        # Fetch data from the `master_sales_rep` table
        df_sales_rep = fetch_table_data("master_sales_rep")

        # Display editable DataFrame
        st.subheader("Master Sales Rep Data (Editable)")
        edited_df = st.data_editor(
            df_sales_rep,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            key="sales_rep_editor"
        )

        # Check for save initiation
        if "territory_save_initiated" not in st.session_state:
            st.session_state.territory_save_initiated = False

        # Save button
        if st.button("Save Changes"):
            st.session_state.territory_save_initiated = True
            st.warning("Are you sure you want to replace the current table with the new data?")

        # Confirmation button (appears only if save was initiated)
        if st.session_state.territory_save_initiated:
            if st.button("Yes, Replace Table"):
                update_table_data("master_sales_rep", edited_df)
                st.session_state.territory_save_initiated = False

    except Exception as e:
        st.error(f"Error: {e}")
