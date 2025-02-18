import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def fetch_table_data(table_name):
    """Fetch data from the specified PostgreSQL table."""
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
                # Clear the existing table data.
                conn.execute(text(f"DELETE FROM {table_name};"))
                # Insert the updated data.
                df.to_sql(table_name, con=engine, if_exists="append", index=False)
        st.success(f"Changes successfully saved to the {table_name} table!")
    except Exception as e:
        st.error(f"Error updating the {table_name} table: {e}")
    finally:
        engine.dispose()

def render_preview_table(df, css_class=""):
    """Render a DataFrame as an HTML table without showing the index."""
    html_table = df.reset_index(drop=True).to_html(index=False, classes=css_class)
    st.markdown(html_table, unsafe_allow_html=True)

# ---------------------------------
# Session State Initialization
# ---------------------------------
if "access_editing" not in st.session_state:
    st.session_state.access_editing = False
if "access_save_initiated" not in st.session_state:
    st.session_state.access_save_initiated = False
if "loaded_access_df" not in st.session_state:
    st.session_state.loaded_access_df = None

# ---------------------------------
# User Account Administration Page
# ---------------------------------
st.title("User Account Administration")
st.write("Manage app access levels, user names, and passwords.")

# READ-ONLY MODE: Display current data if not editing.
if not st.session_state.access_editing:
    st.subheader("Current User Accounts (Read-Only)")
    df_access = fetch_table_data("master_access_level")
    st.dataframe(df_access, use_container_width=True, hide_index=True)
    if st.button("Edit Data"):
        st.session_state.access_editing = True
        st.rerun()
else:
    # EDITING MODE: Allow admins to modify the table.
    st.subheader("Edit User Accounts")
    
    # Optional: Allow file upload to load new data.
    uploaded_file_access = st.file_uploader("Upload an .xlsx file to load user account data", type=["xlsx"], key="access_file_uploader")
    if uploaded_file_access is not None:
        if st.button("Load from File", key="access_load_file_button"):
            try:
                df_from_file = pd.read_excel(uploaded_file_access)
                st.session_state.loaded_access_df = df_from_file
                st.success("File loaded successfully. You can now edit the data below.")
            except Exception as e:
                st.error(f"Error loading file: {e}")
    
    # Determine which DataFrame to show in the editor:
    base_df = st.session_state.loaded_access_df if st.session_state.loaded_access_df is not None else fetch_table_data("master_access_level")
    
    # # Optional: Reorder columns for consistency.
    # desired_columns = ["Sales Rep Name", "Password", "Permission"]
    # if set(desired_columns).issubset(base_df.columns):
    #     base_df = base_df[desired_columns]
    # Optional: Reorder columns for consistency.
    desired_columns = ["Sales Rep Name", "Email", "Password", "Permission"]
    if set(desired_columns).issubset(base_df.columns):
        base_df = base_df[desired_columns]
    # Define a column configuration for the editable DataFrame:
    col_config = {
        "Permission": st.column_config.SelectboxColumn(
            "Permission",
            options=["admin", "user"],
            help="Select a permission level: Admin or User"
        )
    }
    edited_df = st.data_editor(
        base_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="access_editor",
        column_config=col_config
    )
    
    # Confirmation logic for saving changes.
    if st.button("Save Changes"):
        st.session_state.access_save_initiated = True
        st.warning("Are you sure you want to replace the current table with the new data?")
    
    if st.session_state.access_save_initiated:
        if st.button("Yes, Replace Table", key="access_confirm_button"):
            update_table_data("master_access_level", edited_df)
            st.session_state.access_save_initiated = False
            st.session_state.loaded_access_df = None
            st.session_state.access_editing = False
            st.rerun()
    
    if st.button("Cancel Editing", key="access_cancel_button"):
        st.session_state.access_editing = False
        st.session_state.loaded_access_df = None
        st.rerun()
