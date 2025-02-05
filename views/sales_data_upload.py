import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import tempfile

# Import your existing loaders
from data_loaders.cygnus.cygnus_loader import load_excel_file_cygnus
from data_loaders.logiquip.logiquip_loader import load_excel_file_logiquip
from data_loaders.summit_medical.summit_medical_loader import load_pdf_file_summit_medical
from data_loaders.quickbooks.quickbooks_loader import load_excel_file_quickbooks

# Import your existing db_utils
from data_loaders.cygnus.cygnus_db_utils import save_dataframe_to_db as save_cygnus_to_db
from data_loaders.logiquip.logiquip_db_utils import save_dataframe_to_db as save_logiquip_to_db
from data_loaders.summit_medical.summit_medical_db_utils import save_dataframe_to_db as save_summit_medical_to_db
from data_loaders.quickbooks.quickbooks_db_utils import save_dataframe_to_db as save_quickbooks_to_db

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine


FILE_TYPES = {
    "Logiquip": "Logiquip",
    "Cygnus": "Cygnus",
    "Summit Medical": "Summit Medical",
    "QuickBooks": "QuickBooks",
}

def load_excel_file(filepath: str, file_type: str, debug_info: list) -> pd.DataFrame:
    """
    Generic dispatcher to the correct loader function, or a direct read_excel if none matched.
    `filepath` is the path to a temporary file on disk.
    """
    if file_type == "Cygnus":
        return load_excel_file_cygnus(filepath)
    elif file_type == "Logiquip":
        return load_excel_file_logiquip(filepath)
    elif file_type == "Summit Medical":
        # Should not reach here if we handle PDF logic separately, but let's keep it for safety.
        return load_pdf_file_summit_medical(filepath)
    elif file_type == "QuickBooks":
        return load_excel_file_quickbooks(filepath)
    else:
        return pd.read_excel(filepath)

def check_for_blanks_with_details(df: pd.DataFrame) -> list:
    """
    Return a list of (row_number, [columns_with_blanks]) for any row that has blank cells.
    """
    blank_details = []
    for row_idx, row in df.iterrows():
        blank_columns = row[row.isnull() | (row == "")].index.tolist()
        if blank_columns:
            blank_details.append((row_idx + 1, blank_columns))
    return blank_details

def check_for_amount_line_issues(df: pd.DataFrame) -> list:
    """
    Check for rows where 'Amount line' is ≤ 0 and return a list of problematic rows.
    """
    issues = []
    if 'Amount line' in df.columns:
        for row_idx, row in df.iterrows():
            if row['Amount line'] <= 0:
                issues.append(row_idx + 1)  # Add 1 to row index to make it human-readable
    return issues

def fetch_table_data(table_name: str) -> pd.DataFrame:
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

def update_table_data(table_name: str, df: pd.DataFrame):
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"DELETE FROM {table_name};"))
                df.to_sql(table_name, con=engine, if_exists="append", index=False)
        st.success(f"Changes successfully saved to the {table_name} table!")
    except Exception as e:
        st.error(f"Error updating the {table_name} table: {e}")
    finally:
        engine.dispose()

def sales_data_tab():
    st.title("Sales Data Upload Hub")

    # If user clicks "Upload a New File", we clear session state and rerun
    if st.button("Upload a New File"):
        st.session_state.clear()
        st.rerun()

    # Initialize a place to store processed dataframes if not present
    if "dataframes" not in st.session_state:
        st.session_state.dataframes = {}

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Step 1: Select Product Line")
        selected_file_type = st.selectbox(
            "Choose the product line:",
            list(FILE_TYPES.keys()),
            help="Select the product line you want to process."
        )
        # If user changes product line, drop the previously confirmed file
        if st.session_state.get("selected_file_type") != selected_file_type:
            st.session_state["selected_file_type"] = selected_file_type
            # Clear any previously confirmed file from session state
            st.session_state.pop("confirmed_file_bytes", None)
            st.session_state.pop("confirmed_file_name", None)
            st.session_state.pop("confirmed_file_type", None)

    with col2:
        st.subheader("Step 2: Upload a File to Process")
        uploaded_file = st.file_uploader("Upload a .xlsx or .pdf file:", type=["xlsx", "pdf"])

        if uploaded_file and st.button("Confirm File Selection"):
            # Read the entire file into memory once
            file_bytes = uploaded_file.read()

            # Store file metadata + bytes in session state
            st.session_state["confirmed_file_bytes"] = file_bytes
            st.session_state["confirmed_file_name"] = uploaded_file.name
            st.session_state["confirmed_file_type"] = uploaded_file.type

            st.success(f"File '{uploaded_file.name}' has been confirmed!")

    # Check if we have a confirmed file in session state
    if "confirmed_file_bytes" not in st.session_state:
        st.warning("Please upload and confirm a file to proceed.")
        return

    st.markdown("---")

    st.subheader("Step 3: Loaded and Enriched Data")

    file_type = st.session_state["selected_file_type"]
    file_name = st.session_state["confirmed_file_name"]
    mime_type = st.session_state["confirmed_file_type"]
    file_bytes = st.session_state["confirmed_file_bytes"]

    st.write(f"### Processing: {file_name} (Type: {file_type})")

    try:
        debug_info = []

        # --- Create a temporary file from the bytes in session state ---
        if file_type == "Summit Medical" and mime_type == "application/pdf":
            # This is a PDF => pass to the PDF loader
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(file_bytes)
                tmp_file_path = tmp_file.name

            try:
                df = load_pdf_file_summit_medical(tmp_file_path)
            finally:
                os.remove(tmp_file_path)  # clean up
        else:
            # This is presumably an Excel (xlsx) or some other type
            suffix = ".xlsx" if mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" else None
            # If the user uploaded a PDF for something else, or xlsx for something else, we still handle it
            extension = suffix if suffix else ".pdf"

            with tempfile.NamedTemporaryFile(delete=False, suffix=extension) as tmp_file:
                tmp_file.write(file_bytes)
                tmp_file_path = tmp_file.name

            try:
                df = load_excel_file(tmp_file_path, file_type, debug_info)
            finally:
                os.remove(tmp_file_path)

        # Check for 'Amount line ≤ 0' only for QuickBooks data
        amount_line_issues = []
        if file_type == "QuickBooks":
            amount_line_issues = check_for_amount_line_issues(df)
            if amount_line_issues:
                st.error("Some rows in the QuickBooks file have 'Amount line' ≤ 0. Please review them.")
                for row in amount_line_issues:
                    st.markdown(f"- **Row:** {row}")
                    
        # Use a unique key for the data_editor to avoid collisions
        unique_key = f"editor_{file_name}_{file_type}"
        edited_df = st.data_editor(
            df,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            key=unique_key
        )

        # Store the edited data frame in session state, keyed by file name
        st.session_state.dataframes[file_name] = (edited_df, file_type)

    except Exception as e:
        st.error(f"Error loading {file_name} of type {file_type}: {e}")
        return

    # Final step: Save to DB
    if st.button("Confirm and Save to Database"):
        if not st.session_state.dataframes:
            st.warning("No data available to save. Please upload and process files first.")
            return

        # Check if any row has blanks
        invalid_files = {}
        for f_name, (df_data, _) in st.session_state.dataframes.items():
            blank_details = check_for_blanks_with_details(df_data)
            if blank_details:
                invalid_files[f_name] = blank_details

        if invalid_files:
            st.error("Some files contain rows with blank values. Please fix them and try again.")
            for fname, row_col_details in invalid_files.items():
                for row, cols in row_col_details:
                    st.markdown(f"- **File:** {fname} | **Row:** {row} | **Columns:** {', '.join(cols)}")
            return

        debug_output = []
        # If all checks pass, let's proceed to save each dataframe
        for f_name, (df_data, f_type) in st.session_state.dataframes.items():
            try:
                if f_type == "Cygnus":
                    debug_output.extend(save_cygnus_to_db(df_data, "master_cygnus_sales"))
                elif f_type == "Logiquip":
                    debug_output.extend(save_logiquip_to_db(df_data, "master_logiquip_sales"))
                elif f_type == "Summit Medical":
                    debug_output.extend(save_summit_medical_to_db(df_data, "master_summit_medical_sales"))
                elif f_type == "QuickBooks":
                    debug_output.extend(save_quickbooks_to_db(df_data, "master_quickbooks_sales"))
                # (Add similar lines for other product lines if needed)
                st.success(f"Data from '{f_name}' successfully saved to the '{f_type}' table.")
            except Exception as e:
                st.error(f"Error saving '{f_name}' to the database: {e}")

        if debug_output:
            st.markdown("### Debug Log")
            for message in debug_output:
                st.markdown(f"- {message}")
        else:
            st.info("No debug messages to display.")

def data_upload_status_tab():
    st.title("Data Upload Status")

    data_status = fetch_table_data("data_status")
    if data_status.empty:
        st.warning("No data available in the data_status table. Initializing with default structure.")
        data_status = pd.DataFrame({
            "Product line": pd.Series(dtype='str'),
            "January": pd.Series(dtype='bool'),
            "February": pd.Series(dtype='bool'),
            "March": pd.Series(dtype='bool'),
            "April": pd.Series(dtype='bool'),
            "May": pd.Series(dtype='bool'),
            "June": pd.Series(dtype='bool'),
            "July": pd.Series(dtype='bool'),
            "August": pd.Series(dtype='bool'),
            "September": pd.Series(dtype='bool'),
            "October": pd.Series(dtype='bool'),
            "November": pd.Series(dtype='bool'),
            "December": pd.Series(dtype='bool')
        })

    st.subheader("Data Status (Editable)")
    boolean_columns = [col for col in data_status.columns if col != "Product line"]
    # Fill missing booleans with False
    for col in boolean_columns:
        data_status[col] = data_status[col].fillna(False).astype(bool)
    data_status["Product line"] = data_status["Product line"].astype(str)

    edited_data = st.data_editor(
        data_status,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="data_status_editor"
    )

    if "save_initiated" not in st.session_state:
        st.session_state.save_initiated = False

    if st.button("Confirm and Upload to Database"):
        st.session_state.save_initiated = True
        st.warning("Are you sure you want to replace the current data with the new changes?")

    if st.session_state.save_initiated:
        if st.button("Yes, Replace Table"):
            # Ensure booleans are indeed booleans
            for col in boolean_columns:
                edited_data[col] = edited_data[col].astype(bool)
            update_table_data("data_status", edited_data)
            st.session_state.save_initiated = False

# Create two tabs in the UI
tab1, tab2 = st.tabs(["Sales Data Upload", "Data Upload Status"])

with tab1:
    sales_data_tab()

with tab2:
    data_upload_status_tab()
