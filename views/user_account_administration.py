# # import streamlit as st
# # import pandas as pd
# # from sqlalchemy import create_engine, text
# # from dotenv import load_dotenv
# # import os

# # # Load environment variables
# # load_dotenv()

# # DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# # def get_db_connection():
# #     """Create a database connection."""
# #     engine = create_engine(DATABASE_URL)
# #     return engine

# # def fetch_table_data(table_name):
# #     """Fetch data from a PostgreSQL table."""
# #     query = f"SELECT * FROM {table_name};"
# #     engine = get_db_connection()
# #     try:
# #         with engine.connect() as conn:
# #             result = conn.execute(text(query))
# #             df = pd.DataFrame(result.fetchall(), columns=result.keys())
# #         return df
# #     except Exception as e:
# #         st.error(f"Error fetching data from {table_name}: {e}")
# #         return pd.DataFrame()
# #     finally:
# #         engine.dispose()

# # def update_table_data(table_name, df):
# #     """Update the PostgreSQL table with the modified DataFrame."""
# #     engine = get_db_connection()
# #     try:
# #         with engine.connect() as conn:
# #             with conn.begin():
# #                 # Clear the existing table
# #                 conn.execute(text(f"DELETE FROM {table_name};"))
                
# #                 # Insert the updated data
# #                 df.to_sql(table_name, con=engine, if_exists="append", index=False)
# #         st.success(f"Changes successfully saved to the {table_name} table!")
# #     except Exception as e:
# #         st.error(f"Error updating the {table_name} table: {e}")
# #     finally:
# #         engine.dispose()

# # def user_account_administration_page():
# #     """User Account Administration Page Logic."""
# #     st.title("User Account Administration")

# #     # Fetch data from the table
# #     data_status = fetch_table_data("data_status")

# #     # If the table is empty, create an empty DataFrame with the required structure
# #     if data_status.empty:
# #         st.warning("No data available in the data_status table. Initializing with default structure.")
# #         data_status = pd.DataFrame({
# #             "Product line": pd.Series(dtype='str'),
# #             "January": pd.Series(dtype='bool'),
# #             "February": pd.Series(dtype='bool'),
# #             "March": pd.Series(dtype='bool'),
# #             "April": pd.Series(dtype='bool'),
# #             "May": pd.Series(dtype='bool'),
# #             "June": pd.Series(dtype='bool'),
# #             "July": pd.Series(dtype='bool'),
# #             "August": pd.Series(dtype='bool'),
# #             "September": pd.Series(dtype='bool'),
# #             "October": pd.Series(dtype='bool'),
# #             "November": pd.Series(dtype='bool'),
# #             "December": pd.Series(dtype='bool')
# #         })

# #     # Editable DataFrame
# #     st.subheader("Data Status (Editable)")

# #     # Convert boolean columns to checkbox-style DataFrame
# #     boolean_columns = [col for col in data_status.columns if col != "Product line"]
# #     for col in boolean_columns:
# #         data_status[col] = data_status[col].fillna(False).astype(bool)

# #     # Ensure "Product line" is treated as text
# #     data_status["Product line"] = data_status["Product line"].astype(str)

# #     # Display editable DataFrame
# #     edited_data = st.data_editor(
# #         data_status,
# #         use_container_width=True,
# #         num_rows="dynamic",
# #         hide_index=True,
# #         key="data_status_editor",
# #     )

# #     # Double-check confirmation logic
# #     if "save_initiated" not in st.session_state:
# #         st.session_state.save_initiated = False

# #     # Save button
# #     if st.button("Confirm and Upload to Database"):
# #         st.session_state.save_initiated = True
# #         st.warning("Are you sure you want to replace the current data with the new changes?")

# #     # Confirmation button (appears only if save was initiated)
# #     if st.session_state.save_initiated:
# #         if st.button("Yes, Replace Table"):
# #             # Convert checkbox values back to boolean for SQL storage
# #             for col in boolean_columns:
# #                 edited_data[col] = edited_data[col].astype(bool)
# #             update_table_data("data_status", edited_data)
# #             st.session_state.save_initiated = False  # Reset state after save

# # # Render the page
# # user_account_administration_page()
# import streamlit as st
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# import os
# from data_loaders.cygnus.cygnus_loader import load_excel_file_cygnus
# from data_loaders.logiquip.logiquip_loader import load_excel_file_logiquip
# from data_loaders.mmm_yyyy_shsi import load_excel_file_mmm_yyyy_shsi
# from data_loaders.dd_month_2024_commission import load_excel_file_dd_month_2024_commission
# from data_loaders.dd_month_2024_commission_to_be_paid import load_excel_file_dd_month_2024_commission_to_be_paid
# from data_loaders.from_pdf import load_excel_file_from_pdf
# from data_loaders.qbs import load_excel_file_qbs
# from data_loaders.cygnus.cygnus_db_utils import save_dataframe_to_db as save_cygnus_to_db
# from data_loaders.logiquip.logiquip_db_utils import save_dataframe_to_db as save_logiquip_to_db

# # Load environment variables
# load_dotenv()

# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# FILE_TYPES = {
#     "Logiquip": "Logiquip",
#     "Cygnus": "Cygnus",
#     "MMMYYYY_SHSI": "MMMYYYY_SHSI",
#     "Sunoptic DD Month 2024 Commission": "DD Month 2024 Commission",
#     "Sunoptic Month 2024 Commission to be paid": "Month 2024 Commission to be paid",
#     "Summit Medical From PDF": "From PDF",
#     "QuickBooks": "QuickBooks",
# }

# def load_excel_file(filepath: str, file_type: str, debug_info: list) -> pd.DataFrame:
#     if file_type == "Cygnus":
#         return load_excel_file_cygnus(filepath)
#     elif file_type == "Logiquip":
#         return load_excel_file_logiquip(filepath)
#     elif file_type == "MMMYYYY_SHSI":
#         return load_excel_file_mmm_yyyy_shsi(filepath)
#     elif file_type == "DD Month 2024 Commission":
#         return load_excel_file_dd_month_2024_commission(filepath, debug_info)
#     elif file_type == "Month 2024 Commission to be paid":
#         return load_excel_file_dd_month_2024_commission_to_be_paid(filepath, debug_info)
#     elif file_type == "From PDF":
#         return load_excel_file_from_pdf(filepath)
#     elif file_type == "QuickBooks":
#         return load_excel_file_qbs(filepath, sheet_name="Sheet1")
#     else:
#         return pd.read_excel(filepath)

# def check_for_blanks_with_details(df: pd.DataFrame) -> list:
#     blank_details = []
#     for row_idx, row in df.iterrows():
#         blank_columns = row[row.isnull() | (row == "")].index.tolist()
#         if blank_columns:
#             blank_details.append((row_idx + 1, blank_columns))
#     return blank_details

# def fetch_table_data(table_name):
#     query = f"SELECT * FROM {table_name};"
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query))
#             df = pd.DataFrame(result.fetchall(), columns=result.keys())
#         return df
#     except Exception as e:
#         st.error(f"Error fetching data from {table_name}: {e}")
#         return pd.DataFrame()
#     finally:
#         engine.dispose()

# def update_table_data(table_name, df):
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             with conn.begin():
#                 conn.execute(text(f"DELETE FROM {table_name};"))
#                 df.to_sql(table_name, con=engine, if_exists="append", index=False)
#         st.success(f"Changes successfully saved to the {table_name} table!")
#     except Exception as e:
#         st.error(f"Error updating the {table_name} table: {e}")
#     finally:
#         engine.dispose()

# def sales_data_tab():
#     st.title("Sales Data Upload Hub")

#     if st.button("Upload a New File"):
#         st.session_state.clear()
#         st.rerun()

#     if 'dataframes' not in st.session_state:
#         st.session_state.dataframes = {}

#     col1, col2 = st.columns(2)

#     with col1:
#         st.subheader("Step 1: Select Product Line")
#         selected_file_type = st.selectbox(
#             "Choose the product line:",
#             list(FILE_TYPES.keys()),
#             help="Select the product line you want to process."
#         )
#         if st.session_state.get("selected_file_type") != selected_file_type:
#             st.session_state["selected_file_type"] = selected_file_type
#             st.session_state.pop("confirmed_file", None)

#     with col2:
#         st.subheader("Step 2: Upload a File to Process")
#         uploaded_file = st.file_uploader("Upload a .xlsx file:", type="xlsx")
#         if uploaded_file and st.button("Confirm File Selection"):
#             st.session_state["confirmed_file"] = uploaded_file
#             st.success(f"File '{uploaded_file.name}' has been confirmed!")

#     if "confirmed_file" not in st.session_state:
#         st.warning("Please upload and confirm a file to proceed.")
#         return

#     st.markdown("---")

#     st.subheader("Step 3: Loaded and Enriched Data")
#     confirmed_file = st.session_state["confirmed_file"]
#     file_type = st.session_state["selected_file_type"]

#     st.write(f"### Processing: {confirmed_file.name} (Type: {file_type})")

#     try:
#         debug_info = []
#         df = load_excel_file(confirmed_file, file_type, debug_info)
#         unique_key = f"editor_{confirmed_file.name}_{file_type}"
#         edited_df = st.data_editor(df, use_container_width=True, num_rows="dynamic", hide_index=True, key=unique_key)
#         st.session_state.dataframes[confirmed_file.name] = (edited_df, file_type)
#     except Exception as e:
#         st.error(f"Error loading {confirmed_file.name} of type {file_type}: {e}")
#         return

#     if st.button("Confirm and Save to Database"):
#         if not st.session_state.dataframes:
#             st.warning("No data available to save. Please upload and process files first.")
#             return

#         invalid_files = {}
#         for file_name, (df, _) in st.session_state.dataframes.items():
#             blank_details = check_for_blanks_with_details(df)
#             if blank_details:
#                 invalid_files[file_name] = blank_details

#         if invalid_files:
#             st.error("Some files contain rows with blank values. Please fix them and try again.")
#             for file_name, row_col_details in invalid_files.items():
#                 for row, cols in row_col_details:
#                     st.markdown(f"- **Row:** {row}, **Columns:** {', '.join(cols)}")
#             return

#         debug_output = []
#         for file_name, (df, file_type) in st.session_state.dataframes.items():
#             try:
#                 if file_type == "Cygnus":
#                     debug_output.extend(save_cygnus_to_db(df, file_type))
#                 elif file_type == "Logiquip":
#                     debug_output.extend(save_logiquip_to_db(df, file_type))
#                 st.success(f"Data from '{file_name}' successfully saved to the '{file_type}' table.")
#             except Exception as e:
#                 st.error(f"Error saving '{file_name}' to the database: {e}")

#         if debug_output:
#             st.markdown("### Debug Log")
#             for message in debug_output:
#                 st.markdown(f"- {message}")
#         else:
#             st.info("No debug messages to display.")

# def data_upload_status_tab():
#     st.title("Data Upload Status")
#     data_status = fetch_table_data("data_status")

#     if data_status.empty:
#         st.warning("No data available in the data_status table. Initializing with default structure.")
#         data_status = pd.DataFrame({
#             "Product line": pd.Series(dtype='str'),
#             "January": pd.Series(dtype='bool'),
#             "February": pd.Series(dtype='bool'),
#             "March": pd.Series(dtype='bool'),
#             "April": pd.Series(dtype='bool'),
#             "May": pd.Series(dtype='bool'),
#             "June": pd.Series(dtype='bool'),
#             "July": pd.Series(dtype='bool'),
#             "August": pd.Series(dtype='bool'),
#             "September": pd.Series(dtype='bool'),
#             "October": pd.Series(dtype='bool'),
#             "November": pd.Series(dtype='bool'),
#             "December": pd.Series(dtype='bool')
#         })

#     st.subheader("Data Status (Editable)")
#     boolean_columns = [col for col in data_status.columns if col != "Product line"]
#     for col in boolean_columns:
#         data_status[col] = data_status[col].fillna(False).astype(bool)
#     data_status["Product line"] = data_status["Product line"].astype(str)

#     edited_data = st.data_editor(
#         data_status, use_container_width=True, num_rows="dynamic", hide_index=True, key="data_status_editor")

#     if "save_initiated" not in st.session_state:
#         st.session_state.save_initiated = False

#     if st.button("Confirm and Upload to Database"):
#         st.session_state.save_initiated = True
#         st.warning("Are you sure you want to replace the current data with the new changes?")

#     if st.session_state.save_initiated:
#         if st.button("Yes, Replace Table"):
#             for col in boolean_columns:
#                 edited_data[col] = edited_data[col].astype(bool)
#             update_table_data("data_status", edited_data)
#             st.session_state.save_initiated = False

# tab1, tab2 = st.tabs(["Sales Data Upload", "Data Upload Status"])
# with tab1:
#     sales_data_tab()

# with tab2:
#     data_upload_status_tab()

