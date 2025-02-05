# import streamlit as st
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# import os
# import re

# # Load environment variables
# load_dotenv()
# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# def get_unique_sales_reps():
#     """Fetch distinct Sales Rep Names from the harmonised_table."""
#     query = """
#         SELECT DISTINCT TRIM("Sales Rep") AS "Sales Rep"
#         FROM harmonised_table
#         WHERE "Sales Rep" IS NOT NULL
#         ORDER BY TRIM("Sales Rep")
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query))
#             sales_reps = {row[0].strip() for row in result.fetchall() if row[0] is not None}
#         return sorted(sales_reps)
#     except Exception as e:
#         st.error(f"Error fetching Sales Reps: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_product_lines_from_tables():
#     """Dynamically fetch product lines by extracting them from table names."""
#     query = """
#         SELECT table_name 
#         FROM information_schema.tables 
#         WHERE table_name LIKE 'master\_%\_sales'
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query))
#             table_names = [row[0] for row in result.fetchall()]

#         # Extract product lines from table names (removing 'master_' and '_sales')
#         product_lines = [re.sub(r"^master_|_sales$", "", table) for table in table_names]
#         return sorted(product_lines)

#     except Exception as e:
#         st.error(f"Error fetching product lines from tables: {e}")
#         return []
#     finally:
#         engine.dispose()

# def fetch_table_data(product_line, selected_sales_reps):
#     """Fetch sales data for the selected product line and sales reps."""
#     table_name = f"master_{product_line}_sales"  # Updated dynamic table name
#     sales_rep_column = "Sales Rep Name"  # Updated column name

#     # Construct the filter query for sales reps
#     sales_rep_filter = " OR ".join([f'"{sales_rep_column}" = :rep{i}' for i in range(len(selected_sales_reps))])

#     # Determine the ordering column
#     ordering_column = get_valid_ordering_column(table_name) or sales_rep_column

#     query = f"""
#         SELECT *
#         FROM {table_name}
#         WHERE ({sales_rep_filter})
#         ORDER BY "{ordering_column}", "{sales_rep_column}"
#     """
#     params = {f"rep{i}": rep for i, rep in enumerate(selected_sales_reps)}

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query), params)
#             df = pd.DataFrame(result.fetchall(), columns=result.keys())

#         # Exclude specific columns if they exist
#         columns_to_exclude = ["row_hash", "SteppingStone"]
#         df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns], errors="ignore")

#         return df
#     except Exception as e:
#         st.error(f"Error fetching data from table '{table_name}': {e}")
#         return pd.DataFrame()
#     finally:
#         engine.dispose()

# def get_valid_ordering_column(table_name):
#     """Fetch the first valid date column from the table schema."""
#     possible_columns = ["ClosedDate", "Date Paid", "Invoice Date", "Date"]  # Possible date columns
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 0"))
#             columns = result.keys()
#             for col in possible_columns:
#                 if col in columns:
#                     return col
#         return None  # No valid column found
#     except Exception as e:
#         st.error(f"Error detecting valid ordering column for table '{table_name}': {e}")
#         return None
#     finally:
#         engine.dispose()

# # Streamlit UI
# st.title("Sales History")

# col1, col2 = st.columns(2)

# # Column 1: Sales Rep Selection
# with col1:
#     sales_reps = get_unique_sales_reps()
#     sales_reps.insert(0, "All")  # Add "All" option
#     selected_sales_rep = st.selectbox("Select Sales Rep:", sales_reps)

# # Column 2: Product Line Selection (now dynamically fetched)
# with col2:
#     product_lines = get_product_lines_from_tables()
#     selected_product_line = st.selectbox("Select a Product Line:", product_lines)

# # Filter Data
# if selected_sales_rep and selected_product_line:
#     if selected_sales_rep == "All":
#         selected_sales_reps = sales_reps[1:]  # Exclude "All"
#     else:
#         selected_sales_reps = [selected_sales_rep]

#     # Fetch filtered data from the selected table
#     sales_history_df = fetch_table_data(selected_product_line, selected_sales_reps)

#     # Display DataFrame
#     if not sales_history_df.empty:
#         st.subheader(f"Sales Data Source: {selected_product_line}")
#         st.dataframe(sales_history_df, use_container_width=True, height=600)
#     else:
#         st.warning(f"No data available for the selected criteria in table 'master_{selected_product_line}_sales'.")
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import re

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def get_unique_sales_reps():
    """Fetch distinct Sales Rep Names from the harmonised_table."""
    query = """
        SELECT DISTINCT TRIM("Sales Rep") AS "Sales Rep"
        FROM harmonised_table
        WHERE "Sales Rep" IS NOT NULL
        ORDER BY TRIM("Sales Rep")
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            sales_reps = {row[0].strip() for row in result.fetchall() if row[0] is not None}
        return sorted(sales_reps)
    except Exception as e:
        st.error(f"Error fetching Sales Reps: {e}")
        return []
    finally:
        engine.dispose()

def get_product_lines_with_friendly_names():
    """Fetch product lines and map them to user-friendly names."""
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'master\_%\_sales'
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            table_names = [row[0] for row in result.fetchall()]

        # Mapping function: Convert SQL table names to user-friendly names
        def format_product_line_name(table_name):
            product_line = re.sub(r"^master_|_sales$", "", table_name)  # Extract product line
            return f"{product_line.replace('_', ' ').title()} Commission Report"  # Format name

        # Create a dictionary mapping table names to display names
        return {table: format_product_line_name(table) for table in table_names}

    except Exception as e:
        st.error(f"Error fetching product lines from tables: {e}")
        return {}
    finally:
        engine.dispose()

def fetch_table_data(product_line, selected_sales_reps):
    """Fetch sales data for the selected product line and sales reps."""
    table_name = product_line  # The selected table name (no need for conversion)
    sales_rep_column = "Sales Rep Name"

    # Construct the filter query for sales reps
    sales_rep_filter = " OR ".join([f'"{sales_rep_column}" = :rep{i}' for i in range(len(selected_sales_reps))])

    # Determine the ordering column
    ordering_column = get_valid_ordering_column(table_name) or sales_rep_column

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE ({sales_rep_filter})
        ORDER BY "{ordering_column}", "{sales_rep_column}"
    """
    params = {f"rep{i}": rep for i, rep in enumerate(selected_sales_reps)}

    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Exclude specific columns if they exist
        columns_to_exclude = ["row_hash", "SteppingStone"]
        df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns], errors="ignore")

        return df
    except Exception as e:
        st.error(f"Error fetching data from table '{table_name}': {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def get_valid_ordering_column(table_name):
    """Fetch the first valid date column from the table schema."""
    possible_columns = ["ClosedDate", "Date Paid", "Invoice Date", "Date"]
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 0"))
            columns = result.keys()
            for col in possible_columns:
                if col in columns:
                    return col
        return None
    except Exception as e:
        st.error(f"Error detecting valid ordering column for table '{table_name}': {e}")
        return None
    finally:
        engine.dispose()

# Fetch product lines with user-friendly names
product_line_mapping = get_product_lines_with_friendly_names()
product_lines_display = list(product_line_mapping.values())  # Display names for the dropdown

# Streamlit UI
st.title("Sales History")

col1, col2 = st.columns(2)

# Column 1: Sales Rep Selection
with col1:
    sales_reps = get_unique_sales_reps()
    sales_reps.insert(0, "All")  # Add "All" option
    selected_sales_rep = st.selectbox("Select Sales Rep:", sales_reps)

# Column 2: Product Line Selection (Now with friendly names)
with col2:
    selected_product_line_display = st.selectbox("Select Data Source:", product_lines_display)

# Convert user-friendly name back to table name for queries
selected_product_line = next((k for k, v in product_line_mapping.items() if v == selected_product_line_display), None)

# Filter Data
if selected_sales_rep and selected_product_line:
    if selected_sales_rep == "All":
        selected_sales_reps = sales_reps[1:]  # Exclude "All"
    else:
        selected_sales_reps = [selected_sales_rep]

    # Fetch filtered data from the selected table
    sales_history_df = fetch_table_data(selected_product_line, selected_sales_reps)

    # Display DataFrame
    if not sales_history_df.empty:
        st.subheader(f"Sales Data Source: {selected_product_line_display}")  # Show user-friendly name
        st.dataframe(sales_history_df, use_container_width=True, height=600)
    else:
        st.warning(f"No data available for the selected criteria in '{selected_product_line_display}'.")
