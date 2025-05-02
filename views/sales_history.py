import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import re

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

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
            
        def format_product_line_name(table_name):
            product_line = re.sub(r"^master_|_sales$", "", table_name)
            return f"{product_line.replace('_', ' ').title()} Commission Report"
            
        # Create the mapping dictionary
        product_line_map = {table: format_product_line_name(table) for table in table_names}
        
        # Sort the dictionary by the friendly names (values)
        sorted_product_line_map = dict(sorted(product_line_map.items(), key=lambda item: item[1]))
        
        return sorted_product_line_map
    except Exception as e:
        st.error(f"Error fetching product lines from tables: {e}")
        return {}
    finally:
        engine.dispose()

def fetch_table_data(product_line, selected_sales_reps):
    """Fetch sales data for the selected product line and sales reps."""
    table_name = product_line  # The selected table name
    # Use the correct column name based on your schema:
    sales_rep_column = "Sales Rep Name"
    
    # Check if selected_sales_reps list is empty
    if not selected_sales_reps:
        # Return an empty DataFrame without querying the database
        return pd.DataFrame()
    
    # Construct the filter query for sales reps.
    sales_rep_filter = " OR ".join([f'"{sales_rep_column}" = :rep{i}' for i in range(len(selected_sales_reps))])
    
    # Get the valid ordering column
    ordering_column = get_valid_ordering_column(table_name) or sales_rep_column
    
    # Construct the query with the WHERE clause only if there are sales reps to filter by
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
            # Check if the table exists and has any data
            table_check_query = text(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name)")
            table_exists = conn.execute(table_check_query, {"table_name": table_name}).scalar()
            
            if not table_exists:
                return pd.DataFrame()
                
            # Check if there's any data in the table
            data_check_query = text(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)")
            has_data = conn.execute(data_check_query).scalar()
            
            if not has_data:
                return pd.DataFrame()
            
            # Execute the main query if the table exists and has data
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            # Exclude specific columns if they exist.
            columns_to_exclude = ["row_hash", "SteppingStone", "Margin"]
            df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns], errors="ignore")
            return df
    except Exception as e:
        # More specific error handling for empty tables
        if "syntax error at or near" in str(e) and "WHERE ()" in str(e):
            # This is the empty WHERE clause error
            return pd.DataFrame()
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

# Fetch product lines with user-friendly names.
product_line_mapping = get_product_lines_with_friendly_names()
product_lines_display = list(product_line_mapping.values())

# Streamlit UI
st.title("Sales History")

col1, col2 = st.columns(2)

# --- Column 1: Sales Rep Selection ---
with col1:
    sales_reps = get_unique_sales_reps()
    # Apply restriction if the logged-in user is a simple user.
    if "user_permission" in st.session_state and st.session_state.user_permission.lower() == "user":
        user_name = st.session_state.user_name
        if user_name in sales_reps:
            sales_reps = ["All", user_name]
        else:
            sales_reps = ["All"]
    else:
        sales_reps.insert(0, "All")  # For Admins, include full list.
    
    selected_sales_rep = st.selectbox("Select Sales Rep:", sales_reps)

# --- Column 2: Product Line Selection (with friendly names) ---
with col2:
    selected_product_line_display = st.selectbox("Select Data Source:", product_lines_display)

# Convert user-friendly name back to table name.
selected_product_line = next((k for k, v in product_line_mapping.items() if v == selected_product_line_display), None)

# --- Data Filtering ---
if selected_sales_rep and selected_product_line:
    if selected_sales_rep == "All":
        selected_sales_reps = sales_reps[1:]  # Exclude "All"
    else:
        selected_sales_reps = [selected_sales_rep]
    
    # Fetch filtered data.
    sales_history_df = fetch_table_data(selected_product_line, selected_sales_reps)
    
    # Format date columns if present.
    if not sales_history_df.empty and "Date YYYY" in sales_history_df.columns:
        sales_history_df["Date YYYY"] = sales_history_df["Date YYYY"].apply(lambda x: str(int(x)))
    if not sales_history_df.empty and "Date MM" in sales_history_df.columns:
        sales_history_df["Date MM"] = sales_history_df["Date MM"].apply(lambda x: f"{int(x):02d}")
    
    # Display the DataFrame.
    if not sales_history_df.empty:
        # Convert "Num" column to plain text if the selected data source is QuickBooks Commission Report.
        if selected_product_line_display.lower().startswith("quickbooks"):
            if "Num" in sales_history_df.columns:
                sales_history_df["Num"] = sales_history_df["Num"].astype(str)
        st.subheader(f"Sales Data Source: {selected_product_line_display}")
        st.dataframe(sales_history_df, use_container_width=True, height=600, hide_index=True)
    else:
        st.info(f"No data available for the selected sales representative(s) in '{selected_product_line_display}'. The table may be empty or no matching records were found.")
