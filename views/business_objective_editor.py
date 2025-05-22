import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Increase the pandas styler limit
pd.set_option("styler.render.max_elements", 12000000)  # Set higher than 11,976,608

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def get_available_years():
    """Fetch distinct years from the harmonised_table."""
    query = """
        SELECT DISTINCT "Date YYYY" AS year
        FROM harmonised_table
        ORDER BY year
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            years = [row[0] for row in result.fetchall()]
        return years
    except Exception as e:
        st.error(f"Error fetching years: {e}")
        return []
    finally:
        engine.dispose()

def fetch_business_objective_data(selected_year):
    """Fetch and construct the business objective DataFrame with sub-totals."""
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # Fetch unique product lines and sales reps per product line with explicit deduplication
            sales_rep_query = """
                SELECT DISTINCT "Product line", "Sales Rep name"
                FROM sales_rep_business_objective
                WHERE "Year" = %s
                ORDER BY "Product line", "Sales Rep name"
            """
            sales_reps = pd.read_sql_query(sales_rep_query, conn, params=(selected_year,))
            
            # Ensure no duplicates in the base DataFrame
            sales_reps = sales_reps.drop_duplicates(subset=["Product line", "Sales Rep name"]).reset_index(drop=True)

            # Fetch commission tier thresholds
            commission_query = """
                SELECT "Product line", "Sales Rep name", "Commission tier threshold"
                FROM sales_rep_commission_tier_threshold
                WHERE "Year" = %s
            """
            commission_tiers = pd.read_sql_query(commission_query, conn, params=(selected_year,))
            
            # Ensure no duplicates in commission tiers
            commission_tiers = commission_tiers.drop_duplicates(subset=["Product line", "Sales Rep name"]).reset_index(drop=True)

            # Fetch monthly objectives - get all at once and pivot
            objective_query = """
                SELECT "Product line", "Sales Rep name", "Month", "Objective"
                FROM sales_rep_business_objective
                WHERE "Year" = %s
                ORDER BY "Product line", "Sales Rep name", "Month"
            """
            objectives = pd.read_sql_query(objective_query, conn, params=(selected_year,))

        # If there are no sales reps, return an empty DataFrame with all expected columns.
        if sales_reps.empty:
            columns = [
                "Product line", "Sales Rep name", "Commission tier threshold",
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December",
                "Annual Objective"
            ]
            return pd.DataFrame(columns=columns)

        # Start with the unique sales reps as the base
        full_df = sales_reps.copy()
        
        # Merge commission tiers
        full_df = full_df.merge(
            commission_tiers, 
            on=["Product line", "Sales Rep name"], 
            how="left"
        )
        
        # Pivot the objectives data to have months as columns
        if not objectives.empty:
            # Convert month numbers to month names for pivoting
            objectives['Month_Name'] = objectives['Month'].apply(
                lambda x: pd.to_datetime(f"2023-{x:02d}-01").strftime("%B")
            )
            
            # Pivot to get months as columns
            pivoted_objectives = objectives.pivot_table(
                index=["Product line", "Sales Rep name"],
                columns="Month_Name",
                values="Objective",
                aggfunc="first"  # Use first in case of duplicates
            ).reset_index()
            
            # Merge the pivoted objectives
            full_df = full_df.merge(
                pivoted_objectives,
                on=["Product line", "Sales Rep name"],
                how="left"
            )
        else:
            # If no objectives, add empty month columns
            for month in range(1, 13):
                month_name = pd.to_datetime(f"2023-{month:02d}-01").strftime("%B")
                full_df[month_name] = None

        # Ensure all month columns exist (in case some months are missing from the pivot)
        month_columns = ["January", "February", "March", "April", "May", "June",
                        "July", "August", "September", "October", "November", "December"]
        for month in month_columns:
            if month not in full_df.columns:
                full_df[month] = None

        # Calculate the Annual Objective (sum of all monthly objectives)
        full_df["Annual Objective"] = full_df[month_columns].sum(axis=1, skipna=True)

        # Final deduplication step to ensure no duplicates
        full_df = full_df.drop_duplicates(subset=["Product line", "Sales Rep name"]).reset_index(drop=True)

        # Format numeric columns with $ symbol
        numeric_columns = [
            "Commission tier threshold", "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December", "Annual Objective"
        ]
        for col in numeric_columns:
            if col in full_df.columns:
                full_df[col] = full_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")

        # Sort by Product line and Sales Rep name
        full_df = full_df.sort_values(by=["Product line", "Sales Rep name"]).reset_index(drop=True)

        # Add Sub-Total Rows
        rows_with_subtotals = []
        for product_line, group in full_df.groupby("Product line"):
            rows_with_subtotals.append(group)
            
            # Calculate sub-totals for numeric columns
            subtotal_data = {}
            for col in month_columns + ["Annual Objective"]:
                if col in group.columns:
                    # Convert currency strings back to numbers for calculation
                    numeric_values = group[col].apply(
                        lambda x: float(str(x).replace('$', '').replace(',', '')) if isinstance(x, str) and x.startswith('$') else (float(x) if pd.notnull(x) else 0)
                    )
                    subtotal_data[col] = f"${numeric_values.sum():,.2f}"
                else:
                    subtotal_data[col] = "$0.00"
            
            # Create subtotal row
            subtotal_row = pd.DataFrame([{
                "Product line": product_line,
                "Sales Rep name": "🔢 SUB-TOTAL",
                "Commission tier threshold": "",  # Leave this empty for subtotal rows
                **subtotal_data
            }])
            rows_with_subtotals.append(subtotal_row)

        # Combine all rows
        final_df = pd.concat(rows_with_subtotals, ignore_index=True)
        return final_df

    except Exception as e:
        st.error(f"Error fetching business objective data: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def update_business_objective_data(df, year):
    """Update the sales_rep_business_objective and sales_rep_commission_tier_threshold tables."""
    engine = get_db_connection()
    try:
        # Filter out "Sub-Total" rows (handle both old and new identifiers)
        filtered_df = df[
            (~df["Sales Rep name"].str.contains("Sub-Total", case=False, na=False)) & 
            (~df["Sales Rep name"].str.contains("SUB-TOTAL", case=False, na=False))
        ].copy()

        # Ensure no duplicates
        filtered_df = filtered_df.drop_duplicates()

        with engine.connect() as conn:
            with conn.begin():
                # Delete existing data for the year
                conn.execute(text("DELETE FROM sales_rep_business_objective WHERE \"Year\" = :year"), {"year": year})
                conn.execute(text("DELETE FROM sales_rep_commission_tier_threshold WHERE \"Year\" = :year"), {"year": year})

                # Prepare and insert sales_rep_business_objective data
                monthly_data = filtered_df.melt(
                    id_vars=["Product line", "Sales Rep name"], 
                    value_vars=["January", "February", "March", "April", "May", "June", 
                                "July", "August", "September", "October", "November", "December"],
                    var_name="Month", 
                    value_name="Objective"
                )
                monthly_data["Year"] = year
                monthly_data["Month"] = monthly_data["Month"].apply(lambda x: pd.to_datetime(x, format="%B").month)

                # Remove rows with null or invalid objectives
                monthly_data = monthly_data[~monthly_data["Objective"].isnull()]
                monthly_data["Objective"] = monthly_data["Objective"].apply(
                    lambda x: float(str(x).replace("$", "").replace(",", "").strip())
                )
                monthly_data.to_sql("sales_rep_business_objective", con=engine, if_exists="append", index=False)

                # Prepare and insert sales_rep_commission_tier_threshold data
                commission_data = filtered_df[["Product line", "Sales Rep name", "Commission tier threshold"]].copy()
                commission_data["Year"] = year
                commission_data["Commission tier threshold"] = commission_data["Commission tier threshold"].apply(
                    lambda x: float(str(x).replace("$", "").replace(",", "").strip())
                )
                commission_data.to_sql("sales_rep_commission_tier_threshold", con=engine, if_exists="append", index=False)

        st.success("Business objectives successfully updated!")
    except Exception as e:
        st.error(f"Error updating business objectives: {e}")
    finally:
        engine.dispose()

def remove_subtotals_for_editing(df):
    """Remove Sub-Total rows before making the DataFrame editable."""
    # Handle both old and new sub-total identifiers
    return df[
        (~df["Sales Rep name"].str.contains("Sub-Total", case=False, na=False)) & 
        (~df["Sales Rep name"].str.contains("SUB-TOTAL", case=False, na=False))
    ].reset_index(drop=True)

def get_unique_sales_reps_commission_tier():
    """Fetch distinct Sales Rep Names from the sales_rep_commission_tier table."""
    query = """
        SELECT DISTINCT "Sales Rep Name"
        FROM sales_rep_commission_tier
        ORDER BY "Sales Rep Name"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            sales_reps = [row[0] for row in result.fetchall()]
        return sales_reps
    except Exception as e:
        st.error(f"Error fetching Sales Reps from commission tier: {e}")
        return []
    finally:
        engine.dispose()

def get_unique_product_lines_service_to_product():
    """Fetch distinct Product Lines from the service_to_product table."""
    query = """
        SELECT DISTINCT "Product Lines"
        FROM service_to_product
        WHERE "Product Lines" IS NOT NULL
        ORDER BY "Product Lines"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            product_lines = [row[0] for row in result.fetchall()]
        return product_lines
    except Exception as e:
        st.error(f"Error fetching unique product lines from service_to_product: {e}")
        return []
    finally:
        engine.dispose()

# ----------------- Streamlit UI -----------------
st.title("Business Objective Editor")
col1, col2 = st.columns([1, 1])

# Hardcoded year
selected_year = "2025"
with col1:
    st.markdown(f"**Year:** {selected_year}")

df = fetch_business_objective_data(selected_year)

# Initialize editing state if not already set.
if "editing" not in st.session_state:
    st.session_state.editing = False

if not st.session_state.editing:
    if df.empty:
        st.warning(f"No data available for the selected year: {selected_year}.")
    else:
        # ============================================================================
        # Complete Overview Section - The main display
        # Provide a comprehensive view of all data with clear sub-total indicators
        # ============================================================================
        st.markdown("### 📈 Business Objectives Overview")
        st.markdown("*All data including individual sales reps and sub-totals*")
        
        # Modify the dataframe to make sub-totals visually distinct
        display_df = df.copy()
        
        # Make sub-total rows more obvious by capitalizing the text
        display_df["Sales Rep name"] = display_df["Sales Rep name"].apply(
            lambda x: f"{x.upper()}" if "SUB-TOTAL" in str(x) else x
        )

        # Display the complete dataframe with enhanced sub-total visibility
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=600,  # Fixed height with scrolling for large datasets
            column_config={
                col: st.column_config.TextColumn(col, disabled=True) 
                for col in display_df.columns
            }
        )
        
        # Add CSS styling to highlight sub-total rows
        st.markdown("""
        <style>
        /* Enhanced styling for better sub-total visibility */
        div[data-testid="stDataFrame"] table {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif !important;
        }
        
        /* Try to style rows containing SUB-TOTAL */
        div[data-testid="stDataFrame"] table tbody tr:contains("SUB-TOTAL") {
            background-color: #f0f8ff !important;
            font-weight: bold !important;
            border-top: 2px solid #4169e1 !important;
            border-bottom: 2px solid #4169e1 !important;
        }
        </style>
        """, unsafe_allow_html=True)
    
    # Edit button with prominent styling
    if st.button("Edit Data", type="primary", use_container_width=True):
        st.session_state.editing = True
        st.rerun()

else:
    # ============================================================================
    # Editing Mode - Clean interface for data modification
    # ============================================================================
    st.markdown("### ✏️ Edit Business Objectives")
    st.markdown("*Sub-total rows are automatically calculated and excluded from editing*")
    
    # Remove sub-total rows for editing
    editable_df = remove_subtotals_for_editing(df)

    # Fetch unique options for dropdown columns
    sales_rep_options = get_unique_sales_reps_commission_tier()
    product_line_options = get_unique_product_lines_service_to_product()

    # Configure column types for better user experience
    col_config = {
        "Sales Rep name": st.column_config.SelectboxColumn(
            "Sales Rep name",
            options=sales_rep_options,
            help="Select a Sales Rep name from the list"
        ),
        "Product line": st.column_config.SelectboxColumn(
            "Product line",
            options=product_line_options,
            help="Select a Product line from the list"
        )
    }

    st.write("**Editable Data** *(Sub-totals will be automatically recalculated)*:")
    edited_df = st.data_editor(
        editable_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        key="business_objective_editor",
        column_config=col_config
    )

    # Save changes with confirmation workflow
    if "save_initiated" not in st.session_state:
        st.session_state.save_initiated = False

    # Action buttons in columns for better layout
    col_save, col_cancel = st.columns(2)
    
    with col_save:
        if st.button("Save Changes", type="primary", use_container_width=True):
            st.session_state.save_initiated = True
            st.warning("⚠️ Are you sure you want to replace the current data with the new changes?")

    with col_cancel:
        if st.button("Cancel Editing", use_container_width=True):
            st.session_state.editing = False
            st.rerun()

    # Confirmation workflow
    if st.session_state.save_initiated:
        col_confirm, col_abort = st.columns(2)
        
        with col_confirm:
            if st.button("✅ Yes, Replace Table", type="primary", use_container_width=True):
                update_business_objective_data(edited_df, selected_year)
                st.session_state.save_initiated = False  # Reset state after save
                st.session_state.editing = False  # Return to read-only mode
                st.rerun()
        
        with col_abort:
            if st.button("❌ No, Keep Current Data", use_container_width=True):
                st.session_state.save_initiated = False
                st.info("Changes cancelled. Current data preserved.")
                st.rerun()