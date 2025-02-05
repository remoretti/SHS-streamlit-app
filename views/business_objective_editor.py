import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

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
            # Fetch unique product lines
            product_lines_query = """
                SELECT DISTINCT "Product line" 
                FROM sales_rep_business_objective
                WHERE "Year" = %s
                ORDER BY "Product line"
            """
            product_lines = pd.read_sql_query(product_lines_query, conn, params=(selected_year,))

            # Fetch sales reps per product line
            sales_rep_query = """
                SELECT DISTINCT "Product line", "Sales Rep name"
                FROM sales_rep_business_objective
                WHERE "Year" = %s
                ORDER BY "Product line", "Sales Rep name"
            """
            sales_reps = pd.read_sql_query(sales_rep_query, conn, params=(selected_year,))

            # Fetch commission tier thresholds
            commission_query = """
                SELECT "Product line", "Sales Rep name", "Commission tier threshold"
                FROM sales_rep_commission_tier_threshold
                WHERE "Year" = %s
            """
            commission_tiers = pd.read_sql_query(commission_query, conn, params=(selected_year,))

            # Fetch monthly objectives
            objective_query = """
                SELECT "Product line", "Sales Rep name", "Month", "Objective"
                FROM sales_rep_business_objective
                WHERE "Year" = %s
            """
            objectives = pd.read_sql_query(objective_query, conn, params=(selected_year,))

        # Merge data to create the full DataFrame
        full_df = sales_reps.merge(commission_tiers, on=["Product line", "Sales Rep name"], how="left")
        for month in range(1, 13):
            month_name = pd.to_datetime(f"{month}", format="%m").strftime("%B")  # Convert month number to name
            month_data = objectives[objectives["Month"] == month].rename(columns={"Objective": month_name})
            full_df = full_df.merge(month_data[["Product line", "Sales Rep name", month_name]], 
                                    on=["Product line", "Sales Rep name"], 
                                    how="left")
        # Calculate the Annual Objective
        full_df["Annual Objective"] = full_df.loc[:, "January":"December"].sum(axis=1, skipna=True)

        # Format numeric columns with $ symbol
        numeric_columns = ["Commission tier threshold", "January", "February", "March", "April", "May", "June",
                           "July", "August", "September", "October", "November", "December", "Annual Objective"]
        for col in numeric_columns:
            if col in full_df.columns:
                full_df[col] = full_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")

        # Sort by Product Line
        full_df = full_df.sort_values(by=["Product line", "Sales Rep name"]).reset_index(drop=True)

        # Add Sub-Total Rows
        rows_with_subtotals = []
        for product_line, group in full_df.groupby("Product line"):
            rows_with_subtotals.append(group)  # Add the group
            # Calculate sub-totals for numeric columns
            subtotal = group.loc[:, "January":"December"].applymap(
                lambda x: float(x.replace('$', '').replace(',', ''))).sum()
            subtotal["Annual Objective"] = subtotal.sum()
            subtotal_row = pd.Series({
                "Product line": product_line,
                "Sales Rep name": "Sub-Total",
                **{col: f"${subtotal[col]:,.2f}" for col in subtotal.index}
            })
            rows_with_subtotals.append(pd.DataFrame([subtotal_row]))  # Add the subtotal row

        # Concatenate the rows with sub-totals
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
        # Filter out "Sub-Total" rows
        filtered_df = df[df["Sales Rep name"] != "Sub-Total"].copy()

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

def highlight_subtotals_readonly(row):
    """Style rows where 'Sales Rep name' is 'Sub-Total'."""
    if row["Sales Rep name"] == "Sub-Total":
        return ["color: red; font-weight: bold;" for _ in row]
    return [""] * len(row)


def remove_subtotals_for_editing(df):
    """Remove Sub-Total rows before making the DataFrame editable."""
    return df[df["Sales Rep name"] != "Sub-Total"].reset_index(drop=True)


st.title("Business Objective Editor")


# Layout with two columns for Year selection and Annual Objective Total
col1, col2 = st.columns([1, 1])

# Fetch available years
years = get_available_years()
if not years:
    st.warning("No years available.")
else:
    with col1:
        # Year selection
        selected_year = st.selectbox("Select Year:", years)

    if selected_year:
        # Fetch the data
        df = fetch_business_objective_data(selected_year)

        if not df.empty:
            # Compute the Annual Objective Total from sub-totals
            sub_totals = df[df["Sales Rep name"] == "Sub-Total"]
            annual_total = sub_totals["Annual Objective"].apply(
                lambda x: float(x.replace("$", "").replace(",", ""))
            ).sum()

            with col2:
                # Display Annual Objective Total, right-aligned
                st.markdown(
                    f"""
                    <div style="text-align: right; font-size: 1.5em; font-weight: bold;">
                        Annual Objective Total: ${annual_total:,.2f}
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # Initialize editing state if not already set
            if "editing" not in st.session_state:
                st.session_state.editing = False

            if not st.session_state.editing:
                # Show the "read-only" DataFrame with sub-totals highlighted
                styled_df = df.style.apply(highlight_subtotals_readonly, axis=1)


                st.write("Preview with Sub-Totals Highlighted (Read-Only):")
                st.write(
                    styled_df,
                    unsafe_allow_html=True,  # Allow styling
                    use_container_width=True,
                )

                # Add the "Edit Data" button
                if st.button("Edit Data"):
                    st.session_state.editing = True  # Switch to edit mode
            else:
                # Show the editable DataFrame with sub-totals removed
                editable_df = remove_subtotals_for_editing(df)

                st.write("Editable DataFrame (Sub-Totals Removed):")
                edited_df = st.data_editor(
                    editable_df,
                    use_container_width=True,
                    num_rows="dynamic",
                    hide_index=True,
                    key="business_objective_editor",
                )

                # Save button logic
                if "save_initiated" not in st.session_state:
                    st.session_state.save_initiated = False

                if st.button("Save Changes"):
                    st.session_state.save_initiated = True
                    st.warning("Are you sure you want to replace the current data with the new changes?")

                # Confirmation button (appears only if save was initiated)
                if st.session_state.save_initiated:
                    if st.button("Yes, Replace Table"):
                        update_business_objective_data(edited_df, selected_year)
                        st.session_state.save_initiated = False  # Reset state after save
                        st.session_state.editing = False  # Return to read-only mode
                        st.rerun()  # Reload the page to show updated data
        else:
            st.warning(f"No data available for the selected year: {selected_year}.")



