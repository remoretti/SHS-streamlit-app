import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def get_unique_sales_reps():
    """Fetch distinct Sales Rep names from the harmonised_table."""
    query = """
        SELECT DISTINCT "Sales Rep"
        FROM harmonised_table
        WHERE "Sales Rep" IS NOT NULL
        ORDER BY "Sales Rep"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            sales_reps = [row[0] for row in result.fetchall()]
        return sales_reps
    except Exception as e:
        st.error(f"Error fetching Sales Reps: {e}")
        return []
    finally:
        engine.dispose()

def get_years_for_sales_rep(sales_rep):
    """Fetch distinct years for a specific Sales Rep from the harmonised_table."""
    query = """
        SELECT DISTINCT "Date YYYY"
        FROM harmonised_table
        WHERE "Sales Rep" = :sales_rep
        ORDER BY "Date YYYY"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"sales_rep": sales_rep})
            years = [row[0] for row in result.fetchall()]
        return years
    except Exception as e:
        st.error(f"Error fetching years for Sales Rep '{sales_rep}': {e}")
        return []
    finally:
        engine.dispose()

def get_unique_product_lines(sales_rep, year):
    """Fetch unique Product Lines from harmonised_table based on Sales Rep and Year."""
    query = """
        SELECT DISTINCT "Product Line"
        FROM harmonised_table
        WHERE "Sales Rep" = :sales_rep
          AND "Date YYYY" = :year
        ORDER BY "Product Line"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"sales_rep": sales_rep, "year": year})
            product_lines = [row[0] for row in result.fetchall()]
        return product_lines
    except Exception as e:
        st.error(f"Error fetching Product Lines: {e}")
        return []
    finally:
        engine.dispose()

def get_monthly_commission(sales_rep, year, month, product_line):
    """
    Fetch the sum of commission for a Sales Rep, Product Line, specific month, and year.
    (Not used in the report generation below.)
    """
    query = """
        SELECT SUM("Comm Amount")
        FROM harmonised_table
        WHERE "Sales Rep" = :sales_rep
          AND "Date YYYY" = :year
          AND "Date MM" = :month
          AND "Product Line" = :product_line
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(query),
                {"sales_rep": sales_rep, "year": year, "month": month, "product_line": product_line}
            )
            total = result.scalar()
        return total if total else 0
    except Exception as e:
        st.error(f"Error fetching monthly commission: {e}")
        return 0
    finally:
        engine.dispose()

def generate_report(sales_rep, year):
    """
    Generate the commission report for a Sales Rep or all Sales Reps.
    It aggregates commissions per Product Line and Month (MM) using the following logic:

      For each month in a Sales Rep/Product Line group:
        - Initialize a cumulative payout variable.
        - If "Commission tier 2 date" is NULL for that month:
             commission = sum("Comm Amount tier 1")
             (And accumulate the deferred sum: sum("Comm tier 2 diff amount"))
        - If "Commission tier 2 date" equals "{year}-{MM}" for that month:
             commission = sum("Comm Amount tier 1") + sum("Comm tier 2 diff amount") + (accumulated payout)
             (Then reset the payout.)
        - Else:
             commission = sum("Comm Amount tier 1") + sum("Comm tier 2 diff amount")

    The report DataFrame displays one row per Product Line with monthly columns and a YTD Total.
    """
    engine = get_db_connection()

    # If "All" is selected, process every Sales Rep; otherwise process only the selected one.
    if sales_rep == "All":
        try:
            with engine.connect() as conn:
                sales_reps_query = """
                    SELECT DISTINCT "Sales Rep"
                    FROM harmonised_table
                    WHERE "Sales Rep" IS NOT NULL
                """
                result = conn.execute(text(sales_reps_query))
                all_sales_reps = [row[0] for row in result.fetchall()]
        except Exception as e:
            st.error(f"Error fetching Sales Reps: {e}")
            return pd.DataFrame()
    else:
        all_sales_reps = [sales_rep]

    # Initialize final report data structure.
    final_report_data = {}

    try:
        with engine.connect() as conn:
            for rep in all_sales_reps:
                product_lines = get_unique_product_lines(rep, year)
                for product_line in product_lines:
                    # Initialize monthly commission amounts (months "01" to "12") and YTD total.
                    if product_line not in final_report_data:
                        final_report_data[product_line] = {str(i).zfill(2): 0 for i in range(1, 13)}
                        final_report_data[product_line]["Total"] = 0
                        # Optionally, if a single rep is selected, also fetch the commission tier threshold.
                        if sales_rep != "All":
                            threshold_query = """
                                SELECT "Commission tier threshold"
                                FROM sales_rep_commission_tier_threshold
                                WHERE "Sales Rep name" = :sales_rep
                                  AND "Year" = :year
                                  AND "Product line" = :product_line
                            """
                            threshold_result = conn.execute(
                                text(threshold_query),
                                {"sales_rep": sales_rep, "year": year, "product_line": product_line}
                            )
                            threshold = threshold_result.scalar() or 0
                            final_report_data[product_line]["Comm Tier Threshold"] = threshold

                    # --- Revised Commission Calculation Logic ---
                    # Group harmonised_table rows by month (Date MM) for this rep/product_line.
                    commission_query = """
                        SELECT "Date MM",
                               SUM("Comm Amount tier 1") AS tier1_sum,
                               SUM("Comm tier 2 diff amount") AS tier2_sum,
                               MAX("Commission tier 2 date") AS tier2_date
                        FROM harmonised_table
                        WHERE "Sales Rep" = :sales_rep
                          AND "Date YYYY" = :year
                          AND "Product Line" = :product_line
                        GROUP BY "Date MM"
                        ORDER BY "Date MM"
                    """
                    # Use .mappings() so that each row is a dict-like object.
                    commission_result = conn.execute(
                        text(commission_query),
                        {"sales_rep": rep, "year": year, "product_line": product_line}
                    ).mappings()
                    
                    # Initialize payout (accumulated deferred tier2 amounts) for this product line.
                    payout = 0

                    # Process each month in ascending order.
                    for row in commission_result.fetchall():
                        month = row["Date MM"]
                        month_str = str(month).zfill(2)
                        tier1_sum = row["tier1_sum"] if row["tier1_sum"] is not None else 0
                        tier2_sum = row["tier2_sum"] if row["tier2_sum"] is not None else 0
                        tier2_date = row["tier2_date"]
                        
                        # New logic:
                        if tier2_date is None:
                            # Tier 2 not yet applied this month: pay only tier1,
                            # and accumulate the tier2 amount for a future month.
                            commission_amount = tier1_sum
                            payout += tier2_sum
                        elif tier2_date == f"{year}-{month_str}":
                            # When the Commission tier 2 date equals the current month,
                            # pay both tier1 and tier2 plus any previously deferred amount.
                            commission_amount = tier1_sum + tier2_sum + payout
                            payout = 0  # Reset the payout after applying.
                        else:
                            # In any other case, pay the full amounts.
                            commission_amount = tier1_sum + tier2_sum

                        final_report_data[product_line][month_str] += commission_amount
                        final_report_data[product_line]["Total"] += commission_amount

    except Exception as e:
        st.error(f"Error generating the report: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

    # Convert the aggregated data into a DataFrame.
    report_df = pd.DataFrame.from_dict(final_report_data, orient="index").reset_index()
    report_df.rename(columns={"index": "Product Line"}, inplace=True)

    # Rename month columns to abbreviated names.
    month_mapping = {
        str(i).zfill(2): month for i, month in enumerate(
            ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1
        )
    }
    report_df.rename(columns=month_mapping, inplace=True)

    # Ensure all numeric columns are correctly formatted.
    numeric_columns = [col for col in report_df.columns if col not in ["Product Line"]]
    for col in numeric_columns:
        report_df[col] = pd.to_numeric(report_df[col], errors="coerce").fillna(0)

    # Compute a sub-total row.
    sub_total_values = report_df.drop(columns=["Product Line"]).sum().to_dict()
    sub_total_values["Product Line"] = "Sub-total"
    report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)

    # Format numeric values as currency.
    currency_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
    for col in currency_columns:
        report_df[col] = report_df[col].map(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")

    # Format "Comm Tier Threshold" only if a single Sales Rep is selected.
    if sales_rep != "All" and "Comm Tier Threshold" in report_df.columns:
        report_df["Comm Tier Threshold"] = report_df["Comm Tier Threshold"].map(
            lambda x: f"${x:,.2f}" if pd.notnull(x) and x > 0 else ""
        )

    return report_df

def get_years_for_sales_rep_any():
    """Fetch distinct years for all Sales Reps from the harmonised_table."""
    query = """
        SELECT DISTINCT "Date YYYY"
        FROM harmonised_table
        ORDER BY "Date YYYY"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            years = [row[0] for row in result.fetchall()]
        return years
    except Exception as e:
        st.error(f"Error fetching years for all Sales Reps: {e}")
        return []
    finally:
        engine.dispose()

def commission_reports_page():
    """Commission Reports Page Logic."""
    st.title("Commission Reports")

    col1, col2 = st.columns(2)

    with col1:
        # Fetch unique Sales Reps and add "All" option.
        sales_reps = get_unique_sales_reps()
        sales_reps.insert(0, "All")
        if not sales_reps:
            st.warning("No Sales Reps available.")
            return

        # Step 1: Select Sales Rep.
        selected_sales_rep = st.selectbox("Select a Sales Rep:", sales_reps)
        if not selected_sales_rep:
            st.warning("Please select a Sales Rep to proceed.")
            return

        # Step 2: Select Year.
        years = get_years_for_sales_rep(selected_sales_rep) if selected_sales_rep != "All" else get_years_for_sales_rep_any()
        if not years:
            st.warning(f"No years available for Sales Rep '{selected_sales_rep}'.")
            return

        selected_year = st.selectbox("Select a Year:", years)
        if not selected_year:
            st.warning("Please select a year to proceed.")
            return

    # Fetch and display commission report.
    report_df = generate_report(selected_sales_rep, selected_year)

    # (Optional) Format numeric columns with the $ symbol.
    if not report_df.empty:
        numeric_columns = report_df.select_dtypes(include=["float64", "int64"]).columns
        report_df[numeric_columns] = report_df[numeric_columns].map(
            lambda x: f"${x:,.2f}" if pd.notnull(x) else ""
        )

    with col2:
        if not report_df.empty:
            # Calculate the YTD Total (excluding the sub-total row).
            if "Total" in report_df.columns:
                filtered_df = report_df[report_df["Product Line"] != "Sub-total"]
                if pd.api.types.is_numeric_dtype(filtered_df["Total"]):
                    total_sum = filtered_df["Total"].sum()
                else:
                    total_sum = filtered_df["Total"].astype(str).str.replace("$", "", regex=False)\
                                    .str.replace(",", "", regex=False).astype(float).sum()
            else:
                total_sum = 0

            st.markdown(
                f"""
                <div style="text-align: right; font-size: 2.5em; font-weight: bold;">
                    YTD Total ${total_sum:,.2f}
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.write("No data available to summarize.")

    if report_df.empty:
        st.warning(f"No data available for Sales Rep '{selected_sales_rep}' in year '{selected_year}'.")
    else:
        st.markdown("---")
        st.subheader(f"Commission Report for {selected_sales_rep} ({selected_year})")
        st.dataframe(report_df, use_container_width=True)

commission_reports_page()
