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

##### EDIT FOR CASE SENSITIVE PRODUCT LINE RETRIEVAL
# EDIT 1: Add this new function after get_db_connection()
def standardize_product_line_name(product_line):
    """Standardize product line names to ensure consistent display."""
    if not product_line:
        return product_line
    
    # Define standardized names for known variations
    standardized_names = {
        'logiquip': 'Logiquip',
        'cygnus': 'Cygnus', 
        'summit medical': 'Summit Medical',
        'inspektor': 'InspeKtor',
        'sunoptic': 'Sunoptic',
        'ternio': 'Ternio',
        'novo': 'Novo',
        'chemence': 'Chemence',
        'miscellaneous': 'Miscellaneous',
        'streamlite': 'Streamlite',
        'surgical instruments': 'Surgical Instruments',
        'services': 'Services',
        'mastel surgical': 'Mastel Surgical',
        'pure processing': 'Pure Processing'
    }
    
    product_line_lower = product_line.lower()
    return standardized_names.get(product_line_lower, product_line)

#######

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
    """Fetch distinct years for a specific Sales Rep from the harmonised_table.
    Uses Commission Date YYYY for commission attribution."""
    query = """
        SELECT DISTINCT "Commission Date YYYY"
        FROM harmonised_table
        WHERE "Sales Rep" = :sales_rep
        ORDER BY "Commission Date YYYY"
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


##### EDIT FOR CASE SENSITIVE PRODUCT LINE RETRIEVAL
# def get_unique_product_lines(sales_rep, year):
#     """Fetch unique Product Lines from harmonised_table based on Sales Rep and Year.
#     Uses Commission Date YYYY for commission attribution."""
#     query = """
#         SELECT DISTINCT "Product Line"
#         FROM harmonised_table
#         WHERE "Sales Rep" = :sales_rep
#           AND "Commission Date YYYY" = :year
#         ORDER BY "Product Line"
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query), {"sales_rep": sales_rep, "year": year})
#             product_lines = [row[0] for row in result.fetchall()]
#         return product_lines
#     except Exception as e:
#         st.error(f"Error fetching Product Lines: {e}")
#         return []
#     finally:
#         engine.dispose()
def get_unique_product_lines(sales_rep, year):
    """Fetch unique Product Lines from harmonised_table based on Sales Rep and Year.
    Uses Commission Date YYYY for commission attribution.
    Groups case-insensitively and returns standardized product line names."""
    query = """
        SELECT LOWER("Product Line") as product_line_lower, 
               MAX("Product Line") as product_line_display
        FROM harmonised_table
        WHERE "Sales Rep" = :sales_rep
          AND "Commission Date YYYY" = :year
        GROUP BY LOWER("Product Line")
        ORDER BY LOWER("Product Line")
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"sales_rep": sales_rep, "year": year})
            # Use standardization function to ensure consistent naming
            product_lines = []
            for row in result.fetchall():
                standardized_name = standardize_product_line_name(row[1])  # Use display name but standardize it
                product_lines.append(standardized_name)
        return product_lines
    except Exception as e:
        st.error(f"Error fetching Product Lines: {e}")
        return []
    finally:
        engine.dispose()

##########

def get_monthly_commission(sales_rep, year, month, product_line):
    """
    Fetch the sum of commission for a Sales Rep, Product Line, specific month, and year.
    Uses Commission Date YYYY/MM for commission attribution.
    (Not used in the report generation below.)
    """
    query = """
        SELECT SUM("Comm Amount")
        FROM harmonised_table
        WHERE "Sales Rep" = :sales_rep
          AND "Commission Date YYYY" = :year
          AND "Commission Date MM" = :month
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


##### EDIT FOR CASE SENSITIVE PRODUCT LINE RETRIEVAL
# def generate_report(sales_rep, year):
#     """
#     Generate the commission report for a Sales Rep or all Sales Reps.
#     It aggregates commissions per Product Line and Month (MM) using the following logic:

#       For each month in a Sales Rep/Product Line group:
#         - Fetch the monthly Sales Actual, tier 1 commission, and tier 2 differential.
#         - Accumulate the Sales Actual.
#         - If cumulative sales < threshold:
#              Report commission = tier 1 commission only.
#              Accumulate the deferred differential (tier 2 diff amount).
#         - When cumulative sales reach (or exceed) the threshold for the first time:
#              Report commission = (tier 1 commission + tier 2 differential for the month)
#                                   + (accumulated differential from previous months).
#              Then reset the accumulated differential.
#         - After the threshold is reached, report commission = tier 1 + tier 2 for that month.
        
#     The final DataFrame displays one row per Product Line with monthly columns and a YTD Total.
    
#     Uses Commission Date YYYY/MM for all commission-related calculations.
#     """
#     engine = get_db_connection()

#     # Determine which sales reps to include.
#     if sales_rep == "All":
#         try:
#             with engine.connect() as conn:
#                 sales_reps_query = """
#                     SELECT DISTINCT "Sales Rep"
#                     FROM harmonised_table
#                     WHERE "Sales Rep" IS NOT NULL
#                 """
#                 result = conn.execute(text(sales_reps_query))
#                 all_sales_reps = [row[0] for row in result.fetchall()]
#         except Exception as e:
#             st.error(f"Error fetching Sales Reps: {e}")
#             return pd.DataFrame()
#     else:
#         all_sales_reps = [sales_rep]

#     final_report_data = {}

#     try:
#         with engine.connect() as conn:
#             for rep in all_sales_reps:
#                 # For each sales rep, get the distinct product lines in the year.
#                 product_lines = get_unique_product_lines(rep, year)
#                 for product_line in product_lines:
                    
#                     # ensure our row exists
#                     if product_line not in final_report_data:
#                         final_report_data[product_line] = {str(i).zfill(2): 0 for i in range(1, 13)}
#                         final_report_data[product_line]["Total"] = 0

#                     # **always** fetch this rep's true threshold**
#                     threshold_query = """
#                         SELECT "Commission tier threshold"
#                         FROM sales_rep_commission_tier_threshold
#                         WHERE lower("Sales Rep name") = lower(:sales_rep)
#                           AND "Year" = :year
#                           AND lower("Product line") = lower(:product_line)
#                     """
#                     threshold_result = conn.execute(
#                         text(threshold_query),
#                         {"sales_rep": rep, "year": year, "product_line": product_line}
#                     )
#                     t = threshold_result.scalar()
#                     # if no row, assume “infinite” so nobody ever hits it
#                     threshold = float('inf') if t is None else t

#                     # if we're in the single-rep view, record it for display
#                     if sales_rep != "All":
#                         final_report_data[product_line]["Comm Tier Threshold"] = threshold

#                     # Modify the commission query to also fetch monthly Sales Actual.
#                     commission_query = """
#                         SELECT 
#                             "Commission Date MM"::INTEGER AS month_number,
#                             SUM("Sales Actual") AS sales_actual,
#                             SUM("Comm Amount tier 1") AS tier1_sum,
#                             SUM("Comm tier 2 diff amount") AS tier2_sum,
#                             MAX("Commission tier 2 date") AS tier2_date
#                         FROM harmonised_table
#                         WHERE "Sales Rep" = :sales_rep
#                         AND "Commission Date YYYY" = :year
#                         AND LOWER("Product Line") = LOWER(:product_line)
                        
#                         GROUP BY "Commission Date MM"
#                         ORDER BY CAST("Commission Date MM" AS INTEGER)
#                     """

#                     commission_result = conn.execute(
#                         text(commission_query),
#                         {"sales_rep": rep, "year": year, "product_line": product_line}
#                     ).mappings()

#                     # Initialize cumulative variables.
#                     payout = 0
#                     cumulative_sales = 0
#                     threshold_applied = False

#                     # Process each month's data in order.
#                     for row in commission_result.fetchall():
#                         month = row["month_number"]
#                         month_str = str(month).zfill(2)
#                         tier1_sum = row["tier1_sum"] if row["tier1_sum"] is not None else 0
#                         tier2_sum = row["tier2_sum"] if row["tier2_sum"] is not None else 0
#                         sales_actual = row["sales_actual"] if row["sales_actual"] is not None else 0

#                         cumulative_sales += sales_actual

#                         if not threshold_applied:
#                             if cumulative_sales < threshold:
#                                 # Threshold not reached: report only tier1 commission.
#                                 commission_amount = tier1_sum
#                                 payout += tier2_sum  # accumulate deferred differential
#                             else:
#                                 # This month is the threshold month.
#                                 commission_amount = (tier1_sum + tier2_sum) + payout
#                                 payout = 0
#                                 threshold_applied = True
#                         else:
#                             # After threshold has been reached, use full tier2 commission.
#                             commission_amount = tier1_sum + tier2_sum

#                         # Sum the commission for the product line for this month.
#                         final_report_data[product_line][month_str] += commission_amount
#                         final_report_data[product_line]["Total"] += commission_amount

#     except Exception as e:
#         st.error(f"Error generating the report: {e}")
#         return pd.DataFrame()
#     finally:
#         engine.dispose()

#     # Build the final DataFrame.
#     report_df = pd.DataFrame.from_dict(final_report_data, orient="index").reset_index()
#     report_df.rename(columns={"index": "Product Line"}, inplace=True)

#     # Rename month columns from "01", "02", ... to "Jan", "Feb", etc.
#     month_mapping = {
#         str(i).zfill(2): month for i, month in enumerate(
#             ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1
#         )
#     }
#     report_df.rename(columns=month_mapping, inplace=True)

#     # # Ensure numeric columns are numeric.
#     # numeric_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
#     # for col in numeric_columns:
#     #     report_df[col] = pd.to_numeric(report_df[col], errors="coerce").fillna(0)

#     # sub_total_values = report_df.drop(columns=["Product Line"]).sum().to_dict()
#     # sub_total_values["Product Line"] = "Sub-total"
#     # report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)

#     ### float/DECIMALS error fixed from the code above ###
#     # # Ensure ALL numeric columns are float type to prevent Decimal/float mixing
#     # numeric_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
#     # for col in numeric_columns:
#     #     report_df[col] = pd.to_numeric(report_df[col], errors="coerce").fillna(0).astype(float)

#     # # Now sum will work correctly with all float values
#     # sub_total_values = report_df.drop(columns=["Product Line"]).sum().to_dict()
#     # sub_total_values["Product Line"] = "Sub-total"
#     # report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)
#     # ### end of fix ###

#     # # Separate the "Sub-total" row and sort the remaining rows by "Product Line" ascending.
#     # subtotal_df = report_df[report_df["Product Line"] == "Sub-total"]
#     # main_df = report_df[report_df["Product Line"] != "Sub-total"].sort_values(by="Product Line", ascending=True)
#     # report_df = pd.concat([main_df, subtotal_df], ignore_index=True)
#     # This approach is more thorough and handles Decimal objects completely
#     for col in report_df.columns:
#         if col != "Product Line":  # Skip non-numeric columns
#             try:
#                 # First convert everything to string to handle any data type
#                 report_df[col] = report_df[col].astype(str)
#                 # Replace any commas that might be in numeric representations
#                 report_df[col] = report_df[col].str.replace(',', '')
#                 # Convert to float, handling any conversion errors
#                 report_df[col] = pd.to_numeric(report_df[col], errors='coerce').fillna(0).astype(float)
#             except Exception as e:
#                 # If conversion fails, keep the column as-is
#                 print(f"Could not convert column {col} to numeric: {e}")
    
#     # Make sure we exclude any remaining non-numeric columns from summing
#     numeric_cols = report_df.select_dtypes(include=['float', 'int']).columns
    
#     # Create a dictionary for the sub-total row with only numeric columns
#     sub_total_values = {}
#     for col in report_df.columns:
#         if col in numeric_cols:
#             sub_total_values[col] = report_df[col].sum()
#         elif col == "Product Line":
#             sub_total_values[col] = "Sub-total"
#         else:
#             sub_total_values[col] = ""  # Non-numeric columns get empty string
    
#     # Add the sub-total row
#     report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)    

#     currency_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
#     for col in currency_columns:
#         report_df[col] = report_df[col].map(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")

#     if sales_rep != "All" and "Comm Tier Threshold" in report_df.columns:
#         report_df["Comm Tier Threshold"] = report_df["Comm Tier Threshold"].map(
#             lambda x: f"${x:,.2f}" if pd.notnull(x) and x > 0 else ""
#         )

#     return report_df
def generate_report(sales_rep, year):
    """
    Generate the commission report for a Sales Rep or all Sales Reps.
    It aggregates commissions per Product Line and Month (MM) using the following logic:

      For each month in a Sales Rep/Product Line group:
        - Fetch the monthly Sales Actual, tier 1 commission, and tier 2 differential.
        - Accumulate the Sales Actual.
        - If cumulative sales < threshold:
             Report commission = tier 1 commission only.
             Accumulate the deferred differential (tier 2 diff amount).
        - When cumulative sales reach (or exceed) the threshold for the first time:
             Report commission = (tier 1 commission + tier 2 differential for the month)
                                  + (accumulated differential from previous months).
             Then reset the accumulated differential.
        - After the threshold is reached, report commission = tier 1 + tier 2 for that month.
        
    The final DataFrame displays one row per Product Line with monthly columns and a YTD Total.
    
    Uses Commission Date YYYY/MM for all commission-related calculations.
    """
    engine = get_db_connection()

    # Determine which sales reps to include.
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

    final_report_data = {}

    try:
        with engine.connect() as conn:
            for rep in all_sales_reps:
                # For each sales rep, get the distinct product lines in the year.
                product_lines = get_unique_product_lines(rep, year)
                # for product_line in product_lines:
                    
                #     # ensure our row exists
                #     if product_line not in final_report_data:
                #         final_report_data[product_line] = {str(i).zfill(2): 0 for i in range(1, 13)}
                #         final_report_data[product_line]["Total"] = 0
                for product_line in product_lines:
                    
                    # Standardize the product line key for consistent grouping
                    standardized_product_line = standardize_product_line_name(product_line)
                    
                    # ensure our row exists - use standardized name as key
                    if standardized_product_line not in final_report_data:
                        final_report_data[standardized_product_line] = {str(i).zfill(2): 0 for i in range(1, 13)}
                        final_report_data[standardized_product_line]["Total"] = 0

                    # **always** fetch this rep's true threshold**
                    threshold_query = """
                        SELECT "Commission tier threshold"
                        FROM sales_rep_commission_tier_threshold
                        WHERE lower("Sales Rep name") = lower(:sales_rep)
                          AND "Year" = :year
                          AND lower("Product line") = lower(:product_line)
                    """
                    threshold_result = conn.execute(
                        text(threshold_query),
                        {"sales_rep": rep, "year": year, "product_line": product_line}
                    )
                    t = threshold_result.scalar()
                    # if no row, assume “infinite” so nobody ever hits it
                    threshold = float('inf') if t is None else t

                    # if we're in the single-rep view, record it for display
                    # if sales_rep != "All":
                    #     final_report_data[product_line]["Comm Tier Threshold"] = threshold
                    if sales_rep != "All":
                        final_report_data[standardized_product_line]["Comm Tier Threshold"] = threshold

                    # Modify the commission query to also fetch monthly Sales Actual.
                    commission_query = """
                        SELECT 
                            "Commission Date MM"::INTEGER AS month_number,
                            SUM("Sales Actual") AS sales_actual,
                            SUM("Comm Amount tier 1") AS tier1_sum,
                            SUM("Comm tier 2 diff amount") AS tier2_sum,
                            MAX("Commission tier 2 date") AS tier2_date
                        FROM harmonised_table
                        WHERE "Sales Rep" = :sales_rep
                        AND "Commission Date YYYY" = :year
                        AND LOWER("Product Line") = LOWER(:product_line)
                        
                        GROUP BY "Commission Date MM"
                        ORDER BY CAST("Commission Date MM" AS INTEGER)
                    """

                    commission_result = conn.execute(
                        text(commission_query),
                        {"sales_rep": rep, "year": year, "product_line": product_line}
                    ).mappings()

                    # Initialize cumulative variables.
                    payout = 0
                    cumulative_sales = 0
                    threshold_applied = False

                    # Process each month's data in order.
                    for row in commission_result.fetchall():
                        month = row["month_number"]
                        month_str = str(month).zfill(2)
                        tier1_sum = row["tier1_sum"] if row["tier1_sum"] is not None else 0
                        tier2_sum = row["tier2_sum"] if row["tier2_sum"] is not None else 0
                        sales_actual = row["sales_actual"] if row["sales_actual"] is not None else 0

                        cumulative_sales += sales_actual

                        if not threshold_applied:
                            if cumulative_sales < threshold:
                                # Threshold not reached: report only tier1 commission.
                                commission_amount = tier1_sum
                                payout += tier2_sum  # accumulate deferred differential
                            else:
                                # This month is the threshold month.
                                commission_amount = (tier1_sum + tier2_sum) + payout
                                payout = 0
                                threshold_applied = True
                        else:
                            # After threshold has been reached, use full tier2 commission.
                            commission_amount = tier1_sum + tier2_sum

                        # # Sum the commission for the product line for this month.
                        # final_report_data[product_line][month_str] += commission_amount
                        # final_report_data[product_line]["Total"] += commission_amount
                        # Sum the commission for the STANDARDIZED product line for this month.
                        final_report_data[standardized_product_line][month_str] += commission_amount
                        final_report_data[standardized_product_line]["Total"] += commission_amount

    except Exception as e:
        st.error(f"Error generating the report: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

    # Build the final DataFrame.
    report_df = pd.DataFrame.from_dict(final_report_data, orient="index").reset_index()
    report_df.rename(columns={"index": "Product Line"}, inplace=True)

    # Rename month columns from "01", "02", ... to "Jan", "Feb", etc.
    month_mapping = {
        str(i).zfill(2): month for i, month in enumerate(
            ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1
        )
    }
    report_df.rename(columns=month_mapping, inplace=True)

    # # Ensure numeric columns are numeric.
    # numeric_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
    # for col in numeric_columns:
    #     report_df[col] = pd.to_numeric(report_df[col], errors="coerce").fillna(0)

    # sub_total_values = report_df.drop(columns=["Product Line"]).sum().to_dict()
    # sub_total_values["Product Line"] = "Sub-total"
    # report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)

    ### float/DECIMALS error fixed from the code above ###
    # # Ensure ALL numeric columns are float type to prevent Decimal/float mixing
    # numeric_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
    # for col in numeric_columns:
    #     report_df[col] = pd.to_numeric(report_df[col], errors="coerce").fillna(0).astype(float)

    # # Now sum will work correctly with all float values
    # sub_total_values = report_df.drop(columns=["Product Line"]).sum().to_dict()
    # sub_total_values["Product Line"] = "Sub-total"
    # report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)
    # ### end of fix ###

    # # Separate the "Sub-total" row and sort the remaining rows by "Product Line" ascending.
    # subtotal_df = report_df[report_df["Product Line"] == "Sub-total"]
    # main_df = report_df[report_df["Product Line"] != "Sub-total"].sort_values(by="Product Line", ascending=True)
    # report_df = pd.concat([main_df, subtotal_df], ignore_index=True)
    # This approach is more thorough and handles Decimal objects completely
    for col in report_df.columns:
        if col != "Product Line":  # Skip non-numeric columns
            try:
                # First convert everything to string to handle any data type
                report_df[col] = report_df[col].astype(str)
                # Replace any commas that might be in numeric representations
                report_df[col] = report_df[col].str.replace(',', '')
                # Convert to float, handling any conversion errors
                report_df[col] = pd.to_numeric(report_df[col], errors='coerce').fillna(0).astype(float)
            except Exception as e:
                # If conversion fails, keep the column as-is
                print(f"Could not convert column {col} to numeric: {e}")
    
    # Make sure we exclude any remaining non-numeric columns from summing
    numeric_cols = report_df.select_dtypes(include=['float', 'int']).columns
    
    # Create a dictionary for the sub-total row with only numeric columns
    sub_total_values = {}
    for col in report_df.columns:
        if col in numeric_cols:
            sub_total_values[col] = report_df[col].sum()
        elif col == "Product Line":
            sub_total_values[col] = "Sub-total"
        else:
            sub_total_values[col] = ""  # Non-numeric columns get empty string
    
    # Add the sub-total row
    report_df = pd.concat([report_df, pd.DataFrame([sub_total_values])], ignore_index=True)

    # ADD this sorting logic:
    # Separate the "Sub-total" row and sort the remaining rows by "Product Line" alphabetically
    subtotal_df = report_df[report_df["Product Line"] == "Sub-total"]
    main_df = report_df[report_df["Product Line"] != "Sub-total"].sort_values(by="Product Line", ascending=True)
    report_df = pd.concat([main_df, subtotal_df], ignore_index=True)    

    currency_columns = [col for col in report_df.columns if col not in ["Product Line", "Comm Tier Threshold"]]
    for col in currency_columns:
        report_df[col] = report_df[col].map(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")

    if sales_rep != "All" and "Comm Tier Threshold" in report_df.columns:
        report_df["Comm Tier Threshold"] = report_df["Comm Tier Threshold"].map(
            lambda x: f"${x:,.2f}" if pd.notnull(x) and x > 0 else ""
        )

    return report_df

#########


def get_years_for_sales_rep_any():
    """Fetch distinct years for all Sales Reps from the harmonised_table.
    Uses Commission Date YYYY for commission attribution."""
    query = """
        SELECT DISTINCT "Commission Date YYYY"
        FROM harmonised_table
        ORDER BY "Commission Date YYYY"
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

def render_preview_table(df, css_class="", drop_index=True):
    """
    Render a DataFrame as an HTML table using custom CSS.
    If drop_index is True, the index is reset and not shown;
    otherwise the index is preserved.
    """
    if drop_index:
        html_table = df.reset_index(drop=True).to_html(index=False, classes=css_class)
    else:
        html_table = df.to_html(index=True, classes=css_class)
    st.markdown(html_table, unsafe_allow_html=True)

def commission_reports_page():
    st.title("Commission Reports")

    col1, col2 = st.columns(2)

    with col1:
        # Fetch unique Sales Reps
        sales_reps = get_unique_sales_reps()
        
        # Apply restriction: if the logged-in user is a simple user, limit options.
        if "user_permission" in st.session_state and st.session_state.user_permission.lower() == "user":
            user_name = st.session_state.user_name
            if user_name in sales_reps:
                sales_reps = ["All", user_name]
            else:
                sales_reps = ["All"]
        else:
            sales_reps.insert(0, "All")
        
        if not sales_reps:
            st.warning("No Sales Reps available.")
            return

        selected_sales_rep = st.selectbox("Select a Sales Rep:", sales_reps)
        if not selected_sales_rep:
            st.warning("Please select a Sales Rep to proceed.")
            return

        if selected_sales_rep != "All":
            years = get_years_for_sales_rep(selected_sales_rep)
        else:
            years = get_years_for_sales_rep_any()
            
        if not years:
            st.warning(f"No years available for Sales Rep '{selected_sales_rep}'.")
            return

        selected_year = st.selectbox("Select a Year:", years)
        if not selected_year:
            st.warning("Please select a year to proceed.")
            return

    report_df = generate_report(selected_sales_rep, selected_year)

    if not report_df.empty:
        numeric_columns = report_df.select_dtypes(include=["float64", "int64"]).columns
        report_df[numeric_columns] = report_df[numeric_columns].map(
            lambda x: f"${x:,.2f}" if pd.notnull(x) else ""
        )

    with col2:
        if not report_df.empty:
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
        
        st.markdown(
            """
            <style>
            .large_table {
                width: 100%;
                font-size: 16px;
                table-layout: fixed;
            }
            .large_table th, .large_table td {
                text-align: right;
                padding: 4px 8px;
            }
            .large_table th:nth-child(1),
            .large_table td:nth-child(1) {
                width: 200px !important;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
        
        render_preview_table(report_df, css_class="large_table")
        
commission_reports_page()