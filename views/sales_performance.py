# import streamlit as st
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# import os
# import matplotlib.pyplot as plt

# # Load environment variables
# load_dotenv()
# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# def get_unique_years():
#     """Fetch distinct years from the sales_rep_business_objective table."""
#     query = """
#         SELECT DISTINCT "Year"
#         FROM sales_rep_business_objective
#         ORDER BY "Year"
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query))
#             years = [row[0] for row in result.fetchall()]
#         return years
#     except Exception as e:
#         st.error(f"Error fetching years: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_salespeople_by_year(selected_year):
#     """Fetch distinct salespeople from harmonised_table filtered by year."""
#     query = """
#         SELECT DISTINCT "Sales Rep"
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#         ORDER BY "Sales Rep"
#     """

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query), {"year": str(selected_year)})  # Ensure year is passed as string
#             salespeople = [row[0] for row in result.fetchall()]
#         return salespeople
#     except Exception as e:
#         st.error(f"Error fetching salespeople: {e}")
#         return []
#     finally:
#         engine.dispose()


# def get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson):
#     """Fetch distinct product lines from harmonised_table filtered by year and salesperson."""
#     query = """
#         SELECT DISTINCT "Product Line"
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {salesperson_filter}
#         ORDER BY "Product Line"
#     """
    
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
    
#     query = query.format(salesperson_filter=salesperson_filter)

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}  # Ensure year is passed as a string
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             product_lines = [row[0] for row in result.fetchall()]
#         return product_lines
#     except Exception as e:
#         st.error(f"Error fetching product lines: {e}")
#         return []
#     finally:
#         engine.dispose()


# def get_ytd_sales_actual(selected_year, selected_product_line, selected_salesperson):
#     """Fetch YTD sales actual from the harmonised_table."""
#     query = """
#         SELECT SUM("Sales Actual") AS ytd_sales_actual
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'

#     query = query.format(
#         product_line_filter=product_line_filter, salesperson_filter=salesperson_filter
#     )

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": selected_year}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_sales_actual = result.scalar() or 0
#         return ytd_sales_actual
#     except Exception as e:
#         st.error(f"Error fetching YTD Sales Actual: {e}")
#         return 0
#     finally:
#         engine.dispose()

# def get_ytd_revenue_actual(selected_year, selected_product_line, selected_salesperson):
#     """Fetch YTD revenue actual from the harmonised_table."""
#     query = """
#         SELECT SUM("Rev Actual") AS ytd_revenue_actual
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'

#     query = query.format(
#         product_line_filter=product_line_filter, salesperson_filter=salesperson_filter
#     )

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": selected_year}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_revenue_actual = result.scalar() or 0
#         return ytd_revenue_actual
#     except Exception as e:
#         st.error(f"Error fetching YTD Revenue Actual: {e}")
#         return 0
#     finally:
#         engine.dispose()

# def get_ytd_shs_margin(selected_year, selected_product_line, selected_salesperson):
#     """Fetch the YTD SHS Margin from the harmonised_table."""
#     query = """
#         SELECT SUM("SHS Margin") AS ytd_shs_margin
#         FROM harmonised_table
#         WHERE "Date YYYY" = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     # Add filters for product line and salesperson
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""

#     # Format the query with dynamic filters
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}  # Cast year to string
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_shs_margin = result.scalar()
#         return ytd_shs_margin if ytd_shs_margin else 0.0
#     except Exception as e:
#         st.error(f"Error fetching YTD SHS Margin: {e}")
#         return 0.0
#     finally:
#         engine.dispose()

# def get_ytd_commission_payout(selected_year, selected_product_line, selected_salesperson):
#     """Fetch the YTD Commission Payout dynamically."""
#     query = """
#         SELECT 
#             SUM(
#                 CASE 
#                     WHEN "Commission tier 2 date" IS NULL 
#                     THEN "Comm Amount tier 1"
#                     WHEN SPLIT_PART("Commission tier 2 date", '-', 2)::INTEGER < "Date MM"::INTEGER
#                     THEN "Comm Amount tier 1"
#                     ELSE "Comm Amount tier 1" + "Comm tier 2 diff amount"
#                 END
#             ) AS ytd_commission_payout
#         FROM harmonised_table
#         WHERE "Date YYYY" = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """

#     # Apply filters dynamically
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""

#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)

#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson

#             result = conn.execute(text(query), params)
#             ytd_commission_payout = result.scalar() or 0.0  # Handle NULL values

#         return ytd_commission_payout
#     except Exception as e:
#         st.error(f"Error fetching YTD Commission Payout: {e}")
#         return 0.0
#     finally:
#         engine.dispose()

# def fetch_monthly_data(selected_year, selected_product_line, selected_salesperson):
#     """Fetch monthly sales performance data including dynamically calculated commission values."""
    
#     engine = get_db_connection()
    
#     try:
#         with engine.connect() as conn:
#             # SQL Query to calculate monthly sales performance from `harmonised_table`
#             query = """
#                 WITH commission_tier2 AS (
#     -- Store payback (tier 2 commission that should be applied in the future)
#     SELECT 
#         "Date MM"::INTEGER AS month_number,
#         "Sales Rep",
#         "Product Line",
#         SUM("Comm tier 2 diff amount") AS payback
#     FROM harmonised_table
#     WHERE "Date YYYY" = :year
#     GROUP BY "Date MM", "Sales Rep", "Product Line"
# )

# SELECT 
#     h."Date MM"::INTEGER AS month_number,
#     SUM(h."Sales Actual") AS Sales_Actual,
#     SUM(h."Rev Actual") AS Revenue_Actual,

#     -- Commission Amount Calculation
#     SUM(
#         CASE 
#             WHEN h."Commission tier 2 date" IS NULL 
#             THEN h."Comm Amount tier 1"
#             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER > h."Date MM"::INTEGER
#             THEN h."Comm Amount tier 1"  -- Store "Comm tier 2 diff amount" as payout
#             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER = h."Date MM"::INTEGER
#             THEN h."Comm Amount tier 1" + h."Comm tier 2 diff amount" + COALESCE(ct.payback, 0)
#             ELSE h."Comm Amount tier 1" + h."Comm tier 2 diff amount"
#         END
#     ) AS Commission_Amount,

#     -- SHS Margin Calculation
#     SUM(
#         CASE 
#             -- If there's no "Commission tier 2 date", deduct only tier 1 commission
#             WHEN h."Commission tier 2 date" IS NULL 
#             THEN h."Rev Actual" - h."Comm Amount tier 1"

#             -- If tier2mm > current month, only deduct tier 1 commission & store tier 2 as payout
#             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER > h."Date MM"::INTEGER
#             THEN h."Rev Actual" - h."Comm Amount tier 1"

#             -- If tier2mm == current month, deduct tier 1 + tier 2 + stored payout
#             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER = h."Date MM"::INTEGER
#             THEN h."Rev Actual" - (h."Comm Amount tier 1" + h."Comm tier 2 diff amount" + COALESCE(ct.payback, 0))

#             -- If tier2mm < current month, deduct tier 1 + tier 2
#             ELSE h."Rev Actual" - (h."Comm Amount tier 1" + h."Comm tier 2 diff amount")
#         END
#     ) AS SHS_Margin,

#     -- Commission Payout Calculation
#     SUM(h."Comm Amount tier 1") + SUM(h."Comm tier 2 diff amount") AS Commission_Payout

# FROM harmonised_table h

# -- Join the stored commission tier 2 payback amounts
# LEFT JOIN commission_tier2 ct
# ON h."Date MM"::INTEGER = ct.month_number
# AND h."Sales Rep" = ct."Sales Rep"
# AND h."Product Line" = ct."Product Line"

# WHERE h."Date YYYY" = :year
#   {product_line_filter}
#   {salesperson_filter}

# GROUP BY h."Date MM"
# ORDER BY month_number;

#             """

#             # Apply filtering logic dynamically
#             product_line_filter = 'AND h."Product Line" = :product_line' if selected_product_line != "All" else ""
#             salesperson_filter = 'AND h."Sales Rep" = :salesperson' if selected_salesperson != "All" else ""

#             # Format the query dynamically with filters
#             query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)

#             # Set up parameters for query execution
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson

#             # Execute query and fetch results into DataFrame
#             result = conn.execute(text(query), params)
#             data = pd.DataFrame(result.fetchall(), columns=[
#                 "month_number", "Sales Actual", "Revenue Actual", 
#                 "Commission Amount", "SHS Margin", "Commission Payout"
#             ])
            
#             # Convert 'month_number' to integer for sorting
#             data["month_number"] = data["month_number"].astype(int)

#             # Convert month numbers to month names
#             data["Month"] = data["month_number"].apply(lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))

#             # Fetch Sales Objective separately from `sales_rep_business_objective`
#             objective_query = """
#                 SELECT "Month"::INTEGER AS month_number, SUM("Objective") AS Sales_Objective
#                 FROM sales_rep_business_objective
#                 WHERE "Year" = :year
#                   {product_line_filter}
#                   {salesperson_filter}
#                 GROUP BY "Month"
#             """

#             # Apply correct filters for the sales objective query
#             objective_query = objective_query.format(
#                 product_line_filter=('AND "Product line" = :product_line' if selected_product_line != "All" else ""),
#                 salesperson_filter=('AND "Sales Rep name" = :salesperson' if selected_salesperson != "All" else "")
#             )

#             # Execute objective query
#             result = conn.execute(text(objective_query), params)
#             objective_data = pd.DataFrame(result.fetchall(), columns=["month_number", "Sales Objective"])
#             objective_data["month_number"] = objective_data["month_number"].astype(int)

#             # Define all 12 months
#             all_months_df = pd.DataFrame({"month_number": list(range(1, 13))})

#             # Merge monthly sales data ensuring all months exist
#             data = pd.merge(all_months_df, data, on="month_number", how="left").fillna(0)

#             # Merge sales objectives ensuring all months exist
#             merged_df = pd.merge(data, objective_data, on="month_number", how="outer").fillna(0)

#             # Convert 'month_number' to month names
#             merged_df["Month"] = merged_df["month_number"].apply(lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))

#             # Ensure all numeric columns are converted to float before calculations
#             numeric_columns = ["Sales Actual", "Sales Objective", "Revenue Actual", "Commission Amount", "SHS Margin", "Commission Payout"]
#             merged_df[numeric_columns] = merged_df[numeric_columns].astype(float)

#             # Calculate % to Objective
#             merged_df["% to Objective"] = merged_df.apply(
#                 lambda row: f"{(row['Sales Actual'] / row['Sales Objective'] * 100):.2f}%" 
#                 if row["Sales Objective"] > 0 else "N/A",
#                 axis=1
#             )

#             # Reorder DataFrame
#             merged_df = merged_df[[
#                 "Month", "Sales Actual", "Sales Objective", "% to Objective", 
#                 "Revenue Actual", "Commission Amount", "SHS Margin", "Commission Payout"
#             ]]

#             return merged_df

#     except Exception as e:
#         st.error(f"Error fetching monthly data: {e}")
#         return pd.DataFrame()
    
#     finally:
#         engine.dispose()



# def get_data_status_summary():
#     """Fetch data status and calculate summary."""
#     query = "SELECT * FROM data_status"
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             # Fetch data status table
#             df = pd.read_sql_query(query, conn)

#             # Convert 't' and 'f' to boolean values
#             boolean_df = df.iloc[:, 1:].replace({'t': True, 'f': False})

#             # Count the number of True values for each Product Line
#             df["Total Months"] = boolean_df.sum(axis=1)

#             # Total months in a year
#             total_months_in_year = 12

#             # Create summary strings
#             summary = [
#                 f"{row['Product line']}: {row['Total Months']}/{total_months_in_year}"
#                 for _, row in df.iterrows()
#             ]
#         return summary
#     except Exception as e:
#         st.error(f"Error fetching data status summary: {e}")
#         return []
#     finally:
#         engine.dispose()

# # Streamlit UI
# st.title("Sales Performance")

# # Layout with two columns
# col1, col2 = st.columns([1, 1])

# # Column 1: Filters
# with col1:
#     years = get_unique_years()
#     if not years:
#         st.warning("No years available.")
#     else:
#         # Step 1: Select a Year
#         selected_year = st.selectbox("Select a Year:", years)

#         if selected_year:
#             # Step 2: Choose a Salesperson
#             if selected_year:
#                 salespeople = get_salespeople_by_year(selected_year)
#                 salespeople.insert(0, "All")  # Add "All" option
#                 selected_salesperson = st.selectbox("Choose a Salesperson:", salespeople)

#             # Step 3: Choose a Product Line
#             if selected_year:
#                 product_lines = get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson)
#                 product_lines.insert(0, "All")  # Add "All" option
#                 selected_product_line = st.selectbox("Choose a Product Line:", product_lines)


# # Column 2: YTD Sales Actual
# with col2:
#     if selected_year and selected_product_line:
#         ytd_sales_actual = get_ytd_sales_actual(selected_year, selected_product_line, selected_salesperson)
#         ytd_revenue_actual = get_ytd_revenue_actual(selected_year, selected_product_line, selected_salesperson)
#         ytd_shs_margin = get_ytd_shs_margin(selected_year, selected_product_line, selected_salesperson)
#         ytd_commission_payout = get_ytd_commission_payout(selected_year, selected_product_line, selected_salesperson)
#         st.markdown(
#             f"""
#             <div style="text-align: right; font-size: 1.5em; font-weight: bold;">
#                 YTD Sales Actual: ${ytd_sales_actual:,.2f}
#                 </br>
#                 YTD Revenue Actual: ${ytd_revenue_actual:,.2f}
#                 </br>
#                 YTD SHS Margin: ${ytd_shs_margin:,.2f}
#                 </br>
#                 YTD Commission Payout: ${ytd_commission_payout:,.2f}
#             </div>
#             """,
#             unsafe_allow_html=True,
#         )

# # Generate Monthly DataFrame
# monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)

# if not monthly_data.empty:
#     # Transpose and format
#     monthly_data = monthly_data.set_index("Month").T

#     # Define the desired order for the rows
#     row_order = ["Sales Actual", "Sales Objective", "% to Objective", "Revenue Actual", "Commission Payout", "SHS Margin"]
#     monthly_data = monthly_data.reindex(row_order)

#     # Remove duplicate columns by keeping only the first occurrence
#     monthly_data = monthly_data.loc[:, ~monthly_data.columns.duplicated()]

#     # Ensure all 12 months exist in the data before plotting
#     all_months = [
#         "January", "February", "March", "April", "May", "June",
#         "July", "August", "September", "October", "November", "December"
#     ]

#     # Extract Sales Actual and Sales Objective as numeric lists
#     sales_actual = [float(str(monthly_data.loc["Sales Actual", m]).replace("$", "").replace(",", "")) 
#                     if m in monthly_data.columns else 0 for m in all_months]

#     sales_objective = [float(str(monthly_data.loc["Sales Objective", m]).replace("$", "").replace(",", "")) 
#                     if m in monthly_data.columns else 0 for m in all_months]

#     # Ensure missing values in Sales Actual are replaced with 0
#     sales_actual = [x if x > 0 else 0 for x in sales_actual]

#     # Plotting the Bar Chart
#     fig, ax = plt.subplots(figsize=(12, 3))  # Adjusted height for a shorter graph
#     ax.bar(all_months, sales_actual, label="Sales Actual", color="blue", alpha=0.7)
#     ax.bar(all_months, sales_objective, label="Sales Objective", color="orange", alpha=0.7, width=0.4, align="edge")
#     ax.set_title("Sales vs Sales Objective", fontsize=16, fontweight="bold")
#     ax.set_xlabel("Months", fontsize=12)
#     ax.set_ylabel("Sales ($)", fontsize=12)
#     ax.legend()
#     ax.grid(axis="y", linestyle="--", alpha=0.7)

#     # Format y-axis as currency
#     ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))

#     # Reduce font size of x-axis labels
#     ax.set_xticklabels(all_months, rotation=45, ha="right", fontsize=9)  # Smaller font size (was default ~12)

#     # Display the chart in Streamlit
#     st.pyplot(fig)

#     # Format numeric values
#     for col in monthly_data.columns:
#         monthly_data[col] = monthly_data[col].apply(
#             lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) and not str(x).endswith('%') else x
#         )

#     # Display the DataFrame
#     st.subheader("Monthly Performance Summary")
#     st.dataframe(monthly_data, use_container_width=True)

#     # Fetch and display the data_status table (Non-Editable)
#     def fetch_data_status():
#         """Fetch the full data_status table."""
#         query = "SELECT * FROM data_status"
#         engine = get_db_connection()
#         try:
#             with engine.connect() as conn:
#                 data_status = pd.read_sql_query(query, conn)
#             return data_status
#         except Exception as e:
#             st.error(f"Error fetching data status table: {e}")
#             return pd.DataFrame()
#         finally:
#             engine.dispose()

#     # Fetch Data Status
#     data_status_df = fetch_data_status()

#     # Display the DataFrame only if it has data
#     if not data_status_df.empty:
#         st.subheader("Data Upload Status")
        
#         # Ensure boolean columns are correctly formatted
#         boolean_columns = [col for col in data_status_df.columns if col != "Product line"]
#         for col in boolean_columns:
#             data_status_df[col] = data_status_df[col].fillna(False).astype(bool)
#         data_status_df["Product line"] = data_status_df["Product line"].astype(str)

#         # Display the table (Non-Editable)
#         st.dataframe(data_status_df, use_container_width=True)
#     else:
#         st.warning("No data available in the data_status table.")

# else:
#     st.warning("No data available for the selected filters.")

### NEW CODE ###
# import streamlit as st
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# import os
# import matplotlib.pyplot as plt

# # Load environment variables
# load_dotenv()
# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
#                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# def get_unique_years():
#     """Fetch distinct years from the sales_rep_business_objective table."""
#     query = """
#         SELECT DISTINCT "Year"
#         FROM sales_rep_business_objective
#         ORDER BY "Year"
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query))
#             years = [row[0] for row in result.fetchall()]
#         return years
#     except Exception as e:
#         st.error(f"Error fetching years: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_salespeople_by_year(selected_year):
#     """Fetch distinct salespeople from harmonised_table filtered by year."""
#     query = """
#         SELECT DISTINCT "Sales Rep"
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#         ORDER BY "Sales Rep"
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query), {"year": str(selected_year)})
#             salespeople = [row[0] for row in result.fetchall()]
#         return salespeople
#     except Exception as e:
#         st.error(f"Error fetching salespeople: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson):
#     """Fetch distinct product lines from harmonised_table filtered by year and salesperson."""
#     query = """
#         SELECT DISTINCT "Product Line"
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {salesperson_filter}
#         ORDER BY "Product Line"
#     """
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
#     query = query.format(salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             product_lines = [row[0] for row in result.fetchall()]
#         return product_lines
#     except Exception as e:
#         st.error(f"Error fetching product lines: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_ytd_sales_actual(selected_year, selected_product_line, selected_salesperson):
#     """Fetch YTD sales actual from the harmonised_table."""
#     query = """
#         SELECT SUM("Sales Actual") AS ytd_sales_actual
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": selected_year}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_sales_actual = result.scalar() or 0
#         return ytd_sales_actual
#     except Exception as e:
#         st.error(f"Error fetching YTD Sales Actual: {e}")
#         return 0
#     finally:
#         engine.dispose()

# def get_ytd_revenue_actual(selected_year, selected_product_line, selected_salesperson):
#     """Fetch YTD revenue actual from the harmonised_table."""
#     query = """
#         SELECT SUM("Rev Actual") AS ytd_revenue_actual
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": selected_year}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_revenue_actual = result.scalar() or 0
#         return ytd_revenue_actual
#     except Exception as e:
#         st.error(f"Error fetching YTD Revenue Actual: {e}")
#         return 0
#     finally:
#         engine.dispose()

# def get_ytd_shs_margin(selected_year, selected_product_line, selected_salesperson):
#     """Fetch the YTD SHS Margin from the harmonised_table."""
#     query = """
#         SELECT SUM("SHS Margin") AS ytd_shs_margin
#         FROM harmonised_table
#         WHERE "Date YYYY" = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_shs_margin = result.scalar()
#         return ytd_shs_margin if ytd_shs_margin else 0.0
#     except Exception as e:
#         st.error(f"Error fetching YTD SHS Margin: {e}")
#         return 0.0
#     finally:
#         engine.dispose()

# def get_ytd_commission_payout(selected_year, selected_product_line, selected_salesperson):
#     """Fetch the YTD Commission Payout dynamically."""
#     query = """
#         SELECT 
#             SUM(
#                 CASE 
#                     WHEN "Commission tier 2 date" IS NULL 
#                     THEN "Comm Amount tier 1"
#                     WHEN SPLIT_PART("Commission tier 2 date", '-', 2)::INTEGER < "Date MM"::INTEGER
#                     THEN "Comm Amount tier 1"
#                     ELSE "Comm Amount tier 1" + "Comm tier 2 diff amount"
#                 END
#             ) AS ytd_commission_payout
#         FROM harmonised_table
#         WHERE "Date YYYY" = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_commission_payout = result.scalar() or 0.0
#         return ytd_commission_payout
#     except Exception as e:
#         st.error(f"Error fetching YTD Commission Payout: {e}")
#         return 0.0
#     finally:
#         engine.dispose()

# def fetch_monthly_data(selected_year, selected_product_line, selected_salesperson):
#     """
#     Fetch monthly sales performance data including dynamically calculated commission values.
#     Now includes 'Sales Objective' retrieved from sales_rep_business_objective.
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             # --- SQL Query with additional columns for payout calculation ---
#             query = """
#                 WITH commission_tier2 AS (
#                     SELECT 
#                         "Date MM"::INTEGER AS month_number,
#                         "Sales Rep",
#                         "Product Line",
#                         SUM("Comm tier 2 diff amount") AS payback
#                     FROM harmonised_table
#                     WHERE "Date YYYY" = :year
#                     GROUP BY "Date MM", "Sales Rep", "Product Line"
#                 )
#                 SELECT 
#                     h."Date MM"::INTEGER AS month_number,
#                     SUM(h."Sales Actual") AS Sales_Actual,
#                     SUM(h."Rev Actual") AS Revenue_Actual,
#                     SUM(
#                         CASE 
#                             WHEN h."Commission tier 2 date" IS NULL 
#                             THEN h."Comm Amount tier 1"
#                             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER > h."Date MM"::INTEGER
#                             THEN h."Comm Amount tier 1"
#                             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER = h."Date MM"::INTEGER
#                             THEN h."Comm Amount tier 1" + h."Comm tier 2 diff amount" + COALESCE(ct.payback, 0)
#                             ELSE h."Comm Amount tier 1" + h."Comm tier 2 diff amount"
#                         END
#                     ) AS Commission_Amount,
#                     -- These columns will be used in Python to compute Commission Payout using iterative logic:
#                     SUM(h."Comm Amount tier 1") AS tier1_sum,
#                     SUM(h."Comm tier 2 diff amount") AS tier2_sum,
#                     MAX(h."Commission tier 2 date") AS tier2_date,
#                     -- Fetch Sales Objective from sales_rep_business_objective with explicit type casting
#                     COALESCE(
#                         (
#                             SELECT sbo."Objective"
#                             FROM sales_rep_business_objective sbo
#                             WHERE sbo."Product line" = h."Product Line"
#                             AND sbo."Sales Rep name" = h."Sales Rep"
#                             AND sbo."Month" = h."Date MM"::INTEGER
#                             AND sbo."Year" = CAST(h."Date YYYY" AS INTEGER)  -- Explicit type casting fix
#                             LIMIT 1
#                         ), 0
#                     ) AS Sales_Objective
#                 FROM harmonised_table h
#                 LEFT JOIN commission_tier2 ct
#                   ON h."Date MM"::INTEGER = ct.month_number
#                  AND h."Sales Rep" = ct."Sales Rep"
#                  AND h."Product Line" = ct."Product Line"
#                 WHERE h."Date YYYY" = :year
#                   {product_line_filter}
#                   {salesperson_filter}
#                 GROUP BY h."Date MM", h."Product Line", h."Sales Rep", h."Date YYYY"
#                 ORDER BY month_number;
#             """
#             # Apply filtering logic dynamically
#             product_line_filter = 'AND h."Product Line" = :product_line' if selected_product_line != "All" else ""
#             salesperson_filter = 'AND h."Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
#             query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             # Load the query results into a DataFrame
#             data = pd.DataFrame(result.fetchall(), columns=[
#                 "month_number", "Sales Actual", "Revenue Actual", "Commission_Amount",
#                 "tier1_sum", "tier2_sum", "tier2_date", "Sales Objective"
#             ])
#             data["month_number"] = data["month_number"].astype(int)
#             data = data.sort_values("month_number")
            
#             # --- Compute Commission Payout in Python using iterative (cumulative) logic ---
#             payout = 0
#             commission_payout_list = []
#             for _, row in data.iterrows():
#                 month_num = int(row["month_number"])
#                 month_str = str(month_num).zfill(2)
#                 tier1 = float(row["tier1_sum"]) if pd.notnull(row["tier1_sum"]) else 0
#                 tier2 = float(row["tier2_sum"]) if pd.notnull(row["tier2_sum"]) else 0
#                 tier2_date = row["tier2_date"]  # may be None or a string like "2023-04"
#                 if pd.isnull(tier2_date):
#                     commission_payout = tier1
#                     payout += tier2
#                 elif tier2_date == f"{selected_year}-{month_str}":
#                     commission_payout = tier1 + tier2 + payout
#                     payout = 0
#                 else:
#                     commission_payout = tier1 + tier2
#                 commission_payout_list.append(commission_payout)
#             data["Commission Payout"] = commission_payout_list
#             # Remove temporary columns no longer needed.
#             data.drop(columns=["tier1_sum", "tier2_sum", "tier2_date"], inplace=True)
#             # Convert month_number to full month names.
#             data["Month"] = data["month_number"].apply(lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
            
#             # Ensure all 12 months are present.
#             all_months_df = pd.DataFrame({"month_number": list(range(1, 13))})
#             merged_df = pd.merge(all_months_df, data, on="month_number", how="left").fillna(0)
#             merged_df["Month"] = merged_df["month_number"].apply(lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
            
#             # Ensure numeric columns are float.
#             numeric_columns = ["Sales Actual", "Revenue Actual", "Commission_Amount", "Commission Payout", "Sales Objective"]
#             merged_df[numeric_columns] = merged_df[numeric_columns].astype(float)

#             # --- Compute % to Objective ---
#             merged_df["% to Objective"] = merged_df.apply(
#                 lambda row: f"{(row['Sales Actual'] / row['Sales Objective'] * 100):.2f}%" 
#                 if row["Sales Objective"] > 0 else "0.00%", axis=1
#             )

#             # --- Compute SHS Margin as Sales Actual minus Commission Payout ---
#             merged_df["SHS Margin"] = merged_df["Revenue Actual"] - merged_df["Commission Payout"]
            
#             return merged_df

#     except Exception as e:
#         st.error(f"Error fetching monthly data: {e}")
#         return pd.DataFrame()
#     finally:
#         engine.dispose()



# def get_data_status_summary():
#     """Fetch data status and calculate summary."""
#     query = "SELECT * FROM data_status"
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             df = pd.read_sql_query(query, conn)
#             boolean_df = df.iloc[:, 1:].replace({'t': True, 'f': False})
#             df["Total Months"] = boolean_df.sum(axis=1)
#             total_months_in_year = 12
#             summary = [
#                 f"{row['Product line']}: {row['Total Months']}/{total_months_in_year}"
#                 for _, row in df.iterrows()
#             ]
#         return summary
#     except Exception as e:
#         st.error(f"Error fetching data status summary: {e}")
#         return []
#     finally:
#         engine.dispose()

# # Streamlit UI
# st.title("Sales Performance")

# # Layout with two columns
# col1, col2 = st.columns([1, 1])

# # Column 1: Filters
# with col1:
#     years = get_unique_years()
#     if not years:
#         st.warning("No years available.")
#     else:
#         selected_year = st.selectbox("Select a Year:", years)
#         if selected_year:
#             salespeople = get_salespeople_by_year(selected_year)
#             salespeople.insert(0, "All")
#             selected_salesperson = st.selectbox("Choose a Salesperson:", salespeople)
#             product_lines = get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson)
#             product_lines.insert(0, "All")
#             selected_product_line = st.selectbox("Choose a Product Line:", product_lines)

# # Generate Monthly DataFrame
# monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)

# # Ensure `monthly_data` is defined and has data
# if monthly_data is None or monthly_data.empty:
#     monthly_data = pd.DataFrame()

# # Column 2: YTD Summary
# with col2:
#     if not monthly_data.empty:
#         # Convert numeric columns from formatted strings (like "$12,345.67") to floats for summation
#         def parse_currency(value):
#             """Convert formatted currency strings to float."""
#             try:
#                 return float(str(value).replace("$", "").replace(",", "").replace("%", ""))
#             except ValueError:
#                 return 0.0  # Handle cases where data is missing or incorrectly formatted

#         # Ensure numeric data is properly converted
#         numeric_columns = ["Sales Actual", "Revenue Actual", "SHS Margin", "Commission Payout"]
#         for col in numeric_columns:
#             if col in monthly_data.columns:
#                 monthly_data[col] = monthly_data[col].apply(parse_currency)

#         # Correctly compute YTD values
#         ytd_sales_actual = monthly_data["Sales Actual"].sum() if "Sales Actual" in monthly_data.columns else 0.0
#         ytd_revenue_actual = monthly_data["Revenue Actual"].sum() if "Revenue Actual" in monthly_data.columns else 0.0
#         ytd_shs_margin = monthly_data["SHS Margin"].sum() if "SHS Margin" in monthly_data.columns else 0.0
#         ytd_commission_payout = monthly_data["Commission Payout"].sum() if "Commission Payout" in monthly_data.columns else 0.0

#         # Display YTD summary
#         st.markdown(
#             f"""
#             <div style="text-align: right; font-size: 1.5em; font-weight: bold;">
#                 YTD Sales Actual: ${ytd_sales_actual:,.2f}<br/>
#                 YTD Revenue Actual: ${ytd_revenue_actual:,.2f}<br/>
#                 YTD SHS Margin: ${ytd_shs_margin:,.2f}<br/>
#                 YTD Commission Payout: ${ytd_commission_payout:,.2f}
#             </div>
#             """,
#             unsafe_allow_html=True,
#         )
#     else:
#         st.warning("No data available for the selected filters.")

# # Generate Monthly DataFrame
# monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)

# if not monthly_data.empty:
#     # Transpose and format for display
#     monthly_data = monthly_data.set_index("Month").T
#     # Define the desired order for the rows
#     row_order = ["Sales Actual", "Sales Objective", "% to Objective", 
#                  "Revenue Actual", "Commission Payout", "SHS Margin"]
#     monthly_data = monthly_data.reindex(row_order)
#     # Remove duplicate columns (if any)
#     monthly_data = monthly_data.loc[:, ~monthly_data.columns.duplicated()]
#     # Ensure all 12 months are present
#     all_months = [
#         "January", "February", "March", "April", "May", "June",
#         "July", "August", "September", "October", "November", "December"
#     ]
#     # Extract Sales Actual and Sales Objective as numeric lists for plotting
#     sales_actual = [float(str(monthly_data.loc["Sales Actual", m]).replace("$", "").replace(",", "")) 
#                     if m in monthly_data.columns else 0 for m in all_months]
#     sales_objective = [float(str(monthly_data.loc["Sales Objective", m]).replace("$", "").replace(",", "")) 
#                     if m in monthly_data.columns else 0 for m in all_months]
#     sales_actual = [x if x > 0 else 0 for x in sales_actual]
#     # Plotting the Bar Chart
#     fig, ax = plt.subplots(figsize=(12, 3))
#     ax.bar(all_months, sales_actual, label="Sales Actual", color="blue", alpha=0.7)
#     ax.bar(all_months, sales_objective, label="Sales Objective", color="orange", alpha=0.7, width=0.4, align="edge")
#     ax.set_title("Sales vs Sales Objective", fontsize=16, fontweight="bold")
#     ax.set_xlabel("Months", fontsize=12)
#     ax.set_ylabel("Sales ($)", fontsize=12)
#     ax.legend()
#     ax.grid(axis="y", linestyle="--", alpha=0.7)
#     ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
#     ax.set_xticklabels(all_months, rotation=45, ha="right", fontsize=9)
#     st.pyplot(fig)
#     # Format numeric values as currency
#     for col in monthly_data.columns:
#         monthly_data[col] = monthly_data[col].apply(
#             lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) and not str(x).endswith('%') else x
#         )
#     st.subheader("Monthly Performance Summary")
#     st.dataframe(monthly_data, use_container_width=True)
#     # Data Status Table (Non-Editable)
#     def fetch_data_status():
#         query = "SELECT * FROM data_status"
#         engine = get_db_connection()
#         try:
#             with engine.connect() as conn:
#                 data_status = pd.read_sql_query(query, conn)
#             return data_status
#         except Exception as e:
#             st.error(f"Error fetching data status table: {e}")
#             return pd.DataFrame()
#         finally:
#             engine.dispose()
#     data_status_df = fetch_data_status()
#     if not data_status_df.empty:
#         st.subheader("Data Upload Status")
#         boolean_columns = [col for col in data_status_df.columns if col != "Product line"]
#         for col in boolean_columns:
#             data_status_df[col] = data_status_df[col].fillna(False).astype(bool)
#         data_status_df["Product line"] = data_status_df["Product line"].astype(str)
#         st.dataframe(data_status_df, use_container_width=True)
#     else:
#         st.warning("No data available in the data_status table.")
# else:
#     st.warning("No data available for the selected filters.")

### TOTAL FIXED FOR ALL OPTION, STILL OBJECTIVE TO BE FIXED ####
# import streamlit as st
# import pandas as pd
# from sqlalchemy import create_engine, text
# from dotenv import load_dotenv
# import os
# import matplotlib.pyplot as plt

# # Load environment variables
# load_dotenv()
# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
#                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# def get_unique_years():
#     """Fetch distinct years from the sales_rep_business_objective table."""
#     query = """
#         SELECT DISTINCT "Year"
#         FROM sales_rep_business_objective
#         ORDER BY "Year"
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query))
#             years = [row[0] for row in result.fetchall()]
#         return years
#     except Exception as e:
#         st.error(f"Error fetching years: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_salespeople_by_year(selected_year):
#     """Fetch distinct salespeople from harmonised_table filtered by year."""
#     query = """
#         SELECT DISTINCT "Sales Rep"
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#         ORDER BY "Sales Rep"
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text(query), {"year": str(selected_year)})
#             salespeople = [row[0] for row in result.fetchall()]
#         return salespeople
#     except Exception as e:
#         st.error(f"Error fetching salespeople: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson):
#     """Fetch distinct product lines from harmonised_table filtered by year and salesperson."""
#     query = """
#         SELECT DISTINCT "Product Line"
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {salesperson_filter}
#         ORDER BY "Product Line"
#     """
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
#     query = query.format(salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             product_lines = [row[0] for row in result.fetchall()]
#         return product_lines
#     except Exception as e:
#         st.error(f"Error fetching product lines: {e}")
#         return []
#     finally:
#         engine.dispose()

# def get_ytd_sales_actual(selected_year, selected_product_line, selected_salesperson):
#     """Fetch YTD sales actual from the harmonised_table."""
#     query = """
#         SELECT SUM("Sales Actual") AS ytd_sales_actual
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": selected_year}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_sales_actual = result.scalar() or 0
#         return ytd_sales_actual
#     except Exception as e:
#         st.error(f"Error fetching YTD Sales Actual: {e}")
#         return 0
#     finally:
#         engine.dispose()

# def get_ytd_revenue_actual(selected_year, selected_product_line, selected_salesperson):
#     """Fetch YTD revenue actual from the harmonised_table."""
#     query = """
#         SELECT SUM("Rev Actual") AS ytd_revenue_actual
#         FROM harmonised_table
#         WHERE CAST("Date YYYY" AS INTEGER) = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = ""
#     if selected_salesperson != "All":
#         salesperson_filter = 'AND "Sales Rep" = :salesperson'
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": selected_year}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_revenue_actual = result.scalar() or 0
#         return ytd_revenue_actual
#     except Exception as e:
#         st.error(f"Error fetching YTD Revenue Actual: {e}")
#         return 0
#     finally:
#         engine.dispose()

# def get_ytd_shs_margin(selected_year, selected_product_line, selected_salesperson):
#     """Fetch the YTD SHS Margin from the harmonised_table."""
#     query = """
#         SELECT SUM("SHS Margin") AS ytd_shs_margin
#         FROM harmonised_table
#         WHERE "Date YYYY" = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_shs_margin = result.scalar()
#         return ytd_shs_margin if ytd_shs_margin else 0.0
#     except Exception as e:
#         st.error(f"Error fetching YTD SHS Margin: {e}")
#         return 0.0
#     finally:
#         engine.dispose()

# def get_ytd_commission_payout(selected_year, selected_product_line, selected_salesperson):
#     """Fetch the YTD Commission Payout dynamically."""
#     query = """
#         SELECT 
#             SUM(
#                 CASE 
#                     WHEN "Commission tier 2 date" IS NULL 
#                     THEN "Comm Amount tier 1"
#                     WHEN SPLIT_PART("Commission tier 2 date", '-', 2)::INTEGER < "Date MM"::INTEGER
#                     THEN "Comm Amount tier 1"
#                     ELSE "Comm Amount tier 1" + "Comm tier 2 diff amount"
#                 END
#             ) AS ytd_commission_payout
#         FROM harmonised_table
#         WHERE "Date YYYY" = :year
#           {product_line_filter}
#           {salesperson_filter}
#     """
#     product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
#     salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
#     query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
#             ytd_commission_payout = result.scalar() or 0.0
#         return ytd_commission_payout
#     except Exception as e:
#         st.error(f"Error fetching YTD Commission Payout: {e}")
#         return 0.0
#     finally:
#         engine.dispose()

# def fetch_monthly_data(selected_year, selected_product_line, selected_salesperson):
#     """
#     Fetch monthly sales performance data including dynamically calculated commission values.
#     Now includes 'Sales Objective' retrieved from sales_rep_business_objective.
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             # --- SQL Query with additional columns for payout calculation ---
#             query = """
#                 WITH commission_tier2 AS (
#                     SELECT 
#                         "Date MM"::INTEGER AS month_number,
#                         "Sales Rep",
#                         "Product Line",
#                         SUM("Comm tier 2 diff amount") AS payback
#                     FROM harmonised_table
#                     WHERE "Date YYYY" = :year
#                     GROUP BY "Date MM", "Sales Rep", "Product Line"
#                 )
#                 SELECT 
#                     h."Date MM"::INTEGER AS month_number,
#                     SUM(h."Sales Actual") AS "Sales Actual",
#                     SUM(h."Rev Actual") AS "Revenue Actual",
#                     SUM(
#                         CASE 
#                             WHEN h."Commission tier 2 date" IS NULL 
#                             THEN h."Comm Amount tier 1"
#                             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER > h."Date MM"::INTEGER
#                             THEN h."Comm Amount tier 1"
#                             WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER = h."Date MM"::INTEGER
#                             THEN h."Comm Amount tier 1" + h."Comm tier 2 diff amount" + COALESCE(ct.payback, 0)
#                             ELSE h."Comm Amount tier 1" + h."Comm tier 2 diff amount"
#                         END
#                     ) AS "Commission_Amount",
#                     -- These columns will be used in Python to compute Commission Payout using iterative logic:
#                     SUM(h."Comm Amount tier 1") AS tier1_sum,
#                     SUM(h."Comm tier 2 diff amount") AS tier2_sum,
#                     MAX(h."Commission tier 2 date") AS tier2_date,
#                     -- Fetch Sales Objective from sales_rep_business_objective with explicit type casting
#                     COALESCE(
#                         (
#                             SELECT sbo."Objective"
#                             FROM sales_rep_business_objective sbo
#                             WHERE sbo."Product line" = h."Product Line"
#                               AND sbo."Sales Rep name" = h."Sales Rep"
#                               AND sbo."Month" = h."Date MM"::INTEGER
#                               AND sbo."Year" = CAST(h."Date YYYY" AS INTEGER)
#                             LIMIT 1
#                         ), 0
#                     ) AS "Sales Objective"
#                 FROM harmonised_table h
#                 LEFT JOIN commission_tier2 ct
#                   ON h."Date MM"::INTEGER = ct.month_number
#                  AND h."Sales Rep" = ct."Sales Rep"
#                  AND h."Product Line" = ct."Product Line"
#                 WHERE h."Date YYYY" = :year
#                   {product_line_filter}
#                   {salesperson_filter}
#                 GROUP BY h."Date MM", h."Product Line", h."Sales Rep", h."Date YYYY"
#                 ORDER BY month_number;
#             """
#             # Apply filtering logic dynamically
#             product_line_filter = 'AND h."Product Line" = :product_line' if selected_product_line != "All" else ""
#             salesperson_filter = 'AND h."Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
#             query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
#             params = {"year": str(selected_year)}
#             if selected_product_line != "All":
#                 params["product_line"] = selected_product_line
#             if selected_salesperson != "All":
#                 params["salesperson"] = selected_salesperson
#             result = conn.execute(text(query), params)
            
#             # Load the query results into a DataFrame
#             data = pd.DataFrame(result.fetchall(), columns=[
#                 "month_number", "Sales Actual", "Revenue Actual", "Commission_Amount",
#                 "tier1_sum", "tier2_sum", "tier2_date", "Sales Objective"
#             ])
#             data["month_number"] = data["month_number"].astype(int)
#             data = data.sort_values("month_number")
            
#             # --- Compute Commission Payout in Python using iterative (cumulative) logic ---
#             payout = 0
#             commission_payout_list = []
#             for _, row in data.iterrows():
#                 month_num = int(row["month_number"])
#                 month_str = str(month_num).zfill(2)
#                 tier1 = float(row["tier1_sum"]) if pd.notnull(row["tier1_sum"]) else 0
#                 tier2 = float(row["tier2_sum"]) if pd.notnull(row["tier2_sum"]) else 0
#                 tier2_date = row["tier2_date"]  # may be None or a string like "2023-04"
#                 if pd.isnull(tier2_date):
#                     commission_payout = tier1
#                     payout += tier2
#                 elif tier2_date == f"{selected_year}-{month_str}":
#                     commission_payout = tier1 + tier2 + payout
#                     payout = 0
#                 else:
#                     commission_payout = tier1 + tier2
#                 commission_payout_list.append(commission_payout)
#             data["Commission Payout"] = commission_payout_list
            
#             # Remove temporary columns no longer needed.
#             data.drop(columns=["tier1_sum", "tier2_sum", "tier2_date"], inplace=True)
#             # Convert month_number to full month names.
#             data["Month"] = data["month_number"].apply(lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
            
#             # --- Approach 2: Post-Aggregation in Python if both filters are "All" ---
#             if selected_product_line == "All" and selected_salesperson == "All":
#                 aggregated = data.groupby("month_number", as_index=False).agg({
#                     "Sales Actual": "sum",
#                     "Revenue Actual": "sum",
#                     "Commission_Amount": "sum",
#                     "Sales Objective": "sum",
#                     "Commission Payout": "sum"
#                 })
#                 aggregated["Month"] = aggregated["month_number"].apply(
#                     lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
#                 data = aggregated
            
#             # Ensure all 12 months are present.
#             all_months_df = pd.DataFrame({"month_number": list(range(1, 13))})
#             merged_df = pd.merge(all_months_df, data, on="month_number", how="left").fillna(0)
#             merged_df["Month"] = merged_df["month_number"].apply(
#                 lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
            
#             # Ensure numeric columns are float.
#             numeric_columns = ["Sales Actual", "Revenue Actual", "Commission_Amount", "Commission Payout", "Sales Objective"]
#             merged_df[numeric_columns] = merged_df[numeric_columns].astype(float)

#             # --- Compute % to Objective ---
#             merged_df["% to Objective"] = merged_df.apply(
#                 lambda row: f"{(row['Sales Actual'] / row['Sales Objective'] * 100):.2f}%" 
#                 if row["Sales Objective"] > 0 else "0.00%", axis=1
#             )

#             # --- Compute SHS Margin as Sales Actual minus Commission Payout ---
#             merged_df["SHS Margin"] = merged_df["Revenue Actual"] - merged_df["Commission Payout"]
            
#             return merged_df

#     except Exception as e:
#         st.error(f"Error fetching monthly data: {e}")
#         return pd.DataFrame()
#     finally:
#         engine.dispose()

# def get_data_status_summary():
#     """Fetch data status and calculate summary."""
#     query = "SELECT * FROM data_status"
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             df = pd.read_sql_query(query, conn)
#             boolean_df = df.iloc[:, 1:].replace({'t': True, 'f': False})
#             df["Total Months"] = boolean_df.sum(axis=1)
#             total_months_in_year = 12
#             summary = [
#                 f"{row['Product line']}: {row['Total Months']}/{total_months_in_year}"
#                 for _, row in df.iterrows()
#             ]
#         return summary
#     except Exception as e:
#         st.error(f"Error fetching data status summary: {e}")
#         return []
#     finally:
#         engine.dispose()

# # Streamlit UI
# st.title("Sales Performance")

# # Layout with two columns
# col1, col2 = st.columns([1, 1])

# # Column 1: Filters
# with col1:
#     years = get_unique_years()
#     if not years:
#         st.warning("No years available.")
#     else:
#         selected_year = st.selectbox("Select a Year:", years)
#         if selected_year:
#             salespeople = get_salespeople_by_year(selected_year)
#             salespeople.insert(0, "All")
#             selected_salesperson = st.selectbox("Choose a Salesperson:", salespeople)
#             product_lines = get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson)
#             product_lines.insert(0, "All")
#             selected_product_line = st.selectbox("Choose a Product Line:", product_lines)

# # Generate Monthly DataFrame
# monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)

# # Ensure `monthly_data` is defined and has data
# if monthly_data is None or monthly_data.empty:
#     monthly_data = pd.DataFrame()

# # Column 2: YTD Summary
# with col2:
#     if not monthly_data.empty:
#         # Convert numeric columns from formatted strings (like "$12,345.67") to floats for summation
#         def parse_currency(value):
#             """Convert formatted currency strings to float."""
#             try:
#                 return float(str(value).replace("$", "").replace(",", "").replace("%", ""))
#             except ValueError:
#                 return 0.0  # Handle cases where data is missing or incorrectly formatted

#         # Ensure numeric data is properly converted
#         numeric_columns = ["Sales Actual", "Revenue Actual", "SHS Margin", "Commission Payout"]
#         for col in numeric_columns:
#             if col in monthly_data.columns:
#                 monthly_data[col] = monthly_data[col].apply(parse_currency)

#         # Correctly compute YTD values
#         ytd_sales_actual = monthly_data["Sales Actual"].sum() if "Sales Actual" in monthly_data.columns else 0.0
#         ytd_revenue_actual = monthly_data["Revenue Actual"].sum() if "Revenue Actual" in monthly_data.columns else 0.0
#         ytd_shs_margin = monthly_data["SHS Margin"].sum() if "SHS Margin" in monthly_data.columns else 0.0
#         ytd_commission_payout = monthly_data["Commission Payout"].sum() if "Commission Payout" in monthly_data.columns else 0.0

#         # Display YTD summary
#         st.markdown(
#             f"""
#             <div style="text-align: right; font-size: 1.5em; font-weight: bold;">
#                 YTD Sales Actual: ${ytd_sales_actual:,.2f}<br/>
#                 YTD Revenue Actual: ${ytd_revenue_actual:,.2f}<br/>
#                 YTD SHS Margin: ${ytd_shs_margin:,.2f}<br/>
#                 YTD Commission Payout: ${ytd_commission_payout:,.2f}
#             </div>
#             """,
#             unsafe_allow_html=True,
#         )
#     else:
#         st.warning("No data available for the selected filters.")

# # Generate Monthly DataFrame for display (again)
# monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)

# if not monthly_data.empty:
#     # Transpose and format for display
#     monthly_data = monthly_data.set_index("Month").T
#     # Define the desired order for the rows
#     row_order = ["Sales Actual", "Sales Objective", "% to Objective", 
#                  "Revenue Actual", "Commission Payout", "SHS Margin"]
#     monthly_data = monthly_data.reindex(row_order)
#     # Remove duplicate columns (if any)
#     monthly_data = monthly_data.loc[:, ~monthly_data.columns.duplicated()]
#     # Ensure all 12 months are present
#     all_months = [
#         "January", "February", "March", "April", "May", "June",
#         "July", "August", "September", "October", "November", "December"
#     ]
#     # Extract Sales Actual and Sales Objective as numeric lists for plotting
#     sales_actual = [float(str(monthly_data.loc["Sales Actual", m]).replace("$", "").replace(",", "")) 
#                     if m in monthly_data.columns else 0 for m in all_months]
#     sales_objective = [float(str(monthly_data.loc["Sales Objective", m]).replace("$", "").replace(",", "")) 
#                     if m in monthly_data.columns else 0 for m in all_months]
#     sales_actual = [x if x > 0 else 0 for x in sales_actual]
#     # Plotting the Bar Chart
#     fig, ax = plt.subplots(figsize=(12, 3))
#     ax.bar(all_months, sales_actual, label="Sales Actual", color="blue", alpha=0.7)
#     ax.bar(all_months, sales_objective, label="Sales Objective", color="orange", alpha=0.7, width=0.4, align="edge")
#     ax.set_title("Sales vs Sales Objective", fontsize=16, fontweight="bold")
#     ax.set_xlabel("Months", fontsize=12)
#     ax.set_ylabel("Sales ($)", fontsize=12)
#     ax.legend()
#     ax.grid(axis="y", linestyle="--", alpha=0.7)
#     ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
#     ax.set_xticklabels(all_months, rotation=45, ha="right", fontsize=9)
#     st.pyplot(fig)
#     # Format numeric values as currency
#     for col in monthly_data.columns:
#         monthly_data[col] = monthly_data[col].apply(
#             lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) and not str(x).endswith('%') else x
#         )
#     st.subheader("Monthly Performance Summary")
#     st.dataframe(monthly_data, use_container_width=True)
    
#     # Data Status Table (Non-Editable)
#     def fetch_data_status():
#         query = "SELECT * FROM data_status"
#         engine = get_db_connection()
#         try:
#             with engine.connect() as conn:
#                 data_status = pd.read_sql_query(query, conn)
#             return data_status
#         except Exception as e:
#             st.error(f"Error fetching data status table: {e}")
#             return pd.DataFrame()
#         finally:
#             engine.dispose()
#     data_status_df = fetch_data_status()
#     if not data_status_df.empty:
#         st.subheader("Data Upload Status")
#         boolean_columns = [col for col in data_status_df.columns if col != "Product line"]
#         for col in boolean_columns:
#             data_status_df[col] = data_status_df[col].fillna(False).astype(bool)
#         data_status_df["Product line"] = data_status_df["Product line"].astype(str)
#         st.dataframe(data_status_df, use_container_width=True)
#     else:
#         st.warning("No data available in the data_status table.")
# else:
#     st.warning("No data available for the selected filters.")

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt

# Load environment variables
load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def get_unique_years():
    """Fetch distinct years from the sales_rep_business_objective table."""
    query = """
        SELECT DISTINCT "Year"
        FROM sales_rep_business_objective
        ORDER BY "Year"
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

def get_salespeople_by_year(selected_year):
    """Fetch distinct salespeople from harmonised_table filtered by year."""
    query = """
        SELECT DISTINCT "Sales Rep"
        FROM harmonised_table
        WHERE CAST("Date YYYY" AS INTEGER) = :year
        ORDER BY "Sales Rep"
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"year": str(selected_year)})
            salespeople = [row[0] for row in result.fetchall()]
        return salespeople
    except Exception as e:
        st.error(f"Error fetching salespeople: {e}")
        return []
    finally:
        engine.dispose()

def get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson):
    """Fetch distinct product lines from harmonised_table filtered by year and salesperson."""
    query = """
        SELECT DISTINCT "Product Line"
        FROM harmonised_table
        WHERE CAST("Date YYYY" AS INTEGER) = :year
          {salesperson_filter}
        ORDER BY "Product Line"
    """
    salesperson_filter = ""
    if selected_salesperson != "All":
        salesperson_filter = 'AND "Sales Rep" = :salesperson'
    query = query.format(salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": str(selected_year)}
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            product_lines = [row[0] for row in result.fetchall()]
        return product_lines
    except Exception as e:
        st.error(f"Error fetching product lines: {e}")
        return []
    finally:
        engine.dispose()

def get_ytd_sales_actual(selected_year, selected_product_line, selected_salesperson):
    """Fetch YTD sales actual from the harmonised_table."""
    query = """
        SELECT SUM("Sales Actual") AS ytd_sales_actual
        FROM harmonised_table
        WHERE CAST("Date YYYY" AS INTEGER) = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
    salesperson_filter = ""
    if selected_salesperson != "All":
        salesperson_filter = 'AND "Sales Rep" = :salesperson'
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": selected_year}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_sales_actual = result.scalar() or 0
        return ytd_sales_actual
    except Exception as e:
        st.error(f"Error fetching YTD Sales Actual: {e}")
        return 0
    finally:
        engine.dispose()

def get_ytd_revenue_actual(selected_year, selected_product_line, selected_salesperson):
    """Fetch YTD revenue actual from the harmonised_table."""
    query = """
        SELECT SUM("Rev Actual") AS ytd_revenue_actual
        FROM harmonised_table
        WHERE CAST("Date YYYY" AS INTEGER) = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
    salesperson_filter = ""
    if selected_salesperson != "All":
        salesperson_filter = 'AND "Sales Rep" = :salesperson'
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": selected_year}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_revenue_actual = result.scalar() or 0
        return ytd_revenue_actual
    except Exception as e:
        st.error(f"Error fetching YTD Revenue Actual: {e}")
        return 0
    finally:
        engine.dispose()

def get_ytd_shs_margin(selected_year, selected_product_line, selected_salesperson):
    """Fetch the YTD SHS Margin from the harmonised_table."""
    query = """
        SELECT SUM("SHS Margin") AS ytd_shs_margin
        FROM harmonised_table
        WHERE "Date YYYY" = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
    salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": str(selected_year)}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_shs_margin = result.scalar()
        return ytd_shs_margin if ytd_shs_margin else 0.0
    except Exception as e:
        st.error(f"Error fetching YTD SHS Margin: {e}")
        return 0.0
    finally:
        engine.dispose()

def get_ytd_commission_payout(selected_year, selected_product_line, selected_salesperson):
    """Fetch the YTD Commission Payout dynamically."""
    query = """
        SELECT 
            SUM(
                CASE 
                    WHEN "Commission tier 2 date" IS NULL 
                    THEN "Comm Amount tier 1"
                    WHEN SPLIT_PART("Commission tier 2 date", '-', 2)::INTEGER < "Date MM"::INTEGER
                    THEN "Comm Amount tier 1"
                    ELSE "Comm Amount tier 1" + "Comm tier 2 diff amount"
                END
            ) AS ytd_commission_payout
        FROM harmonised_table
        WHERE "Date YYYY" = :year
          {product_line_filter}
          {salesperson_filter}
    """
    product_line_filter = 'AND "Product Line" = :product_line' if selected_product_line != "All" else ""
    salesperson_filter = 'AND "Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
    query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            params = {"year": str(selected_year)}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            ytd_commission_payout = result.scalar() or 0.0
        return ytd_commission_payout
    except Exception as e:
        st.error(f"Error fetching YTD Commission Payout: {e}")
        return 0.0
    finally:
        engine.dispose()

def fetch_objectives(selected_year, selected_product_line, selected_salesperson):
    """
    Fetch the monthly Sales Objective from the sales_rep_business_objective table,
    applying filters based on the user's selection.
    """
    query = """
    SELECT "Month", SUM("Objective") as "Sales Objective"
    FROM sales_rep_business_objective
    WHERE "Year" = :year
    """
    params = {"year": selected_year}
    if selected_salesperson != "All":
        query += ' AND "Sales Rep name" = :sales_rep'
        params["sales_rep"] = selected_salesperson
    if selected_product_line != "All":
        query += ' AND "Product line" = :product_line'
        params["product_line"] = selected_product_line
    query += ' GROUP BY "Month" ORDER BY "Month"::integer'
    
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, params=params)
        # Ensure the Month column is integer and rename it for merging.
        df["Month"] = df["Month"].astype(int)
        df = df.rename(columns={"Month": "month_number"})
        return df
    except Exception as e:
        st.error(f"Error fetching objectives: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def fetch_monthly_data(selected_year, selected_product_line, selected_salesperson):
    """
    Fetch monthly sales performance data (from harmonised_table) including dynamically calculated commission values.
    Then, merge in the monthly Sales Objective (Business Objective) from sales_rep_business_objective
    based on the user-selected filters.
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            # --- SQL Query with additional columns for payout calculation ---
            # Note: The inline Sales Objective subquery has been removed.
            query = """
                WITH commission_tier2 AS (
                    SELECT 
                        "Date MM"::INTEGER AS month_number,
                        "Sales Rep",
                        "Product Line",
                        SUM("Comm tier 2 diff amount") AS payback
                    FROM harmonised_table
                    WHERE "Date YYYY" = :year
                    GROUP BY "Date MM", "Sales Rep", "Product Line"
                )
                SELECT 
                    h."Date MM"::INTEGER AS month_number,
                    SUM(h."Sales Actual") AS "Sales Actual",
                    SUM(h."Rev Actual") AS "Revenue Actual",
                    SUM(
                        CASE 
                            WHEN h."Commission tier 2 date" IS NULL 
                            THEN h."Comm Amount tier 1"
                            WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER > h."Date MM"::INTEGER
                            THEN h."Comm Amount tier 1"
                            WHEN SPLIT_PART(h."Commission tier 2 date", '-', 2)::INTEGER = h."Date MM"::INTEGER
                            THEN h."Comm Amount tier 1" + h."Comm tier 2 diff amount" + COALESCE(ct.payback, 0)
                            ELSE h."Comm Amount tier 1" + h."Comm tier 2 diff amount"
                        END
                    ) AS "Commission_Amount",
                    SUM(h."Comm Amount tier 1") AS tier1_sum,
                    SUM(h."Comm tier 2 diff amount") AS tier2_sum,
                    MAX(h."Commission tier 2 date") AS tier2_date
                FROM harmonised_table h
                LEFT JOIN commission_tier2 ct
                  ON h."Date MM"::INTEGER = ct.month_number
                 AND h."Sales Rep" = ct."Sales Rep"
                 AND h."Product Line" = ct."Product Line"
                WHERE h."Date YYYY" = :year
                  {product_line_filter}
                  {salesperson_filter}
                GROUP BY h."Date MM", h."Product Line", h."Sales Rep", h."Date YYYY"
                ORDER BY month_number;
            """
            # Apply filtering logic dynamically.
            product_line_filter = 'AND h."Product Line" = :product_line' if selected_product_line != "All" else ""
            salesperson_filter = 'AND h."Sales Rep" = :salesperson' if selected_salesperson != "All" else ""
            query = query.format(product_line_filter=product_line_filter, salesperson_filter=salesperson_filter)
            params = {"year": str(selected_year)}
            if selected_product_line != "All":
                params["product_line"] = selected_product_line
            if selected_salesperson != "All":
                params["salesperson"] = selected_salesperson
            result = conn.execute(text(query), params)
            
            # Load the query results into a DataFrame.
            data = pd.DataFrame(result.fetchall(), columns=[
                "month_number", "Sales Actual", "Revenue Actual", "Commission_Amount",
                "tier1_sum", "tier2_sum", "tier2_date"
            ])
            data["month_number"] = data["month_number"].astype(int)
            data = data.sort_values("month_number")
            
            # --- Compute Commission Payout in Python using iterative logic ---
            payout = 0
            commission_payout_list = []
            for _, row in data.iterrows():
                month_num = int(row["month_number"])
                month_str = str(month_num).zfill(2)
                tier1 = float(row["tier1_sum"]) if pd.notnull(row["tier1_sum"]) else 0
                tier2 = float(row["tier2_sum"]) if pd.notnull(row["tier2_sum"]) else 0
                tier2_date = row["tier2_date"]  # may be None or a string like "2023-04"
                if pd.isnull(tier2_date):
                    commission_payout = tier1
                    payout += tier2
                elif tier2_date == f"{selected_year}-{month_str}":
                    commission_payout = tier1 + tier2 + payout
                    payout = 0
                else:
                    commission_payout = tier1 + tier2
                commission_payout_list.append(commission_payout)
            data["Commission Payout"] = commission_payout_list
            
            # Remove temporary columns no longer needed.
            data.drop(columns=["tier1_sum", "tier2_sum", "tier2_date"], inplace=True)
            # Convert month_number to full month names.
            data["Month"] = data["month_number"].apply(lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
            
            # --- If both filters are "All", aggregate rows by month ---
            if selected_product_line == "All" and selected_salesperson == "All":
                aggregated = data.groupby("month_number", as_index=False).agg({
                    "Sales Actual": "sum",
                    "Revenue Actual": "sum",
                    "Commission_Amount": "sum",
                    "Commission Payout": "sum"
                })
                aggregated["Month"] = aggregated["month_number"].apply(
                    lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
                data = aggregated
            
            # Ensure all 12 months are present.
            all_months_df = pd.DataFrame({"month_number": list(range(1, 13))})
            merged_df = pd.merge(all_months_df, data, on="month_number", how="left").fillna(0)
            merged_df["Month"] = merged_df["month_number"].apply(
                lambda x: pd.to_datetime(str(x), format="%m").strftime("%B"))
            # Ensure numeric columns are float.
            numeric_columns = ["Sales Actual", "Revenue Actual", "Commission_Amount", "Commission Payout"]
            merged_df[numeric_columns] = merged_df[numeric_columns].astype(float)
            
            # --- Merge in the Sales Objective from sales_rep_business_objective ---
            objectives_df = fetch_objectives(selected_year, selected_product_line, selected_salesperson)
            merged_df = pd.merge(merged_df, objectives_df, on="month_number", how="left").fillna(0)
            
            # --- Compute % to Objective ---
            merged_df["% to Objective"] = merged_df.apply(
                lambda row: f"{(row['Sales Actual'] / row['Sales Objective'] * 100):.2f}%"
                if row["Sales Objective"] > 0 else "0.00%", axis=1
            )
            # --- Compute SHS Margin as Revenue Actual minus Commission Payout ---
            merged_df["SHS Margin"] = merged_df["Revenue Actual"] - merged_df["Commission Payout"]
            
            return merged_df

    except Exception as e:
        st.error(f"Error fetching monthly data: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def get_data_status_summary():
    """Fetch data status and calculate summary."""
    query = "SELECT * FROM data_status"
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
            boolean_df = df.iloc[:, 1:].replace({'t': True, 'f': False})
            df["Total Months"] = boolean_df.sum(axis=1)
            total_months_in_year = 12
            summary = [
                f"{row['Product line']}: {row['Total Months']}/{total_months_in_year}"
                for _, row in df.iterrows()
            ]
        return summary
    except Exception as e:
        st.error(f"Error fetching data status summary: {e}")
        return []
    finally:
        engine.dispose()

# ----------------- Streamlit UI -----------------

st.title("Sales Performance")

# Layout with two columns
col1, col2 = st.columns([1, 1])

# Column 1: Filters
with col1:
    years = get_unique_years()
    if not years:
        st.warning("No years available.")
    else:
        selected_year = st.selectbox("Select a Year:", years)
        if selected_year:
            salespeople = get_salespeople_by_year(selected_year)
            salespeople.insert(0, "All")
            selected_salesperson = st.selectbox("Choose a Salesperson:", salespeople)
            product_lines = get_product_lines_by_year_and_salesperson(selected_year, selected_salesperson)
            product_lines.insert(0, "All")
            selected_product_line = st.selectbox("Choose a Product Line:", product_lines)

# Generate Monthly DataFrame
monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)
if monthly_data is None or monthly_data.empty:
    monthly_data = pd.DataFrame()

# Column 2: YTD Summary
with col2:
    if not monthly_data.empty:
        def parse_currency(value):
            """Convert formatted currency strings to float."""
            try:
                return float(str(value).replace("$", "").replace(",", "").replace("%", ""))
            except ValueError:
                return 0.0
        numeric_columns = ["Sales Actual", "Revenue Actual", "SHS Margin", "Commission Payout"]
        for col in numeric_columns:
            if col in monthly_data.columns:
                monthly_data[col] = monthly_data[col].apply(parse_currency)
        ytd_sales_actual = monthly_data["Sales Actual"].sum() if "Sales Actual" in monthly_data.columns else 0.0
        ytd_revenue_actual = monthly_data["Revenue Actual"].sum() if "Revenue Actual" in monthly_data.columns else 0.0
        ytd_shs_margin = monthly_data["SHS Margin"].sum() if "SHS Margin" in monthly_data.columns else 0.0
        ytd_commission_payout = monthly_data["Commission Payout"].sum() if "Commission Payout" in monthly_data.columns else 0.0

        st.markdown(
            f"""
            <div style="text-align: right; font-size: 1.5em; font-weight: bold;">
                YTD Sales Actual: ${ytd_sales_actual:,.2f}<br/>
                YTD Revenue Actual: ${ytd_revenue_actual:,.2f}<br/>
                YTD SHS Margin: ${ytd_shs_margin:,.2f}<br/>
                YTD Commission Payout: ${ytd_commission_payout:,.2f}
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.warning("No data available for the selected filters.")

# Generate Monthly DataFrame for display (again)
monthly_data = fetch_monthly_data(selected_year, selected_product_line, selected_salesperson)
if not monthly_data.empty:
    monthly_data = monthly_data.set_index("Month").T
    row_order = ["Sales Actual", "Sales Objective", "% to Objective", 
                 "Revenue Actual", "Commission Payout", "SHS Margin"]
    monthly_data = monthly_data.reindex(row_order)
    monthly_data = monthly_data.loc[:, ~monthly_data.columns.duplicated()]
    all_months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    sales_actual = [float(str(monthly_data.loc["Sales Actual", m]).replace("$", "").replace(",", ""))
                    if m in monthly_data.columns else 0 for m in all_months]
    sales_objective = [float(str(monthly_data.loc["Sales Objective", m]).replace("$", "").replace(",", ""))
                    if m in monthly_data.columns else 0 for m in all_months]
    sales_actual = [x if x > 0 else 0 for x in sales_actual]
    fig, ax = plt.subplots(figsize=(12, 3))
    ax.bar(all_months, sales_actual, label="Sales Actual", color="blue", alpha=0.7)
    ax.bar(all_months, sales_objective, label="Sales Objective", color="orange", alpha=0.7, width=0.4, align="edge")
    ax.set_title("Sales vs Sales Objective", fontsize=16, fontweight="bold")
    ax.set_xlabel("Months", fontsize=12)
    ax.set_ylabel("Sales ($)", fontsize=12)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax.set_xticklabels(all_months, rotation=45, ha="right", fontsize=9)
    st.pyplot(fig)
    for col in monthly_data.columns:
        monthly_data[col] = monthly_data[col].apply(
            lambda x: f"${x:,.2f}" if isinstance(x, (int, float)) and not str(x).endswith('%') else x
        )
    st.subheader("Monthly Performance Summary")
    st.dataframe(monthly_data, use_container_width=True)
    
    def fetch_data_status():
        query = "SELECT * FROM data_status"
        engine = get_db_connection()
        try:
            with engine.connect() as conn:
                data_status = pd.read_sql_query(query, conn)
            return data_status
        except Exception as e:
            st.error(f"Error fetching data status table: {e}")
            return pd.DataFrame()
        finally:
            engine.dispose()
    data_status_df = fetch_data_status()
    if not data_status_df.empty:
        st.subheader("Data Upload Status")
        boolean_columns = [col for col in data_status_df.columns if col != "Product line"]
        for col in boolean_columns:
            data_status_df[col] = data_status_df[col].fillna(False).astype(bool)
        data_status_df["Product line"] = data_status_df["Product line"].astype(str)
        st.dataframe(data_status_df, use_container_width=True)
    else:
        st.warning("No data available in the data_status table.")
else:
    st.warning("No data available for the selected filters.")
