import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from data_loaders.validation_utils import validate_file_format

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def load_master_sales_rep():
    """Load the master_sales_rep table from the database."""
    query = """
        SELECT "Source", "Customer field", "Data field value", "Sales Rep name", "Valid from", "Valid until"
        FROM master_sales_rep
        WHERE "Source" = 'Sunoptics'
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            master_df = pd.read_sql_query(query, conn)
        # Convert date columns to datetime
        master_df["Valid from"] = pd.to_datetime(master_df["Valid from"], errors='coerce')
        master_df["Valid until"] = pd.to_datetime(master_df["Valid until"], errors='coerce')
        return master_df
    except Exception as e:
        raise RuntimeError(f"Error loading master_sales_rep table: {e}")
    finally:
        engine.dispose()

def load_excel_file_sunoptic(filepath: str, year: str = None, month: str = None, 
                            rev_year: str = None, rev_month: str = None) -> pd.DataFrame:
    """
    Load and transform a Sunoptic Excel file into a pandas DataFrame.
    
    Args:
        filepath: Path to the Excel file
        year: Selected Commission Date year
        month: Selected Commission Date month
        rev_year: Selected Revenue Recognition year
        rev_month: Selected Revenue Recognition month
        
    Returns:
        Transformed DataFrame with standardized columns
    """
    # Read the Excel file starting from the correct header row
    raw_df = pd.read_excel(filepath, header=0)
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Sunoptic")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

    # Proceed with the cleaning and enrichment using the raw_df
    df = raw_df.copy()
    
    # 1. Drop rows that have no values in key columns
    required_columns = [
        "Invoice ID", "Invoice Date", "Customer ID", "Bill Name", 
        "Sales Order ID", "Item ID", "Item Name", "Prod Fam", 
        "Unit Price", "Ship Qty", "Customer Type", "Ship To Name", 
        "Address Ship to", "Ship To City", "Ship To State"
    ]
    
    # Ensure all required columns exist
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
    
    df = df.dropna(subset=required_columns, how='all')

    # 6. Convert "Commission %" from percentage to decimal factor
    if "Commission %" in df.columns:
        df["Commission %"] = df["Commission %"].astype(str).str.replace('%', '', regex=False)
        df["Commission %"] = pd.to_numeric(df["Commission %"], errors='coerce')

    # 2. Handle Revenue Recognition Date based on user selection or file data
    # Check if Revenue Recognition date parameters are provided
    if rev_year and rev_month:
        # Convert month names to numbers (1-12)
        month_map = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }
        rev_month_num = month_map.get(rev_month)
        if not rev_month_num:
            raise ValueError(f"Invalid Revenue Recognition month: {rev_month}")
        
        # Create Revenue Recognition Date columns from user selection
        df["Revenue Recognition Date"] = f"{rev_year}-{rev_month_num}"
        df["Revenue Recognition Date YYYY"] = rev_year
        df["Revenue Recognition Date MM"] = rev_month_num
    else:
        # Use "Invoice Date" from the file if available (original behavior)
        if "Invoice Date" in df.columns:
            # Convert to datetime for processing
            invoice_date = pd.to_datetime(df["Invoice Date"], errors='coerce')
            
            # Create Revenue Recognition Date columns
            df["Revenue Recognition Date"] = invoice_date.dt.strftime('%Y-%m')
            df["Revenue Recognition Date YYYY"] = invoice_date.dt.year.astype(str)
            df["Revenue Recognition Date MM"] = invoice_date.dt.month.astype(str).str.zfill(2)
    
    # 3. Handle Commission Date based on user selection
    if year and month:
        # Convert month names to numbers (1-12)
        month_map = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }
        month_num = month_map.get(month)
        if not month_num:
            raise ValueError(f"Invalid Commission Date month: {month}")
        
        # Create Commission Date columns
        df["Commission Date"] = f"{year}-{month_num}"
        df["Commission Date YYYY"] = year
        df["Commission Date MM"] = month_num
    else:
        # Default behavior if Commission Date wasn't provided (should never happen)
        df["Commission Date"] = ""
        df["Commission Date YYYY"] = ""
        df["Commission Date MM"] = ""
    
    # Remove the original Invoice Date column - we don't need it anymore
    if "Invoice Date" in df.columns:
        df = df.drop(columns=["Invoice Date"])
    
    # 4. Remove '$' from "Unit Price", "Line Amount", and "Commission $"
    monetary_columns = ["Unit Price", "Line Amount", "Commission $"]
    for col in monetary_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    
    # 5. Convert "Ship Qty" to numeric with specified precision
    if "Ship Qty" in df.columns:
        df["Ship Qty"] = pd.to_numeric(df["Ship Qty"], errors='coerce').fillna(0).round(2)

    # ✅ Load the master data from the database
    master_df = load_master_sales_rep()

    # ✅ Enhanced Sales Rep lookup function that considers Valid from and Valid until dates
    def enrich_sales_rep(customer_id, rev_year=None, rev_month=None):
        """
        Find the Sales Rep assigned to a customer at the time of the transaction.
        
        Args:
            customer_id: The customer ID to look up
            rev_year: Revenue Recognition Year (YYYY)
            rev_month: Revenue Recognition Month (MM) - zero-padded string like "01", "02", etc.
        """
        # Convert empty strings to None before processing
        if pd.isna(customer_id) or str(customer_id).strip() == "":
            return None
            
        # Construct date for comparison
        if rev_year is None or rev_month is None:
            current_date = pd.Timestamp.now()
            compare_date = pd.Timestamp(year=current_date.year, month=current_date.month, day=1)
        else:
            try:
                compare_date = pd.Timestamp(year=int(rev_year), month=int(rev_month), day=1)
            except (ValueError, TypeError):
                current_date = pd.Timestamp.now()
                compare_date = pd.Timestamp(year=current_date.year, month=current_date.month, day=1)
        
        # Find matches considering Valid from and Valid until dates
        match = master_df[
            (master_df["Source"] == "Sunoptics") &
            (master_df["Customer field"] == "Customer ID") &
            (master_df["Data field value"].str.strip() == str(customer_id).strip()) &
            (master_df["Valid from"] <= compare_date) &
            ((master_df["Valid until"].isnull()) | (master_df["Valid until"] > compare_date))
        ]
        
        if not match.empty:
            return match.iloc[0]["Sales Rep name"]
        
        # Explicitly return None, not an empty string
        return None
    
    # After enrichment, ensure empty strings are converted to None
    if "Sales Rep Name" in df.columns:
        df["Sales Rep Name"] = df["Sales Rep Name"].apply(
            lambda x: None if pd.isna(x) or str(x).strip() == "" else x
        )
    
    # Apply the enhanced Sales Rep lookup using Revenue Recognition date
    if "Customer ID" in df.columns and "Revenue Recognition Date YYYY" in df.columns and "Revenue Recognition Date MM" in df.columns:
        df["Sales Rep Name"] = df.apply(
            lambda row: enrich_sales_rep(
                row["Customer ID"],
                row["Revenue Recognition Date YYYY"],
                row["Revenue Recognition Date MM"]
            ),
            axis=1
        )
    else:
        # Fallback if Revenue Recognition dates are not available
        if "Customer ID" in df.columns:
            df["Sales Rep Name"] = df["Customer ID"].apply(lambda x: enrich_sales_rep(x))
    
    return df

# import pandas as pd
# from sqlalchemy import create_engine
# from dotenv import load_dotenv
# import os
# from data_loaders.validation_utils import validate_file_format

# # Load environment variables
# load_dotenv()

# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# def load_master_sales_rep():
#     """Load the master_sales_rep table from the database."""
#     query = """
#         SELECT "Source", "Customer field", "Data field value", "Sales Rep name", "Valid from", "Valid until"
#         FROM master_sales_rep
#         WHERE "Source" = 'Sunoptics'
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             master_df = pd.read_sql_query(query, conn)
#         # Convert date columns to datetime
#         master_df["Valid from"] = pd.to_datetime(master_df["Valid from"], errors='coerce')
#         master_df["Valid until"] = pd.to_datetime(master_df["Valid until"], errors='coerce')
#         return master_df
#     except Exception as e:
#         raise RuntimeError(f"Error loading master_sales_rep table: {e}")
#     finally:
#         engine.dispose()

# def load_excel_file_sunoptic(filepath: str, year: str = None, month: str = None, 
#                             rev_year: str = None, rev_month: str = None) -> pd.DataFrame:
#     """
#     Load and transform a Sunoptic Excel file into a pandas DataFrame.
    
#     Args:
#         filepath: Path to the Excel file
#         year: Selected Commission Date year
#         month: Selected Commission Date month
#         rev_year: Selected Revenue Recognition year
#         rev_month: Selected Revenue Recognition month
        
#     Returns:
#         Transformed DataFrame with standardized columns
#     """
#     # Read the Excel file starting from the correct header row
#     raw_df = pd.read_excel(filepath, header=0)
#     # Run validation on the raw DataFrame
#     is_valid, missing = validate_file_format(raw_df, "Sunoptic")
#     if not is_valid:
#         raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

#     # Proceed with the cleaning and enrichment using the raw_df
#     df = raw_df.copy()
    
#     # 1. Drop rows that have no values in key columns
#     required_columns = [
#         "Invoice ID", "Invoice Date", "Customer ID", "Bill Name", 
#         "Sales Order ID", "Item ID", "Item Name", "Prod Fam", 
#         "Unit Price", "Ship Qty", "Customer Type", "Ship To Name", 
#         "Address Ship to", "Ship To City", "Ship To State"
#     ]
    
#     # Ensure all required columns exist
#     for col in required_columns:
#         if col not in df.columns:
#             df[col] = None
    
#     df = df.dropna(subset=required_columns, how='all')

#     # 6. Convert "Commission %" from percentage to decimal factor
#     if "Commission %" in df.columns:
#         df["Commission %"] = df["Commission %"].astype(str).str.replace('%', '', regex=False)
#         df["Commission %"] = pd.to_numeric(df["Commission %"], errors='coerce')

#     # 2. Handle Revenue Recognition Date based on user selection or file data
#     # Check if Revenue Recognition date parameters are provided
#     if rev_year and rev_month:
#         # Convert month names to numbers (1-12)
#         month_map = {
#             "January": "01", "February": "02", "March": "03", "April": "04",
#             "May": "05", "June": "06", "July": "07", "August": "08",
#             "September": "09", "October": "10", "November": "11", "December": "12"
#         }
#         rev_month_num = month_map.get(rev_month)
#         if not rev_month_num:
#             raise ValueError(f"Invalid Revenue Recognition month: {rev_month}")
        
#         # Create Revenue Recognition Date columns from user selection
#         df["Revenue Recognition Date"] = f"{rev_year}-{rev_month_num}"
#         df["Revenue Recognition Date YYYY"] = rev_year
#         df["Revenue Recognition Date MM"] = rev_month_num
#     else:
#         # Use "Invoice Date" from the file if available (original behavior)
#         if "Invoice Date" in df.columns:
#             # Convert to datetime for processing
#             invoice_date = pd.to_datetime(df["Invoice Date"], errors='coerce')
            
#             # Create Revenue Recognition Date columns
#             df["Revenue Recognition Date"] = invoice_date.dt.strftime('%Y-%m')
#             df["Revenue Recognition Date YYYY"] = invoice_date.dt.year.astype(str)
#             df["Revenue Recognition Date MM"] = invoice_date.dt.month.astype(str).str.zfill(2)
    
#     # 3. Handle Commission Date based on user selection
#     if year and month:
#         # Convert month names to numbers (1-12)
#         month_map = {
#             "January": "01", "February": "02", "March": "03", "April": "04",
#             "May": "05", "June": "06", "July": "07", "August": "08",
#             "September": "09", "October": "10", "November": "11", "December": "12"
#         }
#         month_num = month_map.get(month)
#         if not month_num:
#             raise ValueError(f"Invalid Commission Date month: {month}")
        
#         # Create Commission Date columns
#         df["Commission Date"] = f"{year}-{month_num}"
#         df["Commission Date YYYY"] = year
#         df["Commission Date MM"] = month_num
#     else:
#         # Default behavior if Commission Date wasn't provided (should never happen)
#         df["Commission Date"] = ""
#         df["Commission Date YYYY"] = ""
#         df["Commission Date MM"] = ""
    
#     # Remove the original Invoice Date column - we don't need it anymore
#     if "Invoice Date" in df.columns:
#         df = df.drop(columns=["Invoice Date"])
    
#     # 4. Remove '$' from "Unit Price", "Line Amount", and "Commission $"
#     monetary_columns = ["Unit Price", "Line Amount", "Commission $"]
#     for col in monetary_columns:
#         if col in df.columns:
#             df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
#             df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    
#     # 5. Convert "Ship Qty" to numeric with specified precision
#     if "Ship Qty" in df.columns:
#         df["Ship Qty"] = pd.to_numeric(df["Ship Qty"], errors='coerce').fillna(0).round(2)

#     # ✅ Load the master data from the database
#     master_df = load_master_sales_rep()

#     # ✅ Enrich the DataFrame
#     def enrich_sales_rep(name):
#         # Convert empty strings to None before processing
#         if pd.isna(name) or str(name).strip() == "":
#             return None
            
#         # Ensure we're using string comparison correctly
#         match = master_df[
#             (master_df["Source"] == "Sunoptics") &
#             (master_df["Data field value"].str.strip() == str(name).strip())
#         ]
        
#         if not match.empty:
#             return match.iloc[0]["Sales Rep name"]
        
#         # Explicitly return None, not an empty string
#         return None
    
#     # After enrichment, ensure empty strings are converted to None
#     if "Sales Rep Name" in df.columns:
#         df["Sales Rep Name"] = df["Sales Rep Name"].apply(
#             lambda x: None if pd.isna(x) or str(x).strip() == "" else x
#         )
    
#     # Enrich the data
#     if "Customer ID" in df.columns:
#         df["Sales Rep Name"] = df["Customer ID"].apply(enrich_sales_rep)
        
#     return df
# import pandas as pd
# from sqlalchemy import create_engine
# from dotenv import load_dotenv
# import os
# from data_loaders.validation_utils import validate_file_format

# # Load environment variables
# load_dotenv()

# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

# def get_db_connection():
#     """Create a database connection."""
#     engine = create_engine(DATABASE_URL)
#     return engine

# def load_master_sales_rep():
#     """Load the master_sales_rep table from the database."""
#     query = """
#         SELECT "Source", "Customer field", "Data field value", "Sales Rep name", "Valid from", "Valid until"
#         FROM master_sales_rep
#         WHERE "Source" = 'Sunoptics'
#     """
#     engine = get_db_connection()
#     try:
#         with engine.connect() as conn:
#             master_df = pd.read_sql_query(query, conn)
#         # Convert date columns to datetime
#         master_df["Valid from"] = pd.to_datetime(master_df["Valid from"], errors='coerce')
#         master_df["Valid until"] = pd.to_datetime(master_df["Valid until"], errors='coerce')
#         return master_df
#     except Exception as e:
#         raise RuntimeError(f"Error loading master_sales_rep table: {e}")
#     finally:
#         engine.dispose()

# def load_excel_file_sunoptic(filepath: str) -> pd.DataFrame:
#     # Read the Excel file starting from the correct header row
#     raw_df = pd.read_excel(filepath, header=0)
#     # Run validation on the raw DataFrame
#     is_valid, missing = validate_file_format(raw_df, "Sunoptic")
#     if not is_valid:
#         raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

#     # Proceed with the cleaning and enrichment using the raw_df
#     df = raw_df.copy()
    
#     # 1. Drop rows that have no values in key columns
#     required_columns = [
#         "Invoice ID", "Invoice Date", "Customer ID", "Bill Name", 
#         "Sales Order ID", "Item ID", "Item Name", "Prod Fam", 
#         "Unit Price", "Ship Qty", "Customer Type", "Ship To Name", 
#         "Address Ship to", "Ship To City", "Ship To State"
#     ]
    
#     # Ensure all required columns exist
#     for col in required_columns:
#         if col not in df.columns:
#             df[col] = None
    
#     df = df.dropna(subset=required_columns, how='all')

#     # 6. Convert "Commission %" from percentage to decimal factor
#     if "Commission %" in df.columns:
#         df["Commission %"] = df["Commission %"].astype(str).str.replace('%', '', regex=False)
#         df["Commission %"] = pd.to_numeric(df["Commission %"], errors='coerce')

#     # 2. Convert "Invoice Date" to "Revenue Recognition Date" fields
#     if "Invoice Date" in df.columns:
#         # Convert to datetime for processing
#         invoice_date = pd.to_datetime(df["Invoice Date"], errors='coerce')
        
#         # Create Revenue Recognition Date columns
#         df["Revenue Recognition Date"] = invoice_date.dt.strftime('%Y-%m-%d')
#         df["Revenue Recognition Date YYYY"] = invoice_date.dt.year.astype(str)
#         df["Revenue Recognition Date MM"] = invoice_date.dt.month.astype(str).str.zfill(2)
        
#         # Remove the original Invoice Date column - we don't need it anymore
#         df = df.drop(columns=["Invoice Date"])
    
#     # 4. Remove '$' from "Unit Price", "Line Amount", and "Commission $"
#     monetary_columns = ["Unit Price", "Line Amount", "Commission $"]
#     for col in monetary_columns:
#         if col in df.columns:
#             df[col] = df[col].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
#             df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    
#     # 5. Convert "Ship Qty" to numeric with specified precision
#     if "Ship Qty" in df.columns:
#         df["Ship Qty"] = pd.to_numeric(df["Ship Qty"], errors='coerce').fillna(0).round(2)

#     # ✅ Load the master data from the database
#     master_df = load_master_sales_rep()

#     # ✅ Enrich the DataFrame
#     def enrich_sales_rep(name):
#         # Convert empty strings to None before processing
#         if pd.isna(name) or str(name).strip() == "":
#             return None
            
#         # Ensure we're using string comparison correctly
#         match = master_df[
#             (master_df["Source"] == "Sunoptics") &
#             (master_df["Data field value"].str.strip() == str(name).strip())
#         ]
        
#         if not match.empty:
#             return match.iloc[0]["Sales Rep name"]
        
#         # Explicitly return None, not an empty string
#         return None
    
#     # After enrichment, ensure empty strings are converted to None
#     if "Sales Rep Name" in df.columns:
#         df["Sales Rep Name"] = df["Sales Rep Name"].apply(
#             lambda x: None if pd.isna(x) or str(x).strip() == "" else x
#         )
    
#     # Enrich the data
#     if "Customer ID" in df.columns:
#         df["Sales Rep Name"] = df["Customer ID"].apply(enrich_sales_rep)
        
#     return df