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
        WHERE "Source" = 'Cygnus'
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

def load_excel_file_cygnus(filepath: str) -> pd.DataFrame:
    # Read the Excel file starting from the correct header row
    raw_df = pd.read_excel(filepath, header=3)
    # Run validation on the raw DataFrame
    is_valid, missing = validate_file_format(raw_df, "Cygnus")
    if not is_valid:
        raise ValueError(f"Raw file format invalid. Missing columns: {', '.join(missing)}")

    # Proceed with the cleaning and enrichment using the raw_df
    df = raw_df.copy()
    
    # Drop rows that are completely empty
    df.dropna(how='all', inplace=True)
    
    # Remove rows that contain "Total" in the first column
    if df.columns[0] not in (None, ""):
        df = df[~df.iloc[:, 0].astype(str).str.contains("Total", case=False, na=False)]
    
    # Remove rows where "Cust. ID" contains "total" (case-insensitive)
    if "Cust. ID" in df.columns:
        df = df[~df["Cust. ID"].astype(str).str.contains("total", case=False, na=False)]
    
    # Convert Rep % from strings like "7,0%" to floats like 0.07
    if "Rep %" in df.columns:
        df["Rep %"] = df["Rep %"].astype(str).str.replace("%", "", regex=False).str.replace(",", ".", regex=False)
        df["Rep %"] = pd.to_numeric(df["Rep %"], errors='coerce')
    
    # Handle the ClosedDate column - now renamed to Revenue Recognition Date
    date_columns = ["Inv Date", "Due Date", "ClosedDate"]
    for date_col in date_columns:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if date_col == "ClosedDate":
                # Rename to Revenue Recognition Date
                df["Revenue Recognition Date"] = df[date_col].dt.strftime('%Y-%m-%d')
                df["Revenue Recognition Date YYYY"] = df[date_col].dt.year.astype("Int64").astype(str).str.replace(",", "")
                df["Revenue Recognition Date MM"] = df[date_col].dt.month.astype("Int64").astype(str).str.zfill(2)
                # Drop the original column
                df = df.drop(columns=[date_col])
            else:
                df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')

    # Reorder columns to place Revenue Recognition Date fields together
    if "Revenue Recognition Date" in df.columns and "Revenue Recognition Date YYYY" in df.columns and "Revenue Recognition Date MM" in df.columns:
        cols = list(df.columns)
        # Remove the columns from their current positions
        cols.remove("Revenue Recognition Date")
        cols.remove("Revenue Recognition Date YYYY")
        cols.remove("Revenue Recognition Date MM")
        # Find position to insert them together (after Due Date)
        if "Due Date" in cols:
            index_of_due_date = cols.index("Due Date")
            cols.insert(index_of_due_date + 1, "Revenue Recognition Date")
            cols.insert(index_of_due_date + 2, "Revenue Recognition Date YYYY")
            cols.insert(index_of_due_date + 3, "Revenue Recognition Date MM")
        else:
            # If Due Date isn't present, just add them at the end of the DataFrame
            cols.extend(["Revenue Recognition Date", "Revenue Recognition Date YYYY", "Revenue Recognition Date MM"])
        df = df[cols]

    # Convert numeric columns and ensure proper formatting
    for numeric_col in ["Invoice Total", "Total Rep Due"]:
        if numeric_col in df.columns:
            df[numeric_col] = (
                df[numeric_col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.strip()
            )
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors='coerce')
            df[numeric_col] = df[numeric_col].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

    # Ensure "Invoice" column is treated as a string without numeric interpretation
    if "Invoice" in df.columns:
        df["Invoice"] = df["Invoice"].ffill()
        df["Invoice"] = df["Invoice"].apply(
            lambda x: str(int(x)) if pd.notnull(x) and isinstance(x, float) else str(x)
        )
        df["Invoice"] = df["Invoice"].str.strip()
        df = df[df["Invoice"] != ""]
        
    # Ensure "Sales Rep" column is filled with the value above if empty
    if "Sales Rep" in df.columns:
        df["Sales Rep"] = df["Sales Rep"].ffill().str.strip()

    # Ensure specified columns are filled with the value above if empty
    columns_to_fill = ["Cust. ID", "Cust- Name", "Name", "Address", "City", "State"]
    for column in columns_to_fill:
        if column in df.columns:
            df[column] = df[column].ffill().str.strip()

    # Enrich the DataFrame with Sales Rep Name from master_sales_rep
    master_df = load_master_sales_rep()

    def enrich_sales_rep(name, rev_year=None, rev_month=None):
        """
        Find the Sales Rep assigned to a customer at the time of the transaction.
        
        Args:
            name: The customer name to look up
            rev_year: Revenue Recognition Year (YYYY)
            rev_month: Revenue Recognition Month (MM) - zero-padded string like "01", "02", etc.
        """
        if pd.isna(name) or name == "":
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
            (master_df["Source"] == "Cygnus") & 
            (master_df["Data field value"].str.strip() == str(name).strip()) &
            (master_df["Valid from"] <= compare_date) &
            ((master_df["Valid until"].isnull()) | (master_df["Valid until"] > compare_date))
        ]
        
        if not match.empty:
            return match.iloc[0]["Sales Rep name"]
        return None

    # Apply the enriched Sales Rep lookup with Revenue Recognition date
    if "Name" in df.columns and "Revenue Recognition Date YYYY" in df.columns and "Revenue Recognition Date MM" in df.columns:
        df["Sales Rep Name"] = df.apply(
            lambda row: enrich_sales_rep(
                row["Name"], 
                row["Revenue Recognition Date YYYY"], 
                row["Revenue Recognition Date MM"]
            ), 
            axis=1
        )
    else:
        # Fallback if Revenue Recognition dates are not available
        if "Name" in df.columns:
            df["Sales Rep Name"] = df["Name"].apply(lambda x: enrich_sales_rep(x))

    # Move the "Sales Rep Name" column right after "Sales Rep"
    if "Sales Rep" in df.columns and "Sales Rep Name" in df.columns:
        cols = list(df.columns)
        cols.remove("Sales Rep Name")
        index_of_sales_rep = cols.index("Sales Rep")
        cols.insert(index_of_sales_rep + 1, "Sales Rep Name")
        df = df[cols]

    return df