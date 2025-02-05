import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create a database connection."""
    engine = create_engine(DATABASE_URL)
    return engine

def load_excel_file_quickbooks(filepath: str) -> pd.DataFrame:
    """
    Load and preprocess a QuickBooks Excel file.
    """
    # Read the Excel file
    df = pd.read_excel(filepath, header=0)

    # Drop rows that are completely empty
    df.dropna(how='all', inplace=True)

    # Strip whitespace from all column names
    df.columns = df.columns.str.strip()

    # Ensure no trailing spaces or non-breaking spaces before inserting in the database.
    if 'Sales Rep Name' in df.columns:
        df["Sales Rep Name"] = df["Sales Rep Name"].fillna("").astype(str).str.strip()

    # # Remove the 'Description' column if it exists
    if 'Description' in df.columns:
        df.drop(columns=['Description'], inplace=True)

    # Drop rows where 'Line order' == 0
    if 'Line order' in df.columns:
        df = df[df['Line order'] != 0]

    # Drop rows where 'Quantity' == None
    if 'Quantity' in df.columns:
        df = df[df['Quantity'].notnull()]

    # # Drop rows where 'Customer name' == "HHS Transfers Customer"
    if 'Company name' in df.columns:
        df = df[df['Company name'] != "HHS Transfers Customer"]

    # Remove rows where 'Purchase description' contains 'shipping' (case-insensitive)
    if 'Purchase description' in df.columns:
        df = df[~df['Purchase description'].str.contains('shipping', case=False, na=False)]

    # # Populate 'Company name' based on 'Customer' conditions
    if 'Customer' in df.columns and 'Company name' in df.columns:
        def populate_company_name(customer, company_name):
            if len(customer) >= 7 and customer[-7] == "-" and customer[-6] == "H":
                return customer[-6:]
            return company_name
        
        df['Company name'] = df.apply(
            lambda row: populate_company_name(row['Customer'], row['Company name']), axis=1
        )

    # Clean and process 'Amount line' as strings, then convert to float
    if 'Amount line' in df.columns:
        df['Amount line'] = df['Amount line'].astype(str)  # Ensure 'Amount line' is a string
        df['Amount line'] = (
            df['Amount line']
            .str.replace(",", "", regex=False)  # Remove commas
            .str.strip()  # Remove any extra spaces
        )
        df['Amount line'] = pd.to_numeric(df['Amount line'], errors='coerce').fillna(0.0)  # Convert to float
        df['Amount line'] = df['Amount line'].round(2)  # Round to two decimal places

    # # Add a new column 'Product Lines' after 'Date'
    if 'Date' in df.columns and 'Service Lines' in df.columns:
        # Fetch service_to_product mapping from the database
        service_to_product_query = """
        SELECT "Service Lines", "Product Lines"
        FROM service_to_product
        """
        engine = get_db_connection()
        try:
            with engine.connect() as conn:
                service_to_product = pd.read_sql_query(service_to_product_query, conn)
            
            # Convert the mapping to a dictionary for faster lookup
            service_to_product_dict = service_to_product.set_index("Service Lines")["Product Lines"].to_dict()

            # Add the new column 'Product Lines'
            df.insert(df.columns.get_loc('Date') + 1, 'Product Lines', '')  # Add the column after 'Date'

            # Populate 'Product Lines' based on the 'Service Lines' mapping
            def map_product_line(service_line):
                return service_to_product_dict.get(service_line, '')  # Return an empty string if no match is found
            
            df['Product Lines'] = df['Service Lines'].apply(map_product_line)

        except Exception as e:
            raise RuntimeError(f"Error fetching service_to_product data: {e}")
        finally:
            engine.dispose()

    # Clean and convert 'Purchase price' to numeric
    if 'Purchase price' in df.columns:
        df['Purchase price'] = df['Purchase price'].astype(str)  # Ensure it's a string
        df['Purchase price'] = (
            df['Purchase price']
            .str.replace("$", "", regex=False)  # Remove the '$' symbol
            .str.replace(",", "", regex=False)  # Remove commas if any
            .str.strip()  # Remove any extra spaces
        )
        df['Purchase price'] = pd.to_numeric(df['Purchase price'], errors='coerce')  # Convert to float

     # Add calculated columns after 'Quantity'
    if all(col in df.columns for col in ['Amount line', 'Purchase price', 'Quantity']):
        # Convert relevant columns to numeric, coercing errors to NaN
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')

        # Fill NaN values with 0 to ensure calculations work
        df[['Purchase price', 'Quantity']] = df[['Purchase price', 'Quantity']].fillna(0)

        # Add the new columns
        df.insert(df.columns.get_loc('Quantity') + 1, 'Margin', 
                  df['Amount line'] - (df['Purchase price'] * df['Quantity']))
        #df.insert(df.columns.get_loc('Margin') + 1, 'Commission rate', 0.35)
        #df.insert(df.columns.get_loc('Commission rate') + 1, 'Comm Amount', 
        #          df['Margin'] * df['Commission rate'])
        #df.insert(df.columns.get_loc('Comm Amount') + 1, 'SHS Margin', 
        #          df['Margin'] - df['Comm Amount'])


    # Format numeric columns to float with two decimals and no separators
    numeric_columns = ['Amount line', 'Purchase price', 'Margin']#, 'Comm Amount', 'SHS Margin']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: round(float(x), 2) if pd.notnull(x) else 0.0)

    # Transform the 'Date' column
    if 'Date' in df.columns:
        # Convert the 'Date' column to datetime format, assuming MM/DD/YYYY
        df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce')

        # Check for invalid dates and drop rows with invalid dates
        df = df[df['Date'].notnull()]

        # Format the 'Date' column to YYYY-MM-DD (string representation)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        # Add new columns 'Date YYYY' and 'Date MM'
        df.insert(df.columns.get_loc('Date') + 1, 'Date YYYY', df['Date'].str[:4])  # Extract the year
        df.insert(df.columns.get_loc('Date YYYY') + 1, 'Date MM', df['Date'].str[5:7])  # Extract the month


    # Keep all values as-is without specific formatting
    return df

def fetch_master_sales_rep():
    """Load the master sales rep data from the database."""
    query = """
    SELECT "Source", "Customer field", "Data field value", "Sales Rep name", "Valid from", "Valid until"
    FROM master_sales_rep
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            master_df = pd.read_sql_query(query, conn)
        return master_df
    except Exception as e:
        raise RuntimeError(f"Error fetching master_sales_rep data: {e}")
    finally:
        engine.dispose()

def enrich_sales_rep(df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich the DataFrame with Sales Rep information from the master_sales_rep table.
    """
    def match_sales_rep(customer):
        match = master_df[master_df['Data field value'].str.strip() == str(customer).strip()]
        if not match.empty:
            return match.iloc[0]['Sales Rep name']
        return None

    if 'Customer' in df.columns:
        df['Enriched Sales Rep'] = df['Customer'].apply(match_sales_rep)

    return df
