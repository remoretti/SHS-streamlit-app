import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
st.set_page_config(layout="wide")
# Load environment variables from .env file
load_dotenv()

# Build the DATABASE_URL from environment variables.
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@" \
               f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

def get_db_connection():
    """Create and return a database connection using SQLAlchemy."""
    engine = create_engine(DATABASE_URL)
    return engine

def authenticate_user(email, password):
    """
    Authenticate the user by querying the master_access_level table.
    The table now has:
      - "Email" (to be used for login)
      - "Sales Rep Name" (to be stored in the session for filtering)
      - "Password"
      - "Permission"
    Returns:
      - A tuple (sales_rep_name, permission) if authentication succeeds.
      - (None, None) if credentials are invalid.
    """
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            query = text('''
                SELECT "Sales Rep Name", "Password", "Permission"
                FROM master_access_level
                WHERE "Email" = :email
            ''')
            result = conn.execute(query, {"email": email}).fetchone()
            if result is not None:
                sales_rep_name, db_password, permission = result
                if password == db_password:
                    return sales_rep_name, permission
            return None, None
    except Exception as e:
        st.error(f"Error authenticating user: {e}")
        return None, None
    finally:
        engine.dispose()

# Initialize session state variables if they don't exist.
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.user_permission = None
    st.session_state.user_name = None

if not st.session_state.authenticated:
    st.title("Welcome to Sales Performance Tracker")
    st.write("Please log in to continue.")

    # Create two columns for login inputs.
    col1, col2 = st.columns(2)
    with col1:
        email = st.text_input("Email")
    with col2:
        password = st.text_input("Password", type="password")
    
    

    if st.button("Confirm"):
        sales_rep_name, permission = authenticate_user(email, password)
        if permission:
            st.session_state.authenticated = True
            st.session_state.user_permission = permission
            st.session_state.user_name = sales_rep_name  # Use the Sales Rep Name for filtering
            st.success("Logged in successfully!")
            st.rerun()
        else:
            st.error("Invalid credentials! Please try again.")
    
    # Display an image below the two columns.
    st.image("assets/images-2.jpeg", use_container_width=False, caption="Powered by Xantage")
    st.stop()  # Prevent the rest of the app from loading until login succeeds.


# ---------------------------
# Sidebar Navigation (after successful login)
with st.sidebar:
    st.markdown(
        """
        <style>
            .sidebar-logo { text-align: center; margin-bottom: 10px; }
            .sidebar-title { font-size: 22px; font-weight: bold; text-align: center; color: #ffffff; margin-bottom: 20px; }
        </style>
        """,
        unsafe_allow_html=True
    )
    st.image("assets/images-2.jpeg", use_container_width=True, caption="Sales Performance Tracker")
    st.write(f"Logged in as: **{st.session_state.user_name}** ({st.session_state.user_permission})")
    if st.button("Logout"):
        st.session_state.authenticated = False
        st.session_state.user_permission = None
        st.session_state.user_name = None
        st.rerun()

# ---------------------------
# Configure Navigation Based on Permission
# Import your pages
if st.session_state.user_permission.lower() == "admin":
    # Admins see all pages
    tracker_sales_performance = st.Page("views/sales_performance.py", title="Sales Performance")
    tracker_commission_report = st.Page("views/commission_reports.py", title="Commission Reports")
    tracker_sales_history = st.Page("views/sales_history.py", title="Sales History")
    tracker_analytics = st.Page("views/analytics.py", title="Analytics")
    daum_sales_data_upload = st.Page("views/sales_data_upload.py", title="Sales Data Upload")
    daum_business_objective_editor = st.Page("views/business_objective_editor.py", title="Business Objective Editor")
    daum_portfolio_management = st.Page("views/portfolio_management.py", title="Portfolio Management")
    daum_user_account_administration = st.Page("views/user_account_administration.py", title="User Account Administration")
    
    navigation_dict = {
        "Tracker": [tracker_sales_performance, tracker_commission_report, tracker_sales_history, tracker_analytics],
        "Data User Management": [daum_sales_data_upload, daum_business_objective_editor, daum_portfolio_management, daum_user_account_administration],
    }
else:
    # Regular Users see only Tracker pages.
    tracker_sales_performance = st.Page("views/sales_performance.py", title="Sales Performance")
    tracker_commission_report = st.Page("views/commission_reports.py", title="Commission Reports")
    tracker_sales_history = st.Page("views/sales_history.py", title="Sales History")
    tracker_analytics = st.Page("views/analytics.py", title="Analytics")
    
    navigation_dict = {
        "Tracker": [tracker_sales_performance, tracker_commission_report, tracker_sales_history, tracker_analytics],
    }

# Set page layout and run navigation.
pg = st.navigation(navigation_dict, expanded=False)
pg.run()