import streamlit as st

tracker_sales_performance = st.Page(
    "views/sales_performance.py",
    title="Sales Performance",
    #icon=":material/bar_chart:",
)
tracker_commission_report = st.Page(
    "views/commission_reports.py",
    title="Commission Reports",
    #icon=":material/bar_chart:",
)
tracker_sales_history = st.Page(
    "views/sales_history.py",
    title="Sales History",
    #icon=":material/bar_chart:",
)
tracker_analytics = st.Page(
    "views/analytics.py",
    title="Analytics",
    #icon=":material/bar_chart:",
)
daum_sales_data_upload = st.Page(
    "views/sales_data_upload.py",
    title="Sales Data Upload",
    #icon=":material/smart_toy:",
)
daum_business_objective_editor = st.Page(
    "views/business_objective_editor.py",
    title="Business Objective Editor",
    #icon=":material/smart_toy:",
)
daum_portfolio_management = st.Page(
    "views/portfolio_management.py",
    title="Portfolio Management",
    #icon=":material/smart_toy:",
)
daum_user_account_administration = st.Page(
    "views/user_account_administration.py",
    title="User Account Administration",
    #icon=":material/smart_toy:",
)

# --- NAVIGATION SETUP [WITH SECTIONS]---
pg = st.navigation(
    {
        "Tracker": [tracker_sales_performance, tracker_commission_report, tracker_sales_history, tracker_analytics],
        "Data User Management": [daum_sales_data_upload, daum_business_objective_editor, daum_portfolio_management, daum_user_account_administration],
    },
    expanded=False
)
# Set the page layout to wide
st.set_page_config(layout="wide")
# Sidebar Logo and Title
with st.sidebar:
    st.markdown(
        """
        <style>
            .sidebar-logo {
                text-align: center;
                margin-bottom: 10px;
            }
            .sidebar-title {
                font-size: 22px;
                font-weight: bold;
                text-align: center;
                color: #ffffff; /* White text for dark theme */
                margin-bottom: 20px;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    # Centered Image
    st.image("assets/images-2.jpeg", use_container_width=True, caption="Sales Performance Tracker")

    
# --- RUN NAVIGATION ---
pg.run()