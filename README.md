# Sales Performance Tracker

A comprehensive Streamlit application for tracking sales performance, commission reports, and managing sales data across multiple product lines. This application allows sales representatives and administrators to visualize sales data, track commissions, and manage user accounts.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Data Models](#data-models)
- [Technical Components](#technical-components)
- [Setup and Installation](#setup-and-installation)
- [AWS Deployment](#aws-deployment)
- [Security Considerations](#security-considerations)
- [User Roles and Permissions](#user-roles-and-permissions)
- [Product Lines](#product-lines)

## Overview

The Sales Performance Tracker is designed to help organizations manage their sales data, track sales representative performance, calculate commissions based on customizable rules, and provide insights through interactive data visualizations. The application supports multiple product lines including Cygnus, Logiquip, Summit Medical, QuickBooks, InspeKtor, and Sunoptic.

## Features

### User Authentication and Access Control
- Secure login with email and password
- Role-based access control (admin and user)
- Password management (change and reset functionality)
- User session management

### Sales Performance Tracking
- Interactive dashboards showing YTD sales actual, revenue actual, and commission payouts
- Monthly performance summaries with objectives vs. actuals
- Visual representations of sales data with charts and graphs
- Data upload status tracking

### Commission Reports
- Detailed commission reports by sales representative and product line
- Monthly and YTD commission calculations based on tiered commission structures
- Calculation of tier 1 and tier 2 commissions based on business objectives
- Automatic threshold detection for tier upgrades

### Business Objective Management
- Setting and editing sales objectives for each sales representative
- Configuring commission tier thresholds
- Managing objectives by product line and time period

### Data Management
- Upload and process sales data from various sources
- Data validation and enrichment
- Harmonization of data across different product lines into a standardized format
- Historical sales data access and filtering

### Portfolio Management
- Service-to-product mapping for QuickBooks integration
- Sales representative commission tier configuration
- Sales territory management

### User Account Administration
- Create, edit, and delete user accounts
- Assign permissions (admin/user)
- Send email notifications for account actions

## Architecture

The application is built on a three-tier architecture:

1. **Frontend**: Streamlit web application with interactive UI components
2. **Backend**: Python-based business logic for data processing and analysis
3. **Database**: PostgreSQL database for persistent storage

### Technology Stack

- **Frontend Framework**: Streamlit
- **Backend Language**: Python 3.9+
- **Database**: PostgreSQL
- **Data Processing**: Pandas, SQLAlchemy
- **Visualization**: Matplotlib, Pygwalker
- **PDF Processing**: Camelot, PyPDF2
- **Email**: SMTP integration

## Data Models

The application uses several key database tables:

### Core Tables
- `master_access_level`: User authentication and permissions
- `harmonised_table`: Standardized sales data from all sources
- `sales_rep_commission_tier`: Commission rate configurations
- `sales_rep_commission_tier_threshold`: Business objective thresholds
- `sales_rep_business_objective`: Monthly sales objectives

### Product-Specific Tables
- `master_cygnus_sales`: Cygnus product line sales data
- `master_logiquip_sales`: Logiquip product line sales data
- `master_summit_medical_sales`: Summit Medical product line sales data
- `master_quickbooks_sales`: QuickBooks sales data
- `master_inspektor_sales`: InspeKtor product line sales data
- `master_sunoptic_sales`: Sunoptic product line sales data

### Mapping Tables
- `service_to_product`: Maps QuickBooks service lines to product lines
- `master_sales_rep`: Territory and product line mapping for sales reps

## Technical Components

### Data Loaders

The application includes specialized data loaders for each product line:

- `cygnus_loader.py`: Processes Cygnus Excel files
- `logiquip_loader.py`: Processes Logiquip Excel files
- `summit_medical_loader.py`: Processes Summit Medical PDF files
- `quickbooks_loader.py`: Processes QuickBooks Excel files
- `inspektor_loader.py`: Processes InspeKtor Excel files
- `sunoptic_loader.py`: Processes Sunoptic Excel files

Each loader handles the specific format and requirements of its data source and performs data cleaning, enrichment, and transformation.

### Database Utilities

Each product line has its own database utility module:

- `cygnus_db_utils.py`
- `logiquip_db_utils.py`
- `summit_medical_db_utils.py`
- `quickbooks_db_utils.py`
- `inspektor_db_utils.py`
- `sunoptic_db_utils.py`

These utilities handle:
- Saving data to product-specific tables
- Mapping product data to the harmonised_table format
- Calculating commission tier thresholds
- Updating commission tier 2 dates

### Views

The application is organized into distinct view modules:

- `streamlit_app.py`: Main application entry point and authentication
- `views/sales_performance.py`: Dashboard showing sales metrics
- `views/commission_reports.py`: Detailed commission reporting
- `views/sales_history.py`: Historical sales data view
- `views/analytics.py`: Interactive data exploration with Pygwalker
- `views/sales_data_upload.py`: Interface for uploading sales data
- `views/business_objective_editor.py`: Edit sales objectives and thresholds
- `views/portfolio_management.py`: Manage product mappings and commissions
- `views/user_account_administration.py`: User account management

## Setup and Installation

### Prerequisites

- Python 3.9+
- PostgreSQL 13+
- SMTP server access for email functionality

### Environment Variables

Create a `.env` file with the following variables:

```
# Database Configuration
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=sales_commissions_db

# Email Configuration
SMTP_HOST=your_smtp_host
SMTP_PORT=587
SMTP_USER=your_smtp_user
SMTP_PASSWORD=your_smtp_password
```

### Installation Steps

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Set up the PostgreSQL database:
   - Create a new database named `sales_commissions_db`
   - Run the SQL schema scripts to create necessary tables

4. Run the application:
   ```
   streamlit run streamlit_app.py
   ```

## AWS Deployment

The application is currently deployed on AWS with a containerized architecture. All resources are tagged with Project: SHS-streamlit-app, Owner: renatomoretti, and Environment: Production for easy resource management and cost tracking.

### AWS Infrastructure

- **Amazon ECS (Elastic Container Service)**: Container orchestration with Fargate launch type
  - Cluster: shs-cluster-2
  - Services: shs-service and shs-service-alb
  - Task Definition: shs-task
- **Amazon ECR (Elastic Container Registry)**: Docker image repository for the application
- **Amazon RDS**: PostgreSQL database instance for data storage
- **Amazon CloudWatch**: Monitoring and logging of the containers and services
- **AWS Application Load Balancer**: For load balancing and routing traffic to the ECS services
- **AWS IAM**: Identity and access management for service roles and permissions
- **AWS CodePipeline/CodeBuild**: CI/CD pipeline for automated builds and deployments
- **AWS EC2**: Bastion for RDS connection

### Containerized Architecture

The application is containerized using Docker and deployed via AWS ECS with the Fargate launch type, which provides serverless container management. This architecture offers several benefits:

1. **Scalability**: The Fargate tasks can scale based on demand
2. **Isolation**: Each container runs in its own environment
3. **Reproducibility**: Consistent deployment across environments
4. **Resource Efficiency**: Only pay for the resources you use

### Deployment Process

1. **Container Build and Push**:
   - The application is packaged into a Docker container
   - Images are pushed to Amazon ECR with tags for versioning
   - Latest images are approximately 640MB in size

2. **ECS Deployment**:
   - Two services are running in the ECS cluster:
     - `shs-service`: Main application service
     - `shs-service-alb`: Service connected to the Application Load Balancer
   - Each service manages one task running the container

3. **Database Connection**:
   - The application connects to an RDS PostgreSQL instance
   - Connection parameters are passed as environment variables

4. **Load Balancing**:
   - Application Load Balancer routes traffic to the ECS services
   - Provides SSL termination and health checks

### CI/CD Pipeline

The application uses a CI/CD pipeline for automated deployments:

1. **Source**: Code changes are pushed to the source repository
2. **Build**: AWS CodeBuild creates a new Docker image
3. **Test**: Automated tests are run against the new image
4. **Deploy**: The new image is deployed to ECS if tests pass
5. **Monitoring**: CloudWatch monitors the deployment and service health

### Monitoring and Maintenance

- **CloudWatch**: Logs and metrics from the ECS tasks and services
- **ECS Task Health**: Automatic restart of failed tasks
- **Database Backups**: Automated RDS backups
- **Image Management**: Regular cleanup of unused ECR images

## Security Considerations

1. **Authentication**:
   - Password hashing and secure storage
   - Session management
   - Rate limiting for login attempts

2. **Authorization**:
   - Role-based access control
   - Data isolation between users
   - Principle of least privilege for database access

3. **Data Protection**:
   - Encryption in transit (HTTPS)
   - Encryption at rest for sensitive data
   - Secure handling of environment variables

4. **AWS Security**:
   - Network isolation using VPC
   - Security groups and network ACLs
   - IAM roles with minimal permissions
   - Regular security updates and patches

## User Roles and Permissions

The application supports two primary user roles:

1. **Admin**:
   - Full access to all features
   - Can view data for all sales representatives
   - Can manage user accounts
   - Can modify commission structures
   - Can upload and process sales data
   - Can edit business objectives

2. **User** (Sales Representative):
   - Limited access to features
   - Can view only their own sales data and performance
   - Cannot access user administration
   - Cannot modify commission structures
   - Cannot upload sales data
   - Limited analytics capabilities

## Product Lines

The application supports the following product lines, each with its own data format and processing logic:

1. **Cygnus**:
   - Excel-based sales data
   - Fields include Sales Rep, Customer, Invoice, SKU, and financial data

2. **Logiquip**:
   - Excel-based sales data
   - Fields include Rep, Customer, PO Number, Ship To Zip, and commission data

3. **Summit Medical**:
   - PDF-based sales data
   - Fields include Client Name, Invoice #, Item ID, and commission data

4. **QuickBooks**:
   - Excel-based sales data
   - Requires service-to-product mapping for proper categorization

5. **InspeKtor**:
   - Excel-based sales data
   - Fields include Customer, Item, Formula, and commission data

6. **Sunoptic**:
   - Excel-based sales data
   - Requires year and month selection during upload
   - Fields include Invoice ID, Item ID, Line Amount, and commission data

Each product line has its own data loader, validation rules, and processing pipeline to handle the specific requirements and formats of that data source.