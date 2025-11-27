# Sales Analytics Demo Dataset

A comprehensive synthetic dataset generator for building sales analytics demos in Snowflake or Databricks.

## 📁 Project Structure

```
synthetic_data_sales/
├── README.md                    # This file - project overview
├── docs/                        # Documentation files
│   ├── complete_readme.md       # Complete setup guide
│   ├── schema_documentation.md  # Detailed schema documentation
│   ├── QUICK_START_SNOWFLAKE.md # Quick start guide for Snowflake
│   ├── SNOWFLAKE_LOADING_GUIDE.md # Detailed Snowflake loading guide
│   ├── databricks_upload_readme.md # Databricks setup guide
│   ├── quick_reference.md       # Quick reference guide
│   └── sales_rep_questions.md   # Sales rep Q&A
├── scripts/                     # Python scripts
│   ├── sales_data_generator_part1.py  # Generator part 1 (16 tables)
│   ├── sales_data_generator_part2.py  # Generator part 2 (10 tables)
│   ├── sales_data_generator_part3.py  # Generator part 3 (21 tables)
│   ├── sales_data_generator_part4.py  # Generator part 4 (15 tables)
│   ├── load_to_snowflake.py    # Snowflake data loader (loads all tables)
│   ├── load_specific_tables.py  # Quick test script (loads specific tables)
│   ├── verify_dataset.py        # Dataset verification script
│   ├── databricks_unity_catalog_loader.py # Databricks loader
│   └── users_databricks_setup.py # Databricks user setup
├── sql/                         # SQL scripts
│   ├── snowflake_ddl_script.sql # Snowflake DDL (creates all 62 tables)
│   └── security_policies_snowflake.sql # Security policies
├── diagrams/                    # Schema diagrams
│   ├── schema_overview.png      # Overview diagram
│   ├── entity_relationship.png  # ER diagram
│   ├── cross_schema_relationships.png # Cross-schema diagram
│   ├── schema_overview.mmd      # Mermaid source files
│   ├── entity_relationship.mmd
│   └── cross_schema_relationships.mmd
├── sales_analytics_data/        # Generated CSV files (output)
│   ├── sales_data/              # 17 tables
│   ├── usage_data/              # 11 tables
│   ├── marketing_data/           # 13 tables
│   ├── support_data/            # 12 tables
│   └── operational_data/        # 9 tables
├── archive/                     # Old/unused files
└── venv/                        # Python virtual environment
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Snowflake account (for loading data)
- (Optional) Databricks account (for Databricks loading)

### Setup

1. **Clone or download this repository**

2. **Create and activate a virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Generate the dataset:**
   ```bash
   python3 scripts/sales_data_generator_part1.py
   python3 scripts/sales_data_generator_part2.py
   python3 scripts/sales_data_generator_part3.py
   python3 scripts/sales_data_generator_part4.py
   ```

5. **Load into Snowflake:**
   - Set environment variables:
     ```bash
     export SNOWFLAKE_USER="your_username"
     export SNOWFLAKE_PASSWORD="your_password"
     export SNOWFLAKE_ACCOUNT="your_account"
     export SNOWFLAKE_WAREHOUSE="your_warehouse"
     ```
   - See `docs/QUICK_START_SNOWFLAKE.md` for detailed instructions
   - Run: `python3 scripts/load_to_snowflake.py`

## 🚀 Quick Start (Legacy)

### 1. Generate Data

```bash
# Run all four generator scripts
python3 scripts/sales_data_generator_part1.py
python3 scripts/sales_data_generator_part2.py
python3 scripts/sales_data_generator_part3.py
python3 scripts/sales_data_generator_part4.py

# Verify generation
python3 scripts/verify_dataset.py
```

### 2. Load into Snowflake

See `docs/QUICK_START_SNOWFLAKE.md` for quick instructions, or `docs/SNOWFLAKE_LOADING_GUIDE.md` for detailed guide.

### 3. Load into Databricks

See `docs/databricks_upload_readme.md` for instructions.

## 📊 Dataset Overview

- **62 tables** across 5 schemas
- **~1.5 million records** of realistic synthetic data
- **Sales & Customer Data** (17 tables): Customers, products, opportunities, deals, contracts
- **Product Usage & Telemetry** (11 tables): Usage metrics, feature adoption, API calls
- **Marketing Data** (13 tables): Campaigns, leads, engagement, attribution
- **Support & Service** (12 tables): Tickets, resolutions, CSAT, SLA tracking
- **Operational Data** (9 tables): Health scores, invoices, payments, renewals

## 📚 Documentation

- **Complete Guide**: `docs/complete_readme.md`
- **Schema Documentation**: `docs/schema_documentation.md`
- **Snowflake Setup**: `docs/SNOWFLAKE_LOADING_GUIDE.md`
- **Quick Start**: `docs/QUICK_START_SNOWFLAKE.md`
- **Diagrams**: `diagrams/` folder

## 🔧 Requirements

- Python 3.8+
- pandas, numpy
- snowflake-connector-python (for Snowflake loading)
- Snowflake account OR Databricks workspace

## 📝 Usage

### Generate Data
```bash
python3 scripts/sales_data_generator_part1.py
python3 scripts/sales_data_generator_part2.py
python3 scripts/sales_data_generator_part3.py
python3 scripts/sales_data_generator_part4.py
```

### Load to Snowflake
```bash
# 1. Run DDL script in Snowflake Web UI (sql/snowflake_ddl_script.sql)
# 2. Set environment variables (see .env.example)
# 3. Run loader
python3 scripts/load_to_snowflake.py

# Optional: Configure row-level security (after loading data)
# Run sql/security_policies_snowflake.sql in Snowflake Web UI
```

### Verify Dataset
```bash
python3 scripts/verify_dataset.py
```

## 🗂️ File Organization

- **docs/**: All documentation and guides
- **scripts/**: All Python scripts (generators, loaders, utilities)
- **sql/**: SQL DDL and security scripts
- **diagrams/**: Schema diagrams and visualizations
- **sales_analytics_data/**: Generated CSV output (gitignored)
- **archive/**: Old/unused files kept for reference

## 📄 License

This synthetic dataset is provided for demo and educational purposes.

---

For detailed information, see the documentation in the `docs/` folder.

