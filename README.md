# 🔄 Python ETL Pipeline

A modular, production-style **Extract → Transform → Load** pipeline built in Python.
Ingests raw sales CSV data, applies cleaning and business-rule validation, enriches it with derived metrics, and loads the result into a SQL database — with optional Parquet export.

---

## 📁 Project Structure

```
python-etl-pipeline/
├── etl/
│   ├── extract.py      # CSV and SQL extraction
│   ├── transform.py    # Cleaning, validation, enrichment
│   ├── load.py         # SQL / CSV / Parquet loaders
│   └── pipeline.py     # Orchestrator (CLI entry point)
├── data/
│   └── sample_sales.csv
├── requirements.txt
└── README.md
```

---

## ⚙️ What It Does

| Stage | What Happens |
|-------|-------------|
| **Extract** | Reads from CSV (or SQL via SQLAlchemy) |
| **Transform** | Normalises column names, casts types, drops duplicates, validates business rules |
| **Enrich** | Adds `order_year`, `order_quarter`, `profit_margin`, `revenue_per_unit` |
| **Load** | Writes to SQLite (or any RDBMS) + exports to Parquet |
| **Verify** | Row-count check post-load |

---

## 🚀 Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/tallal85/python-etl-pipeline.git
cd python-etl-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python -m etl.pipeline --source data/sample_sales.csv --table sales_fact
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | `data/sample_sales.csv` | Input CSV path |
| `--table` | `sales_fact` | Target SQL table name |
| `--db` | `sqlite:///data/warehouse.db` | SQLAlchemy connection string |
| `--no-parquet` | *(off)* | Skip Parquet export |

---

## 🛠️ Tech Stack

- **Python 3.10+**
- **pandas** — data manipulation
- **SQLAlchemy** — database abstraction (SQLite / PostgreSQL / SQL Server)
- **PyArrow** — Parquet I/O

---

## 🔌 Connecting to a Real Database

Swap the `--db` flag with your connection string:

```bash
# PostgreSQL
python -m etl.pipeline --db "postgresql://user:password@host:5432/mydb"

# SQL Server
python -m etl.pipeline --db "mssql+pyodbc://user:password@server/db?driver=ODBC+Driver+17+for+SQL+Server"
```

---

## 📊 Sample Output

After running, the pipeline produces:
- `data/warehouse.db` — SQLite database with `sales_fact` table
- `data/sales_fact.parquet` — Parquet file for downstream analytics

Derived columns added during enrichment:

| Column | Description |
|--------|-------------|
| `order_year` | Year extracted from `order_date` |
| `order_month` | Month number |
| `order_quarter` | Quarter (1–4) |
| `order_dow` | Day of week name |
| `profit_margin` | `profit / sales` |
| `revenue_per_unit` | `sales / quantity` |

---

## 👤 Author

**Tallal Moshrif** — Data Engineer & BI Developer
[LinkedIn](https://linkedin.com/in/tallalmoshrif) · [GitHub](https://github.com/tallal85)
