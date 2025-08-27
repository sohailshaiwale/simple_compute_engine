# Simple Compute Engine  

A lightweight, pandas/Spark-inspired **dataframe-like engine** built from scratch in pure Python.  
This project provides SQL-like operations such as filtering, selecting, grouping, joins, caching, and CSV I/O — without external dependencies.  

---

## 🚀 Features
- **Read & Write CSVs** with automatic header inference.  
- **DataFrame operations**:
  - `head(n)` → Preview top rows  
  - `columns()` → Get column names  
  - `filter(column, value)` → Filter rows  
  - `select(*cols)` → Select specific columns  
  - `drop(col)` → Drop a column  
  - `withColumnRenamed(old, new)` → Rename a column  
  - `with_column(col, operation, value)` → Add/update column values (`+`, `-`, `*`, `/`)  
- **Sorting**: `sort(column, ascending=True)`  
- **GroupBy**:
  - `.count()` → Row counts by key  
  - `.sum(column)` → Summation by group  
- **Joins**: `inner`, `left`, `right`, `outer`  
- **Caching**: `cache()` & `unpersist()` for in-memory optimization  
- **Chainable API** like pandas / Spark  

---

## 📦 Installation
Clone the repo and use directly (no external dependencies required):  
```bash
git clone https://github.com/yourusername/simple-compute-engine.git
cd simple-compute-engine
