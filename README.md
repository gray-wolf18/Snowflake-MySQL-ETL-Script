# Snowflake-MySQL-ETL-Script
## GitHub README for Snowflake-MySQL ETL Script

### Introduction
This Python script is designed to perform ETL (Extract, Transform, Load) operations between MySQL and Snowflake databases. It primarily extracts data from MySQL, transforms the data by mapping MySQL data types to corresponding Snowflake types, and loads the data into Snowflake.

### Key Features
- **Data Type Mapping:** Converts MySQL data types to compatible Snowflake data types.
- **Dynamic Table Structure Retrieval:** Fetches table structures from MySQL databases.
- **Snowflake Table Creation:** Dynamically creates tables in Snowflake with additional timestamp, type, and offset columns.
- **Data Insertion:** Inserts data from Snowflake source tables to destination tables, handling the latest timestamps and offsets.
- **Error Handling:** Includes robust error handling for smoother ETL processes.

### Prerequisites
- Python 3.x
- `mysql-connector-python`
- `snowflake-connector-python`

### Installation
1. Install necessary Python packages:
   ```bash
   pip install mysql-connector-python snowflake-connector-python
   ```

### Configuration
Fill in the `mysql_config` and `snowflake_config` dictionaries with appropriate connection details:

```python
mysql_config = {
    'host': '<MySQL Host>',
    'user': '<MySQL User>',
    'password': '<MySQL Password>',
    'port': 3306
}

snowflake_config = {
    'user': '<Snowflake User>',
    'password': '<Snowflake Password>',
    'account': '<Snowflake Account>',
    'warehouse': '<Snowflake Warehouse>',
    'database': 'test',
    'schema': 'public'
}
```

### Usage
Run the script in a Python environment. The script will perform the following steps:
1. Connect to both MySQL and Snowflake.
2. Retrieve and process tables from Snowflake.
3. For each table, get the MySQL structure, create a corresponding table in Snowflake, and transfer the data.

### Security Notice
Avoid hardcoding sensitive information like database credentials. Use environment variables or secure secrets management solutions.

### Disclaimer
This script is provided as-is, with no warranties. Always test in a non-production environment before use.

---

**Note**: Ensure you have the appropriate permissions and that the configuration details are correct for a successful ETL process.
