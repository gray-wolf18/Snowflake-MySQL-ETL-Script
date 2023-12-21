import mysql.connector
import snowflake.connector
print('starting')
# MySQL configuration
mysql_config = {
    'host': '172.16.119.128',
    'user': 'abuelayyan',
    'password': '55255haM@',
    'port': 3306
}

# Snowflake configuration
snowflake_config = {
    'user': 'abuelayyan',
    'password': '55255haM@',
    'account': 'hbsnioh-jq41214',
    'warehouse': 'COMPUTE_WH',
    'database': 'test',
    'schema': 'public'
}

# Function to map MySQL data types to Snowflake data types
def map_data_type(mysql_type):
    if 'int' in mysql_type.lower():
        return 'INTEGER'
    elif 'varchar' in mysql_type.lower():
        return 'VARCHAR'
    elif 'timestamp' in mysql_type.lower():
        return 'TIMESTAMP'
    else:
        return 'STRING'

# Function to get table structure from MySQL with database name
def get_mysql_table_structure(conn, database_name, table_name):
    with conn.cursor(buffered=True) as cursor:
        cursor.execute(f"USE `{database_name}`")  # Switch to the correct MySQL database
        cursor.execute(f"DESCRIBE `{table_name}`")
        columns = cursor.fetchall()
        return columns

# Function to create table in Snowflake with additional columns
def create_table_in_snowflake(cursor, database_name, table_name, columns):
    column_definitions = []
    for column in columns:
        # Set all columns to VARCHAR type
        column_definitions.append(f'"{column[0].upper()}" VARCHAR')

    # Add additional columns
    additional_columns = ["ts INT","type VARCHAR", "offset INTEGER"]
    all_column_definitions = column_definitions + additional_columns
    print(all_column_definitions)
    print('checked')
    # Construct the CREATE TABLE statement with all columns
    create_statement = f'CREATE TABLE IF NOT EXISTS "{database_name}"."PUBLIC".{table_name} ({", ".join(all_column_definitions)});'
    cursor.execute(create_statement)


    cursor.execute(create_statement)

def insert_data_snowflake_to_snowflake(sf_cursor, table_name, destination_table):
    # Fetch the latest timestamp and offset from the destination table
    sf_cursor.execute(f"""
        SELECT MAX(ts), MAX(offset)
        FROM {destination_table}
    """)
    latest_record = sf_cursor.fetchone()
    latest_ts, latest_offset = latest_record if latest_record else (None, None)

    # Fetch unique keys from the data object
    sf_cursor.execute(f"""
        SELECT DISTINCT key AS data_key
        FROM {snowflake_config['database']}.{snowflake_config['schema']}.{table_name},
        LATERAL FLATTEN(INPUT => RECORD_CONTENT:"data")
        WHERE RECORD_CONTENT:"data" IS NOT NULL
    """)
    data_keys = [row[0] for row in sf_cursor.fetchall()]

    # Construct the dynamic SQL query
    dynamic_columns = ', '.join([f'RECORD_CONTENT:"data":{key}::string AS {key}' for key in data_keys])
    select_query = f"""
        SELECT
          RECORD_CONTENT:"ts" AS ts,
          RECORD_CONTENT:"type" AS type,
          {dynamic_columns},
          RECORD_METADATA:"offset" AS offset
        FROM
          {snowflake_config['database']}.{snowflake_config['schema']}.{table_name}
    """

    # Adjust the query to handle NULL values correctly
    if latest_ts is not None and latest_offset is not None:
        select_query += f" WHERE (RECORD_CONTENT:\"ts\" > {latest_ts}) OR (RECORD_CONTENT:\"ts\" = {latest_ts} AND RECORD_CONTENT:\"offset\" > {latest_offset})"
    elif latest_ts is not None:
        select_query += f" WHERE RECORD_CONTENT:\"ts\" > {latest_ts}"
    elif latest_offset is not None:
        select_query += f" WHERE RECORD_CONTENT:\"offset\" > {latest_offset}"

    sf_cursor.execute(select_query)
    rows = sf_cursor.fetchall()

    if rows:
        # Define the column names for the INSERT statement dynamically
        column_names = ['ts', 'type'] + data_keys + ['offset']
        insert_query = f"INSERT INTO {destination_table} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))});"

        for row in rows:
            sf_cursor.execute(insert_query, row)
        print(f"{len(rows)} record(s) inserted into table {destination_table}.")
    else:
        print("No new data to insert based on the latest ts and offset.")



# Main execution
try:
    # Connect to Snowflake and MySQL
    sf_conn = snowflake.connector.connect(**snowflake_config)
    sf_cursor = sf_conn.cursor()
    mysql_conn = mysql.connector.connect(**mysql_config)

    # Retrieve tables from Snowflake
    sf_cursor.execute("SHOW TABLES IN test.public")
    sf_tables = sf_cursor.fetchall()

    # Process each table from Snowflake
    for table_info in sf_tables:
        full_table_name = table_info[1] 
        print(full_table_name) # Get the full table name including the database name
        index = full_table_name.find('_')
        if index != -1:
            # Split the full table name into database and table names
            snowflake_db_name = full_table_name[:index].lower()  # Snowflake database name
            table_name = full_table_name[index + 1:].lower()  # Snowflake table name
            
            # Get the structure from MySQL and create a corresponding table in Snowflake
            mysql_db_name = snowflake_db_name  
            columns = get_mysql_table_structure(mysql_conn, mysql_db_name, table_name)
            snowflake_db_name = snowflake_db_name.upper()  # Convert database name to uppercase for Snowflake
            create_table_in_snowflake(sf_cursor, snowflake_db_name, table_name, columns)
            
            # Define your source and destination tables
            source_table = f"{snowflake_db_name}.PUBLIC.{table_name}"
            destination_table = f"{snowflake_db_name}.PUBLIC.{table_name}"  # Name your destination table
            
            # Insert data from the source table to the destination table in Snowflake
            insert_data_snowflake_to_snowflake(sf_cursor, full_table_name, destination_table)
            
            # Commit changes after each table
            sf_conn.commit()
            print(f"Data inserted into table {destination_table} from {source_table}.")
        else:
            print(f"Invalid Snowflake table name format: '{full_table_name}'")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close connections
    if mysql_conn.is_connected():
        mysql_conn.close()
    
        sf_cursor.close()
        sf_conn.close()
