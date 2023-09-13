import sys
import csv
import cx_Oracle
from datetime import datetime

# Read column names from csv (For Header Row, containing column names)
def read_csv_columns(file_path):
    with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.reader(csv_file)
        header_row = next(csv_reader)
        return header_row

# Read a sample of data understanding the data types
def read_csv_data_sample(file_path, sample_size):
    data_sample = []
    with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.reader(csv_file)
        header_row = next(csv_reader)  # Skip the header row
        for _ in range(sample_size):
            try:
                row = next(csv_reader)
                data_sample.append(row)
            except StopIteration:
                break
    return data_sample

# Read the whole CSV data
def read_csv_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            # append data one by one
            data.append(row)
    return data

# Connect to the Oracle database
def connect_to_db(user, password, host, port, sid):
    dsn = cx_Oracle.makedsn(host, port, sid=sid)
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    return connection

# Function for getting the datatypes for each column ******
def infer_data_type(values):
    for value in values:
        try:
            int(value)
            return "NUMBER"
        except ValueError:
            try:
                float(value)
                return "NUMBER"
            except ValueError:
                try:
                    # Adjust format as needed
                    datetime.strptime(value, '%Y-%m-%d')
                    return "DATE"
                except ValueError:
                    pass
    return "VARCHAR2(255)"  # Default to string if no other type is inferred

# Function for genarating the CREATE TABLE query
def generate_create_table_sql(table_name, header_row, data_sample):
    column_definitions = []
    print("Zip of column and sample data", data_sample)
    for col, sample_value in zip(header_row, zip(*data_sample)):
        # print("Column value  : ", col)
        # print("Sample value  : ", sample_value)
        data_type = infer_data_type(sample_value)
        column_definitions.append(f"{col} {data_type}")
    sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
    return sql

# Function for updating / Inserting new rows and syncing with the exixting data
def synchronize_data(connection, data, table_name, csv_columns, data_sample):
    cursor = connection.cursor()

    # Check if the table exists
    table_exists = does_table_exist(connection, table_name)

    if not table_exists:
        # Generate dynamic table name and SQL
        create_table_sql = generate_create_table_sql(table_name, csv_columns, data_sample)
        cursor.execute(create_table_sql)

    # Get the existing column names from the database table
    existing_columns = get_existing_columns(connection, table_name)

    # Add any new columns to the table
    add_new_columns(connection, table_name, csv_columns, existing_columns, data_sample)
    # print("CSV Data : ", csv_columns)
    # print("Existing-Col Data : ", existing_columns)
    for row in data:
        id = row[csv_columns[0]]  # Use the unique identifier from the CSV

        # Check if the identifier exists in the target table
        cursor.execute(f"SELECT * FROM {table_name} WHERE {csv_columns[0]} = :1", (id,))
        existing_record = cursor.fetchone()

        if existing_record:
            # Update existing record if values have changed
            update_existing_record(connection, table_name, csv_columns ,existing_record, row)
        else:
            # Insert new record
            insert_new_record(connection, table_name, row)

    connection.commit()
    cursor.close()

# Check if the table exists in the database
def does_table_exist(connection, table_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE ROWNUM = 1")
        return True
    except cx_Oracle.DatabaseError:
        return False

# Get existing column names from the database table
def get_existing_columns(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SELECT column_name FROM user_tab_columns WHERE table_name = :1", (table_name,))
    return [row[0] for row in cursor]

# Add new columns to the table
def add_new_columns(connection, table_name, csv_columns, existing_columns, sample_data):
    cursor = connection.cursor()
    existing_col_lower = [col.lower() for col in existing_columns]  # Convert existing column names to lowercase
    print("CSV Data : ", csv_columns)
    print("Existing-Col Data : ", existing_columns)
    for col in csv_columns:
        col_lower = col.lower()  # Convert CSV column name to lowercase
        if col_lower not in existing_col_lower:
            sample_col_value = sample_data[0][csv_columns.index(col)]  # Get the sample value for this column
            data_type = infer_data_type([sample_col_value])
            cursor.execute(f"ALTER TABLE {table_name} ADD {col} {data_type}")
            existing_columns.append(col_lower)  # Add the lowercase column name to the existing_columns list
            existing_col_lower.append(col_lower)  # Add the lowercase column name to the existing_lower list
    connection.commit()

# Update existing record if values have changed
def update_existing_record(connection, table_name, csv_columns, existing_record, new_row):
    cursor = connection.cursor()
    identifier_col = csv_columns[0]  # Use the first column from csv_columns as the identifier
    identifier_value = new_row[identifier_col]
    bind_values = {}  # Dictionary to hold bind variable values

    update_columns = []
    for i, col in enumerate(csv_columns[1:], start=1):  # Exclude the identifier column
        if existing_record[i] != new_row[col]:  # Fetch value using tuple index
            update_columns.append(f"{col} = :{col}")
            bind_values[col] = new_row[col] # store bind variable value
    
    if update_columns:
        update_sql = f"UPDATE {table_name} SET {', '.join(update_columns)} WHERE {identifier_col} = :{identifier_col}"
        bind_values[identifier_col] = identifier_value  # Add identifier bind variable value
        # print("Update SQL:", update_sql)  # Debug: Print the update SQL statement
        # print("Bind Values:", bind_values)  # Debug: Print the bind values

        cursor.execute(update_sql, bind_values)
        connection.commit()
    
    cursor.close()

# Insert new record
def insert_new_record(connection, table_name, row):
    cursor = connection.cursor()
    columns = ', '.join(row.keys())
    placeholders = ', '.join([f":{i+1}" for i in range(len(row))])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(insert_sql, list(row.values()))
    connection.commit()


if __name__ == "__main__":
    # Read database credentials and CSV file path from command-line arguments or config
    oracle_user = sys.argv[1]
    oracle_password = sys.argv[2]
    oracle_host = sys.argv[3]
    oracle_port = sys.argv[4]
    oracle_sid = sys.argv[5]

    # Read CSV file path from config or command-line arguments
    csv_file_path = sys.argv[6]

    # Connect to the database
    connection = connect_to_db(oracle_user, oracle_password, oracle_host, oracle_port, oracle_sid)

    # Read CSV header and a few rows of data as sample
    csv_columns = read_csv_columns(csv_file_path)
    data_sample = read_csv_data_sample(csv_file_path, sample_size=len(csv_columns))

    # Generate dynamic table name and SQL
    target_table_name = 'TARGET_DYNAMIC_TABLE'
    
    # Read and transform updated CSV data
    updated_data = read_csv_file(csv_file_path)

    # Insert data into the dynamic table
    synchronize_data(connection, updated_data, target_table_name, csv_columns, data_sample)

    print(f"Check The DB now..")

    # Close the database connection
    connection.close()