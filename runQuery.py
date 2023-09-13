import sys
import cx_Oracle
import csv

# Read the query from the text file
def read_query_from_file(file_path):
    with open(file_path, 'r') as query_file:
        query = query_file.read().replace('\n', ' ')
    return query

# Connect to Oracle Database
def connect_to_db(user, password, host, port, sid):
    dsn = cx_Oracle.makedsn(host, port, sid=sid)
    connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
    return connection

# Execute the query and write the results to a CSV file
def execute_query_and_write_to_csv(connection, query, csv_file_path):
    cursor = connection.cursor()
    cursor.execute(query)
    
    # Get the column names (header)
    column_names = [desc[0] for desc in cursor.description]
    
    # Write the header to the CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(column_names)
        
        # Write the data rows to the CSV file
        csv_writer.writerows(cursor)
    
    cursor.close()

if __name__ == "__main__":
    # Database credentials
    oracle_user = sys.argv[1]
    oracle_password = sys.argv[2]
    oracle_host = sys.argv[3]
    oracle_port = sys.argv[4]
    oracle_sid = sys.argv[5]

    # Path to the query text file
    query_file_path = sys.argv[6]

    # Path to the output CSV file
    csv_output_file_path = 'queryOutput.csv'

    # Read the query from the text file
    query = read_query_from_file(query_file_path)

    # Connect to the Oracle database
    connection = connect_to_db(oracle_user, oracle_password, oracle_host, oracle_port, oracle_sid)

    # Execute the query and get the result in csv
    execute_query_and_write_to_csv(connection,query,csv_output_file_path)
    
    # Close the database connection
    connection.close()

    print("Query executed and result exported to CSV.")
