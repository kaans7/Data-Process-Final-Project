import psycopg2
import csv

f= open("password.txt",'r')
pssword=f.read()

conn=None
cur=None

conn = psycopg2.connect(
    database='dataproject',
    host="localhost",
    user="postgres",
    password=pssword,
    port=5432)
cur =conn.cursor()

def extract_table_to_csv(table_name, file_name):
    # Query to select all data from the table
    query = f"SELECT * FROM {table_name};"
    cur.execute(query) 
    rows = cur.fetchall()

    # Get the column names
    column_names = [desc[0] for desc in cur.description]

    # Write data to CSV file with UTF-8 encoding
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(column_names)
        csv_writer.writerows(rows)


# Extract each table to a CSV file
tables = ["artist_rock", "album_rock", "track_rock", "analyze_rock"]
for table in tables:
    extract_table_to_csv(table, f"{table}.csv")