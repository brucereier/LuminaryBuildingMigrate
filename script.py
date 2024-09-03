import json
import psycopg2

# Load JSON data from file
with open('data.json') as f:
    data = json.load(f)

# Connect to PostgreSQL
pg_conn = psycopg2.connect(
    dbname="postgres", 
    user="postgres", 
    password="postgres", 
    host="postgresql.dept-polaris.svc.cluster.local"
)
pg_cur = pg_conn.cursor()

def get_campus_id(campus_name, cursor):
    # Check if campus exists
    cursor.execute("SELECT campus_id FROM campus WHERE name = %s", (campus_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        # Insert new campus and return its ID
        cursor.execute("INSERT INTO campus (name) VALUES (%s) RETURNING campus_id", (campus_name,))
        return cursor.fetchone()[0]

# Iterate over each entry in the JSON data
for doc in data:
    # Extract fields from JSON
    building_name = doc['name']
    full_name = doc['name']
    abbreviation = doc.get('abbreviation', None)
    latitude = doc.get('defaultLatitude', None)
    longitude = doc.get('defaultLongitude', None)
    address = doc.get('address', None)
    campus_name = doc.get('campus', None)
    
    # Get or create campus_id
    campus_id = get_campus_id(campus_name, pg_cur)
    
    # Insert into building table
    pg_cur.execute(
        "INSERT INTO building (name) VALUES (%s) RETURNING building_id", 
        (building_name,)
    )
    building_id = pg_cur.fetchone()[0]
    
    # Insert into location table
    pg_cur.execute(
        """
        INSERT INTO location (full_name, abbreviation, defaultLatitude, defaultLongitude, geo_address, campus_id) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (full_name, abbreviation, latitude, longitude, address, campus_id)
    )

# Commit the transactions
pg_conn.commit()

# Close the connections
pg_cur.close()
pg_conn.close()