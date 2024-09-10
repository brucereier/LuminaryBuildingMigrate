import json
import psycopg2

# Connect to PostgreSQL
pg_conn = psycopg2.connect(
    dbname="postgres", 
    user="postgres", 
    password="postgres", 
    host="postgresql.dept-polaris.svc.cluster.local"
)
pg_cur = pg_conn.cursor()

# Load building data from data.json to create a lookup of MongoDB ID to PostgreSQL building ID
with open('data.json') as f:
    buildings_data = json.load(f)

# Create a lookup dictionary to map MongoDB _id to building names
building_lookup = {building['_id']: building['name'] for building in buildings_data}

def get_building_id_by_name(building_name, cursor):
    # Look up building_id by name in the PostgreSQL building table
    cursor.execute("SELECT building_id FROM building WHERE name = %s", (building_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        print(f"Building with name {building_name} not found.")
        return None

# Migrate Ramps
def migrate_ramps():
    # Check if the ramp table already contains data
    pg_cur.execute("SELECT COUNT(*) FROM ramp")
    ramp_count = pg_cur.fetchone()[0]

    if ramp_count > 0:
        print("Data already exists in the ramp table. Skipping ramp migration.")
        return

    # Load ramps data from file
    with open('ramps.json') as f:
        ramps = json.load(f)

    for ramp in ramps:
        old_building_id = ramp.get('building', None)
        building_name = building_lookup.get(old_building_id, None)
        building_id = None
        if building_name:
            building_id = get_building_id_by_name(building_name, pg_cur)
        
        # Insert into the ramp table
        pg_cur.execute(
            """
            INSERT INTO ramp (latitude, longitude, building) 
            VALUES (%s, %s, %s) RETURNING ramp_id
            """,
            (ramp['latitude'], ramp['longitude'], building_id)
        )
        ramp_id = pg_cur.fetchone()[0]
        print(f"Ramp {ramp['_id']} inserted as {ramp_id}")

# Migrate Doors and Associate with Ramps
def migrate_doors_and_associate_ramps():
    # Check if the door table already contains data
    pg_cur.execute("SELECT COUNT(*) FROM door")
    door_count = pg_cur.fetchone()[0]

    if door_count > 0:
        print("Data already exists in the door table. Skipping door migration.")
        return

    # Check if the DoorsAndRamps table already contains data
    pg_cur.execute("SELECT COUNT(*) FROM DoorsAndRamps")
    doors_and_ramps_count = pg_cur.fetchone()[0]

    if doors_and_ramps_count > 0:
        print("Data already exists in the DoorsAndRamps table. Skipping door-ramp associations.")
        return

    # Load doors data from file
    with open('doors.json') as f:
        doors = json.load(f)

    for door in doors:
        old_building_id = door.get('building', None)
        building_name = building_lookup.get(old_building_id, None)
        building_id = None
        if building_name:
            building_id = get_building_id_by_name(building_name, pg_cur)

        # Insert into the door table
        pg_cur.execute(
            """
            INSERT INTO door (latitude, longitude, building_id, is_emergency, is_service, is_indoor) 
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING door_id
            """,
            (door['latitude'], door['longitude'], building_id, door.get('emergency', False), 
             door.get('service', False), door.get('entrance', False))
        )
        door_id = pg_cur.fetchone()[0]
        print(f"Door {door['_id']} inserted as {door_id}")

        # If the door has associated ramps, insert those into DoorsAndRamps table
        for ramp_mongo_id in door.get('ramps', []):
            # Look up ramp using the old MongoDB ID (assuming ramp_id was inserted using this ID)
            pg_cur.execute("SELECT ramp_id FROM ramp WHERE ramp_id = %s", (ramp_mongo_id,))
            result = pg_cur.fetchone()
            if result:
                ramp_id = result[0]
                pg_cur.execute(
                    "INSERT INTO DoorsAndRamps (door_id, ramp_id) VALUES (%s, %s)", 
                    (door_id, ramp_id)
                )
                print(f"Associated door {door_id} with ramp {ramp_id}")

# Migrate ramps
migrate_ramps()

# Migrate doors and associate them with ramps
migrate_doors_and_associate_ramps()

# Commit the transactions
pg_conn.commit()

# Close the connections
pg_cur.close()
pg_conn.close()

print("Migration completed successfully.")