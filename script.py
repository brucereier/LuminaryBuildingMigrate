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

# Migrate Ramps and Create a MongoDB to PostgreSQL ID Mapping
def migrate_ramps():
    # Check if the ramp table already contains data
    pg_cur.execute("SELECT COUNT(*) FROM ramp")
    ramp_count = pg_cur.fetchone()[0]

    if ramp_count > 0:
        print("Data already exists in the ramp table. Skipping ramp migration.")
        return {}

    # Load ramps data from file
    with open('ramps.json') as f:
        ramps = json.load(f)

    # Dictionary to store MongoDB ID -> PostgreSQL ID mapping
    ramp_id_map = {}

    for ramp in ramps:
        old_building_id = ramp.get('building', None)
        building_name = building_lookup.get(old_building_id, None)
        building_id = None
        if building_name:
            building_id = get_building_id_by_name(building_name, pg_cur)

        # Insert into the ramp table and get the new Postgres ramp_id
        pg_cur.execute(
            """
            INSERT INTO ramp (latitude, longitude, building) 
            VALUES (%s, %s, %s) RETURNING ramp_id
            """,
            (ramp['latitude'], ramp['longitude'], building_id)
        )
        ramp_id = pg_cur.fetchone()[0]

        # Map the MongoDB ramp _id to the new PostgreSQL ramp_id
        ramp_id_map[ramp['_id']] = ramp_id
        print(f"Ramp {ramp['_id']} inserted as {ramp_id}")

    return ramp_id_map

# Migrate Doors and Associate with Ramps
def migrate_doors_and_associate_ramps(ramp_id_map):
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

        # Insert into the door table and get the new Postgres door_id
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
            # Look up ramp using the old MongoDB ID in our mapping
            ramp_id = ramp_id_map.get(ramp_mongo_id)
            if ramp_id:
                pg_cur.execute(
                    "INSERT INTO DoorsAndRamps (door_id, ramp_id) VALUES (%s, %s)", 
                    (door_id, ramp_id)
                )
                print(f"Associated door {door_id} with ramp {ramp_id}")
            else:
                print(f"Ramp with Mongo ID {ramp_mongo_id} not found for door {door['_id']}")

# Migrate ramps and create the ID map
ramp_id_map = migrate_ramps()

# Migrate doors and associate them with ramps using the ramp_id_map
migrate_doors_and_associate_ramps(ramp_id_map)

# Commit the transactions
pg_conn.commit()

# Close the connections
pg_cur.close()
pg_conn.close()

print("Migration completed successfully.")