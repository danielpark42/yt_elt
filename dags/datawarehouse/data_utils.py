from airflow.providers.postgres.hooks.postgres import PostgresHook

# This will allow retrieval of reports using Python dictionaries and not the default tuples.
from psycopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", db="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    return conn, cur

def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):

    conn, cur = get_conn_cursor()

    # only create schema if it does not already exist
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)

    #making changes to the db, therefore need to commit these changes to the db
    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "Video_Type" VARCHAR(10) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT    
                ); 
            """
    
    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

# Get all video IDs in either the staging or the core layer tables
# This will be helpful when we loop through the rows of data inside the tables
def get_video_ids(cur, schema):

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()
    # exmaple output of ids: [{'Video_ID': 'abc123'}, {'Video_ID': 'def456'}, ...]

    video_ids = [row["Video_ID"] for row in ids]
    # exmaple output of video_ids: ['abc123', 'def456', ...]

    return video_ids
