from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
from concurrent import futures
from grpc_reflection.v1alpha import reflection
from google.protobuf.timestamp_pb2 import Timestamp
from datetime import datetime, timezone
import psycopg2
import grpc
import time
import os
import write_service_pb2
import write_service_pb2_grpc


load_dotenv(override=True, dotenv_path='/Users/borisgans/personal/yaya/yaya_dev/.env')


POSTGRE_DB = os.getenv("POSTGRE_DB")
POSTGRE_USER = os.getenv("POSTGRE_USER")
POSTGRE_PW = os.getenv("POSTGRE_PW")
POSTGRE_HOST = os.getenv("POSTGRE_HOST")
POSTGRE_WRITE_PORT = os.getenv("POSTGRE_WRITE_PORT")

GRPC_INSC_PORT = os.getenv("GRPC_INSC_PORT")


GENDER_MAP = {0: "Male", 1: "Female", 2: "Other"}
SPEND_CLASS_MAP = {0: "A", 1: "B", 2: "C", 3: "D", 4: "E"}
ROLE_IDS = {"USER": 1, "DJ": 2, "ORGANIZER": 3, "VENUE": 4}
ROLE_NAMES = {v: k for k, v in ROLE_IDS.items()}
GENRE_ID_MAP = {
    0: 4,  # DNB
    1: 2,  # EDM
    2: 1,  # HOUSE
    3: 5,  # TECHNO
    4: 3,  # REGGAETON
    5: 6,  # AFRO_HOUSE
    6: 7   # DEEP_HOUSE
}


VENUE_TYPE_MAP = {
    "nightclub": 1,
    "warehouse": 2,
    "festival": 3,
    "rooftop": 4
}


pool = SimpleConnectionPool(1, 3,
    database=POSTGRE_DB,
    user=POSTGRE_USER,
    password=POSTGRE_PW,
    host=POSTGRE_HOST,
    port=POSTGRE_WRITE_PORT
)
err_msg = ""
print(f"Connection details: {POSTGRE_DB, POSTGRE_USER, POSTGRE_PW, POSTGRE_HOST, POSTGRE_WRITE_PORT}")


def db_query(query: str, *params):
    conn = pool.getconn()
    global err_msg
    err_msg = ""

    try:
        with conn.cursor() as cursor:
            print("Connection aqquired. Query to execute:")
            print(cursor.mogrify(query, params).decode())

            cursor.execute(query, params)
            conn.commit()
            result = cursor.fetchone()
            print(f"Data inserted successfully. {result}")
    
    except psycopg2.errors.DatabaseError as dbError:
        print(f"Database Error: \n{dbError}")
        err_msg = dbError
        conn.rollback()
        return None
    except psycopg2.errors.OperationalError as opError:
        print(f"Operational Error: \n{opError}")
        err_msg = opError
        conn.rollback()
        return None
    except Exception as genError:
        print(f"Unexpected Exception: \n{genError}")
        err_msg = genError
        conn.rollback()
        return None
    finally:
        pool.putconn(conn)
        print("Connection returned to pool.\n")
        return result[0]


def create_user_with_role(cursor, user_data, username_override=None, location_override=None, role_id=None) -> int:
    """
    Creates a user and assigns a role using the provided cursor.
    Returns the user_id if successful, raises exception if not.
    """
    try:
        # Insert user
        user_query = """
        INSERT INTO user_data(
            username, first_name, last_name, email, location, language, 
            gender, birthdate, spend_class, pw
        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        RETURNING id;
        """
        
        username = username_override or user_data.username
        location = location_override or user_data.location
        values = (
            username,
            user_data.first_name,
            user_data.last_name,
            user_data.email,
            location,
            user_data.language,
            GENDER_MAP.get(user_data.gender, 'Other'),
            user_data.birthdate,
            'NA',
            user_data.pw
        )
        
        cursor.execute(user_query, values)
        user_id = cursor.fetchone()[0]


        if role_id:
            status = 'confirmed' if role_id == ROLE_IDS["USER"] else 'pending'
            role_query = """
            INSERT INTO user_roles (user_id, role_id, status)
            VALUES (%s, %s, %s);
            """
            cursor.execute(role_query, (user_id, role_id, status))
            print(f"Created user + role with id and role: {user_id}, {ROLE_NAMES[role_id]}\n")

        return user_id
    except Exception as e:
        raise e


class WriteService(write_service_pb2_grpc.WriteServiceServicer):    


    def CreateEvent(self, request, context):
        print(f"\nReceived data: {request.data}")
        try:
            datetime = request.data.date.ToDatetime()
            datetime = datetime.replace(tzinfo=timezone.utc)
            postgre_datetime = datetime.isoformat()

            # Check if genres are provided
            if not request.data.genres:
                return write_service_pb2.CreateEntityResponse(
                    success=False, 
                    message="At least one genre must be specified."
                )

            org_query = """
                SELECT id FROM organizer WHERE user_id = %s;
            """
            organizer_id = db_query(org_query, request.data.org_id)
            
            if not organizer_id:
                return write_service_pb2.CreateEntityResponse(
                    success=False, 
                    message="Organizer not found."
                )

            # Start a transaction
            conn = pool.getconn()
            cur = conn.cursor()
            try:
                event_query = """
                    INSERT INTO event_data (organizer_id, venue_id, event_name, date, budget, pre_event_poster, pre_bio)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """
                event_values = (
                    organizer_id,
                    request.data.venue_id,
                    request.data.name,
                    postgre_datetime,
                    request.data.budget,
                    request.data.pre_event_poster,
                    request.data.pre_bio
                )

                cur.execute(event_query, event_values)
                event_id = cur.fetchone()[0]

                # Insert genres
                genre_query = """
                    INSERT INTO event_genres (event_id, genre_id)
                    VALUES (%s, %s);
                """

                for genre in request.data.genres:
                    genre_id = GENRE_ID_MAP.get(genre)
                    print(f"Genre {genre} to id: {genre_id}")

                    if genre in GENRE_ID_MAP:
                        cur.execute(genre_query, (event_id, genre_id))
                    else:
                        print(f"Warning: genre {genre} not found")

                conn.commit()
                return write_service_pb2.CreateEntityResponse(
                    success=True, 
                    message="Event and genres created successfully!"
                )

            except Exception as e:
                conn.rollback()
                print(f"Transaction failed: {e}")
                return write_service_pb2.CreateEntityResponse(
                    success=False, 
                    message=f"Transaction failed: {e}"
                )
            finally:
                cur.close()
                pool.putconn(conn)

        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(
                success=False, 
                message=f"Exception during writing: {e}"
            )
    
    def CreateUser(self, request, context):
        print(f"Received data: {request}")
        conn = pool.getconn()
        try:
            print("\nStarting transaction...")
            conn.autocommit = False  # Start transaction
            
            with conn.cursor() as cursor:
                # Create user account with USER role
                user_id = create_user_with_role(
                    cursor,
                    request.data,
                    role_id=ROLE_IDS["USER"]
                )

                # Insert user genres if any
                if request.data.genres:
                    genre_query = """
                    INSERT INTO user_genres (user_id, genre_id)
                    VALUES (%s, %s);
                    """
                    for genre_enum in request.data.genres:
                        genre_id = GENRE_ID_MAP.get(genre_enum)
                        if genre_id:
                            cursor.execute(genre_query, (user_id, genre_id))

            conn.commit()
            print("User created successfully!\n")

            return write_service_pb2.CreateEntityResponse(
                success=True, 
                message="User created successfully!"
            )
        except Exception as e:
            conn.rollback()
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(
                success=False, 
                message=f"Exception during writing: {e}"
            )
        finally:
            conn.autocommit = True
            pool.putconn(conn)

    def CreateDj(self, request, context):
        print(f"Received data: {request.data}")
        conn = pool.getconn()
        try:
            print("\nStarting transaction...")
            conn.autocommit = False  # Start transaction
            
            with conn.cursor() as cursor:
                # Create user account with DJ role
                user_id = create_user_with_role(
                    cursor,
                    request.data,
                    username_override=request.data.dj_name,
                    role_id=ROLE_IDS["DJ"]
                )

                # Create DJ entry with user_id
                dj_query = """
                INSERT INTO dj (
                    user_id, alias, first_name, last_name, bio, location, 
                    email, phone
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                RETURNING id;
                """
                
                dj_values = (
                    user_id,
                    request.data.dj_name,
                    request.data.first_name,
                    request.data.last_name,
                    request.data.bio,
                    request.data.location,
                    request.data.email,
                    request.data.phone
                )
                
                cursor.execute(dj_query, dj_values)
                dj_id = cursor.fetchone()[0]

                # Insert DJ genres
                if request.data.genres:
                    genre_query = """
                    INSERT INTO dj_genres (dj_id, genre_id)
                    VALUES (%s, %s);
                    """
                    for genre_enum in request.data.genres:
                        genre_id = GENRE_ID_MAP.get(genre_enum)
                        if genre_id:
                            cursor.execute(genre_query, (dj_id, genre_id))

                # Handle social data if present
                if request.data.HasField("social_data"):
                    social_query = """
                    INSERT INTO dj_socials (
                        dj_id, website, soundcloud, spotify, facebook, 
                        instagram, snapchat, x
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    social_values = (
                        dj_id,
                        request.data.social_data.website,
                        request.data.social_data.soundcloud,
                        request.data.social_data.spotify,
                        request.data.social_data.facebook,
                        request.data.social_data.instagram,
                        request.data.social_data.snapchat,
                        request.data.social_data.x
                    )
                    cursor.execute(social_query, social_values)

            conn.commit()
            print("DJ account created successfully!\n")
            
            return write_service_pb2.CreateEntityResponse(
                success=True,
                message="DJ account created successfully!"
            )

        except Exception as e:
            conn.rollback()
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(
                success=False,
                message=f"Error creating DJ account: {str(e)}"
            )
        finally:
            conn.autocommit = True
            pool.putconn(conn)

    def CreateVenue(self, request, context):
        print(f"Received data: {request.data}")
        conn = pool.getconn()
        try:
            print("\nStarting transaction...")
            conn.autocommit = False  # Start transaction
            
            with conn.cursor() as cursor:
                # Create user account with VENUE role
                user_id = create_user_with_role(
                    cursor,
                    request.data,
                    username_override=request.data.venue_name,
                    location_override=request.data.venue_city,
                    role_id=ROLE_IDS["VENUE"]
                )

                # Create venue entry
                venue_query = """
                INSERT INTO venues (
                    user_id, name, capacity, address, city, state, zip, country, table_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                RETURNING id;
                """

                venue_values = (
                    user_id,
                    request.data.venue_name,
                    request.data.venue_capacity,
                    request.data.venue_address,
                    request.data.venue_city,
                    request.data.venue_state,
                    request.data.venue_zip,
                    request.data.venue_country,
                    request.data.table_count
                )
                
                cursor.execute(venue_query, venue_values)
                venue_id = cursor.fetchone()[0]

                # Insert venue types
                if request.data.venue_types:
                    type_query = """
                    INSERT INTO venue_types (venue_id, type_id)
                    VALUES (%s, %s);
                    """
                    for type_name in request.data.venue_types:
                        type_id = VENUE_TYPE_MAP.get(type_name.lower())
                        if type_id:
                            cursor.execute(type_query, (venue_id, type_id))

            conn.commit()
            print("Venue account created successfully!\n")

            return write_service_pb2.CreateEntityResponse(
                success=True,
                message="Venue account created successfully!"
            )

        except Exception as e:
            conn.rollback()
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(
                success=False,
                message=f"Error creating venue account: {str(e)}"
            )
        finally:
            conn.autocommit = True
            pool.putconn(conn)

    def CreateOrganizer(self, request, context):
        print(f"Received data: {request.data}")
        conn = pool.getconn()
        try:
            print("\nStarting transaction...")
            conn.autocommit = False  # Start transaction
            
            with conn.cursor() as cursor:
                # Create user account with ORGANIZER role
                user_id = create_user_with_role(
                    cursor,
                    request.data,
                    username_override=request.data.org_name,
                    location_override=request.data.country,
                    role_id=ROLE_IDS["ORGANIZER"]
                )

                # Create organizer entry
                org_query = """
                INSERT INTO organizer (
                    user_id, name, first_name, last_name, email, phone, country, website
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                RETURNING id;
                """

                org_values = (
                    user_id,
                    request.data.org_name,
                    request.data.first_name,
                    request.data.last_name,
                    request.data.email,
                    request.data.phone,
                    request.data.country,
                    request.data.website
                )
                
                cursor.execute(org_query, org_values)
                org_id = cursor.fetchone()[0]

            conn.commit()
            print("Organizer account created successfully!\n")
            
            return write_service_pb2.CreateEntityResponse(
                success=True,
                message="Organizer account created successfully!"
            )

        except Exception as e:
            conn.rollback()
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(
                success=False,
                message=f"Error creating organizer account: {str(e)}"
            )
        finally:
            conn.autocommit = True
            pool.putconn(conn)
        
    def PublishEvent(self, request, context):
        print(f"Received data: {request.data}")
        try:

            query = """
                INSERT INTO published_events (event_id, event_poster, bio)
                VALUES (%s, %s, %s) RETURNING event_id;
            """
            values = (
                request.data.event_id,
                request.data.event_poster,
                request.data.bio
            )
            if db_query(query, *values) is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: {err_msg}")

            return write_service_pb2.CreateEntityResponse(success=True, message="Event published!")
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Exception during writing: {e}")
        
    def AddDjEvent(self, request, context):
        print(f"Received data: {request.data}")
        try:

            query = """
                INSERT INTO event_dj (event_id, dj_id)
                VALUES (%s, %s) RETURNING event_id;
            """
            values = (
                request.data.event_id, 
                request.data.dj_id
            )

            if db_query(query, *values) is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: Unable to add DJ to event: {err_msg}")

            return write_service_pb2.CreateEntityResponse(success=True, message="DJ added to event successfully!")
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Exception during writing: {e}")
    
    def DeleteEvent(self, request, context):
        print("Deleting event...\n")
        print(f"Received data: {request.data}")
        try:
            # Get a connection and start transaction
            conn = pool.getconn()
            cur = conn.cursor()
            
            try:
                # First check if event exists and belongs to this organizer
                verify_query = """
                    SELECT EXISTS (
                        SELECT 1 
                        FROM event_data 
                        WHERE id = %s AND organizer_id = %s
                    );
                """
                cur.execute(verify_query, (request.data.event_id, request.data.organizer_id))
                event_exists = cur.fetchone()[0]
                print(f"Event exists check: {event_exists}")
                
                if not event_exists:
                    conn.rollback()
                    return write_service_pb2.CreateEntityResponse(
                        success=False, 
                        message="Event not found or does not belong to this organizer"
                    )

                # Debug: Print event details before deletion
                cur.execute("""
                    SELECT id, organizer_id FROM event_data WHERE id = %s;
                """, (request.data.event_id,))
                event_details = cur.fetchone()
                print(f"Event details before deletion: {event_details}")

                # Check if event is published
                published_check = """
                    SELECT EXISTS (
                        SELECT 1 
                        FROM published_events 
                        WHERE event_id = %s
                    );
                """
                cur.execute(published_check, (request.data.event_id,))
                is_published = cur.fetchone()[0]
                print(f"Event published check: {is_published}")
                
                if is_published:
                    conn.rollback()
                    return write_service_pb2.CreateEntityResponse(
                        success=False, 
                        message="Cannot delete a published event"
                    )

                # Debug: Check for existing DJ relationships
                cur.execute("""
                    SELECT COUNT(*) FROM event_dj WHERE event_id = %s;
                """, (request.data.event_id,))
                dj_count = cur.fetchone()[0]
                print(f"Number of DJ relationships to delete: {dj_count}")

                # Delete from event_dj first (foreign key constraint)
                delete_djs = """
                    DELETE FROM event_dj 
                    WHERE event_id = %s
                    RETURNING event_id;
                """
                cur.execute(delete_djs, (request.data.event_id,))
                deleted_djs = cur.fetchall()
                print(f"Deleted DJ relationships: {deleted_djs}")

                # Finally delete the event itself
                delete_event = """
                    DELETE FROM event_data 
                    WHERE id = %s AND organizer_id = %s
                    RETURNING id;
                """
                cur.execute(delete_event, (request.data.event_id, request.data.organizer_id))
                deleted_event = cur.fetchone()
                print(f"Deleted event result: {deleted_event}")

                # Verify deletion
                verify_deletion = """
                    SELECT EXISTS (
                        SELECT 1 
                        FROM event_data 
                        WHERE id = %s
                    );
                """
                cur.execute(verify_deletion, (request.data.event_id,))
                still_exists = cur.fetchone()[0]
                
                if still_exists:
                    conn.rollback()
                    print("Event still exists after deletion attempt!")
                    return write_service_pb2.CreateEntityResponse(
                        success=False,
                        message="Failed to delete event: Event still exists after deletion"
                    )

                # Commit the transaction
                conn.commit()
                print("Transaction committed successfully")
                
                if not deleted_event:
                    return write_service_pb2.CreateEntityResponse(
                        success=False,
                        message="Event deletion failed: No rows were deleted"
                    )
                
                return write_service_pb2.CreateEntityResponse(
                    success=True, 
                    message=f"Event {request.data.event_id} successfully deleted"
                )

            except Exception as e:
                conn.rollback()
                print(f"Transaction failed: {e}")
                return write_service_pb2.CreateEntityResponse(
                    success=False, 
                    message=f"Transaction failed: {str(e)}"
                )
            finally:
                cur.close()
                pool.putconn(conn)

        except Exception as e:
            print(f"Exception during deletion: {e}")
            return write_service_pb2.CreateEntityResponse(
                success=False, 
                message=f"Exception during deletion: {str(e)}"
            )



# Run gRPC Server
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # 10 workers for concurrent requests
    write_service_pb2_grpc.add_WriteServiceServicer_to_server(WriteService(), server)
    SERVICE_NAMES = (
        write_service_pb2.DESCRIPTOR.services_by_name['WriteService'].full_name,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    server.add_insecure_port(GRPC_INSC_PORT)
    server.start()
    print(f"gRPC server started on port: {GRPC_INSC_PORT}")

    try:
        while True:
            time.sleep(5)
            # logic to check if database works?
    except KeyboardInterrupt:
        print("Shutting down due to manual interruption")

    finally:
        print("Closing all DB connections...")
        pool.closeall()
        server.stop(0)
        print("Server stopped.")

if __name__ == "__main__":
    serve()
