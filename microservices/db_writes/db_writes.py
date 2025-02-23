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
            print("Data inserted successfully.")
    
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
    return result[0] or 1


class WriteService(write_service_pb2_grpc.WriteServiceServicer):    
    def CreateEvent(self, request, context):
        print(f"\nReceived data: {request.data}")
        try:
            datetime = request.data.date.ToDatetime()
            datetime = datetime.replace(tzinfo=timezone.utc)
            postgre_datetime = datetime.isoformat()

            query = """
                INSERT INTO event_data (organizer_id, venue_id, event_name, date, budget, pre_event_poster, pre_bio)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """
            values = (
                request.data.org_id,
                request.data.venue_id,
                request.data.name,
                postgre_datetime,
                request.data.budget,
                request.data.pre_event_poster,
                request.data.pre_bio
            )
            if db_query(query, *values) is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: {err_msg}")

            return write_service_pb2.CreateEntityResponse(success=True, message="Event created!")
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Exception during writing: {e}")
    
    def CreateUser(self, request, context):
        print(f"Received data: {request}")

        try:
            query = """
            INSERT INTO user_data(
                username, first_name, last_name, email, location, language, gender, birthdate, spend_class, pw
            ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """

            birth_datetime = datetime.fromisoformat(request.data.birthdate)
            values = (
                request.data.username,
                request.data.first_name,
                request.data.last_name,
                request.data.email,
                request.data.location,
                request.data.language,
                GENDER_MAP.get(request.data.gender, 'Other'),
                birth_datetime,
                'NA', #when user registers spend class will never be known
                request.data.pw,
            )

            if db_query(query, *values) is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: {err_msg}")

            return write_service_pb2.CreateEntityResponse(success=True, message="User created!")
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Unexpected Exception: {e}")

    def CreateDj(self, request, context):
        print(f"Received data: {request.data}")
        try:
            # MISSING: initial onboarding genres insert;
                # FUTURE: background process to get monthly_streams, social_followers, and better genre representation
                    # DB trigger to auto update metrics column when any of the above get updated
            query = """
            INSERT INTO dj (
                alias, first_name, last_name, bio, location, email, phone
            ) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """
            values = (
                request.data.dj_name,
                request.data.first_name,
                request.data.last_name,
                request.data.bio,
                request.data.location,
                request.data.email,
                request.data.phone
            )

            dj_id = db_query(query, *values)
            if dj_id is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: {err_msg}")


            if request.data.HasField("social_data"):
                print("Processing social data...")
                social = request.data.social_data

                social_query = """
                INSERT INTO dj_socials (dj_id, website, soundcloud, spotify, facebook, instagram, snapchat, x) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING dj_id;
                """
                social_values = (
                    dj_id,
                    social.website or None,
                    social.soundcloud or None,
                    social.spotify or None,
                    social.facebook or None,
                    social.instagram or None,
                    social.snapchat or None,
                    social.x or None
                )

                social_insert = db_query(social_query, *social_values)
                if social_insert is None:
                    return write_service_pb2.CreateEntityResponse(success=False, message="DB Error with social data")

            return write_service_pb2.CreateEntityResponse(success=True, message="DJ created!")
        
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Exception during writing: {e}")

    def CreateVenue(self, request, context):
        print(f"Received data: {request.data}")
        try:
            query = """
            INSERT INTO venues (
                name, capacity, address, city, state, zip, country, table_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """

            values = (
                request.data.venue_name,
                request.data.venue_capacity,
                request.data.venue_address,
                request.data.venue_city,
                request.data.venue_state,
                request.data.venue_zip,
                request.data.venue_country,
                request.data.table_count
            )
            if db_query(query, *values) is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: {err_msg}")

            return write_service_pb2.CreateEntityResponse(success=True, message="Venue created!")
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Exception during writing: {e}")
    
    def CreateOrganizer(self, request, context):
        print(f"Received data: {request.data}")
        try:
            # FUTURE: make website optional?
                # Question: do we need to know features about organizers? Yes, for DJ -> event matching
            query = """
            INSERT INTO organizer (
                name, first_name, last_name, email, phone, country, website
            ) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
            """

            values = (
                request.data.org_name,
                request.data.first_name,
                request.data.last_name,
                request.data.email,
                request.data.phone,
                request.data.country,
                request.data.website
            )
            if db_query(query, *values) is None:
                return write_service_pb2.CreateEntityResponse(success=False, message=f"DB Error: {err_msg}")

            return write_service_pb2.CreateEntityResponse(success=True, message="Organizer created!")
        except Exception as e:
            print(f"Exception during writing: {e}")
            return write_service_pb2.CreateEntityResponse(success=False, message=f"Exception during writing: {e}")
        
    def PublishEvent(self, request, context):
        print(f"Received data: {request.data}")
        try:

            query = """
                INSERT INTO published_events (event_id, event_poster, bio)
                VALUES (%s, %s, %s, %s) RETURNING event_id;
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
