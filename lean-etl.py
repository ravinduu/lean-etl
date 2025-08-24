import json
import logging
import os
import pandas as pd
import sys
import uuid
import psycopg2
import gc
import time
import psutil

from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import execute_batch
from pymongo import MongoClient
from bson import json_util
from typing import List, Tuple, Generator, Dict

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

CPU_THRESHOLD = 80.0
RAM_THRESHOLD = 1024

MONGO_BATCH_SIZE = 200
PROCESS_BATCH_SIZE = 50
POSTGRES_BATCH_SIZE = 500
RESOURCE_CHECK_INTERVAL = 5

INSERT_DRIVER_DATA_QUERY = """
INSERT INTO public.driver_data (
    driver_uuid,
    driver_id,
    first_name,
    last_name,
    driver_rating,
    total_rides_completed
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (driver_uuid) DO NOTHING;
"""

INSERT_VEHICLE_DATA_QUERY = """
INSERT INTO public.vehicle_data (
    driver_uuid,
    vehicle_make,
    vehicle_model,
    vehicle_year,
    vehicle_license_plate
) VALUES (%s, %s, %s, %s, %s);
"""

INSERT_RIDES_DATA_QUERY = """
INSERT INTO public.rides_data (
    ride_id,
    driver_uuid,
    pickup_latitude,
    pickup_longitude,
    pickup_address,
    dropoff_latitude,
    dropoff_longitude,
    dropoff_address,
    miles_driven,
    fare_amount,
    ride_category
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

class LeanEtl:
    def __init__(self, name: str = "LeanEtl"):
        self.name = name
        self.mongo_client = None
        self.pg_pool = None
        self.pg_conn = None
        self.batch_count = 0
        self.peak_cpu_usage = 0.0
        self.peak_ram_usage = 0.0

    def log_resource_usage(self, context: str = ""):
        process = psutil.Process()
        mem_info = process.memory_info()
        rss_mb = mem_info.rss / (1024 * 1024)
        cpu_percent = psutil.cpu_percent(interval=1)

        if rss_mb > self.peak_ram_usage:
            self.peak_ram_usage = rss_mb
        if cpu_percent > self.peak_cpu_usage:
            self.peak_cpu_usage = cpu_percent

        logging.info(
            f"[Resource Usage] {context} | "
            f"Memory Usage: {rss_mb:.2f} MB | "
            f"CPU Usage: {cpu_percent:.2f}% | "
            f"Peak RAM: {self.peak_ram_usage:.2f} MB | "
            f"Peak CPU: {self.peak_cpu_usage:.2f}%"
        )

        if rss_mb > RAM_THRESHOLD or cpu_percent > CPU_THRESHOLD:
            logging.warning(
                f"[Resource Usage Warning] {context} | "
                f"Memory RSS: {rss_mb:.2f} MB | CPU Usage: {cpu_percent:.2f}% "
                f"exceeded thresholds (RAM: {RAM_THRESHOLD} MB, CPU: {CPU_THRESHOLD}%)"
            )
            self.trigger_gc()

    @staticmethod
    def trigger_gc():
        gc.collect()
        logging.info("Garbage collection triggered.")
        time.sleep(1)

    @contextmanager
    def mongo_connection(self, db_url: str):
        try:
            if not self.mongo_client:
                self.mongo_client = MongoClient(
                    db_url,
                    minPoolSize=0,
                    maxPoolSize=5,
                    connectTimeoutMS=10000,
                    socketTimeoutMS=10000
                )
                self.mongo_client.admin.command('ping')
                logging.info("MongoDB connection established successfully")

            yield self.mongo_client

        except Exception as e:
            logging.error(f"MongoDB connection failed: {e}")
            yield None

        finally:
            if self.mongo_client:
                self.mongo_client.close()
                logging.info("MongoDB connection closed")
                self.mongo_client = None

    @contextmanager
    def postgres_connection(self, dbname: str, user: str, password: str, host: str, port: int):
        conn = None
        try:
            if not self.pg_pool:
                self.pg_pool = pool.ThreadedConnectionPool(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port,
                    minconn=1,
                    maxconn=5,
                    connect_timeout=10
                )
                logging.info("PostgreSQL connection pool created successfully")

            self.pg_conn = self.pg_pool.getconn()
            yield self.pg_conn

        except Exception as e:
            logging.error(f"PostgreSQL connection failed: {e}")
            yield None

        finally:
            if self.pg_conn:
                self.pg_pool.putconn(self.pg_conn)
            if self.pg_pool:
                self.pg_pool.closeall()
                logging.info("PostgreSQL connection pool closed")
                self.pg_pool = None

    def extract_data(self, collection) -> Generator[List[Dict], None, None]:
        try:
            logging.info("Fetch documents from the MongoDB collection in batches.")

            cursor = collection.find({}).batch_size(MONGO_BATCH_SIZE)
            batch = []

            for doc in cursor:
                self.batch_count += 1
                batch.append(doc)

                if len(batch) >= PROCESS_BATCH_SIZE:
                    try:
                        json_batch = json.loads(json_util.dumps(batch))
                        yield json_batch

                    except Exception as e:
                        logging.error(f"Error serializing batch: {str(e)}")
                        continue
                
                    batch.clear()
                    self.trigger_gc()

                    if self.batch_count % RESOURCE_CHECK_INTERVAL == 0:
                        self.log_resource_usage(f"Batch {self.batch_count}")
                        time.sleep(0.05)

            if batch:
                self.batch_count += 1
                try:
                    json_batch = json.loads(json_util.dumps(batch))
                    yield json_batch
                except Exception as e:
                    logging.error(f"Error serializing batch: {str(e)}")
                batch.clear()

            cursor.close()
            logging.info("Succesfully processed the final batche.")

        except Exception as e:
            logging.error(f"Error during batch fetching: {str(e)}")
    
    def transform_data(self, data_batch: List[Dict]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        try:
            if not data_batch:
                return pd.DataFrame(), pd.DataFrame()
            self.log_resource_usage("process_data")
            
            rides_data = pd.json_normalize(
                data=data_batch,
                record_path=['rides'],
                meta=[
                    'driver_id',
                    'first_name',
                    'last_name',
                    'driver_rating',
                    'total_rides_completed',
                    ['vehicle_info', 'make'],
                    ['vehicle_info', 'model'],
                    ['vehicle_info', 'year'],
                    ['vehicle_info', 'license_plate'],
                    ['_id', '$oid']
                ],
                errors='ignore'
            )

            column_rename_map = {
                "pickup_location.latitude": "pickup_latitude",
                "pickup_location.longitude": "pickup_longitude",
                "pickup_location.address": "pickup_address",
                "dropoff_location.latitude": "dropoff_latitude",
                "dropoff_location.longitude": "dropoff_longitude",
                "dropoff_location.address": "dropoff_address",
                "vehicle_info.make": "vehicle_make",
                "vehicle_info.model": "vehicle_model",
                "vehicle_info.year": "vehicle_year",
                "vehicle_info.license_plate": "vehicle_license_plate",
                "_id.$oid": "driver_uuid"
            }
            
            rides_data = rides_data.rename(columns=column_rename_map)

            driver_data = rides_data[[
                "driver_uuid", 
                "driver_id",
                "first_name",
                "last_name",
                "driver_rating",
                "total_rides_completed"
            ]].drop_duplicates()

            vehicle_data = rides_data[[
                "driver_uuid", 
                "vehicle_make", 
                "vehicle_model", 
                "vehicle_year", 
                "vehicle_license_plate"
            ]].drop_duplicates()

            rides_data = rides_data[[
                "ride_id",
                "driver_uuid",
                "pickup_latitude",
                "pickup_longitude",
                "pickup_address",
                "dropoff_latitude",
                "dropoff_longitude",
                "dropoff_address",
                "miles_driven",
                "fare_amount",
                "ride_category"
            ]]

            # Add you data transformation/processing logic here. 

            self.log_resource_usage("process_data")
            self.trigger_gc()
            return driver_data, vehicle_data, rides_data

        except Exception as e:
            logging.error(f"Error during batch processing: {str(e)}")

    def load_data(self, cursor, driver_data: List[Tuple], vehicle_data: List[Tuple], rides_data: List[Tuple]) -> bool:
        try:
            cursor.execute("savepoint load_data")
            logging.info("Start loading data.")

            if not driver_data.empty and not vehicle_data.empty and not rides_data.empty:
                driver_data_tuples = list(driver_data.itertuples(index=False, name=None))
                vehicle_data_tuples = list(vehicle_data.itertuples(index=False, name=None))
                rides_data_tuples = list(rides_data.itertuples(index=False, name=None))

                driver_data_success = self.load_data_batch(cursor, INSERT_DRIVER_DATA_QUERY, driver_data_tuples)
                vehicle_data_success = self.load_data_batch(cursor, INSERT_VEHICLE_DATA_QUERY, vehicle_data_tuples)
                rides_data_success = self.load_data_batch(cursor, INSERT_RIDES_DATA_QUERY, rides_data_tuples)

                if not driver_data_success or not vehicle_data_success or not rides_data_success:
                    cursor.execute("rollback to savepoint load_data")
                    logging.error("Batch loading fail.")
                    return False

            del driver_data, vehicle_data, rides_data
            self.trigger_gc()
            logging.info("Batch loading successfull.")
            
            return True

        except Exception as e:
            logging.error(f"Error during batch loading: {str(e)}")
            cursor.execute("rollback to savepoint load_data")
            sys.exit(1)

    def load_data_batch(self, cursor, query: str, data: List[Tuple]) -> bool:
        try:
            for i in range(0, len(data), POSTGRES_BATCH_SIZE):
                batch = data[i:i+POSTGRES_BATCH_SIZE]
                execute_batch(cursor, query, batch, page_size=POSTGRES_BATCH_SIZE)
                del batch
            
            del data
            self.trigger_gc()

            return True
        
        except Exception as e:
            logging.error(f"Error during batch loading: {str(e)}")
            return False

    @staticmethod
    def main():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        etl = LeanEtl()

        etl.log_resource_usage("ETL Start")

        mongo_config = {
            "DB_URL": os.getenv("DB_URL"),
            "DB_NAME": os.getenv("DB_NAME"),
            "COLLECTION_NAME": os.getenv("COLLECTION_NAME"),
        }

        postgres_config = {
            "USER": os.getenv("USER"),
            "PASSWORD": os.getenv("PASSWORD"),
            "HOST": os.getenv("HOST"),
            "PG_DB_NAME": os.getenv("PG_DB_NAME"),
            "PORT": os.getenv("PORT"),
        }

        etl_success = False

        with etl.mongo_connection(mongo_config["DB_URL"]) as mongo_client:
            db = mongo_client[mongo_config["DB_NAME"]]
            collection = db[mongo_config["COLLECTION_NAME"]]

            with etl.postgres_connection(postgres_config["PG_DB_NAME"], postgres_config["USER"], postgres_config["PASSWORD"], postgres_config["HOST"], postgres_config["PORT"]) as pg_conn:
                cursor = pg_conn.cursor()

                logging.info("ETL - Start data extracting")

                for batch_data in etl.extract_data(collection):
                    if not batch_data:
                        continue
                    
                    driver_data, vehicle_data, rides_data = etl.transform_data(batch_data)

                    pd.set_option('display.max_columns', None)

                    etl_success = etl.load_data(cursor, driver_data, vehicle_data, rides_data)

                    if etl_success:
                        etl.pg_conn.commit()
                        logging.info("Commiting changes")

if __name__ == "__main__":
    LeanEtl.main()
    
    