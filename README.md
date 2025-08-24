# Lean ETL (MongoDB → PostgreSQL)

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline in Python.
It extracts documents from MongoDB, transforms them into tabular form using Pandas, and loads them into PostgreSQL.

---

## Prerequisites

- Python 3.9+
- MongoDB (local or Atlas)
- PostgreSQL (local or remote)
- pip

Clone this repository and install dependencies:

git clone <your-repo-url>
cd <your-repo>
pip install -r requirements.txt

---

## Database Setup

### 1. MongoDB Setup

Start MongoDB locally or connect to MongoDB Atlas.

Create the database and collection:

use ride_share
db.createCollection("drivers")

Insert sample data: (use AI tools to create the full dataset)

{
  "driver_id": "d-2345678",
  "first_name": "John",
  "last_name": "Smith",
  "vehicle_info": {
    "make": "Honda",
    "model": "CR-V",
    "year": 2023,
    "license_plate": "ABC-123"
  },
  "driver_rating": 4.91,
  "total_rides_completed": 954,
  "rides": [
    {
      "ride_id": "r-321098765",
      "pickup_location": {"latitude": 40.7128, "longitude": -74.006, "address": "Wall Street, New York, NY"},
      "dropoff_location": {"latitude": 40.758, "longitude": -73.9855, "address": "Times Square, New York, NY"},
      "miles_driven": 2.5,
      "fare_amount": 15.2,
      "ride_category": "UberX"
    },
    {
      "ride_id": "r-654321098",
      "pickup_location": {"latitude": 40.758, "longitude": -73.9855, "address": "Times Square, New York, NY"},
      "dropoff_location": {"latitude": 40.7128, "longitude": -74.006, "address": "Wall Street, New York, NY"},
      "miles_driven": 2.8,
      "fare_amount": 16.5,
      "ride_category": "UberXL"
    }
  ]
}

---

### 2. PostgreSQL Setup

Connect as postgres user:

psql -U postgres

Create database and tables:

CREATE DATABASE ride_share;

\c ride_share;

CREATE TABLE driver_data (
    driver_uuid TEXT PRIMARY KEY,
    driver_id TEXT,
    first_name TEXT,
    last_name TEXT,
    driver_rating NUMERIC,
    total_rides_completed INT
);

CREATE TABLE vehicle_data (
    driver_uuid TEXT REFERENCES driver_data(driver_uuid),
    vehicle
