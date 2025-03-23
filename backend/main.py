from fastapi import FastAPI, BackgroundTasks
from pymongo import MongoClient, GEOSPHERE, TEXT
from datetime import datetime, timedelta
import requests
import os
import asyncio
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional

load_dotenv()

app = FastAPI()

# MongoDB Configuration
client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
db = client.eu_flight_system

class Airport(BaseModel):
    iata_code: str
    name: str
    city: str
    country: str
    latitude: float
    longitude: float
    timezone: str

class Flight(BaseModel):
    flight_number: str
    airline: str
    departure_airport: str
    arrival_airport: str
    scheduled_departure: datetime
    actual_departure: Optional[datetime]
    status: str
    delay_minutes: Optional[int]

@app.on_event("startup")
async def startup_db():
    # Clean up existing indexes first
    try:
        db.flights.drop_index("departure_airport_text")
    except Exception as e:
        print(f"No departure_airport index to remove: {e}")
    
    try:
        db.flights.drop_index("status_text")
    except Exception as e:
        print(f"No status_text index to remove: {e}")

    # Create standard indexes
    db.airports.create_index([("iata_code", 1)], unique=True)
    db.airlines.create_index([("iata_code", 1)], unique=True)
    db.flights.create_index([("flight_number", 1)], unique=True)
    
    # Create regular indexes for query optimization
    db.flights.create_index([("departure_airport", 1)])
    db.flights.create_index([("status", 1)])
    db.flights.create_index([("scheduled_departure", 1)])


async def data_update_scheduler():
    while True:
        await update_flight_data()
        await calculate_delays()
        await asyncio.sleep(300)  # Update every 5 minutes

async def update_flight_data():
    try:
        response = requests.get(
            "https://api.aviationstack.com/v1/flights",
            params={
                "access_key": os.getenv("AVIATIONSTACK_KEY"),
                "dep_iata": "FRA,MUC,BER,HAM,DUS",
                "limit": 100
            }
        )
        
        if response.status_code == 200:
            flights = response.json().get("data", [])
            bulk_ops = []
            
            for flight in flights:
                try:
                    flight_data = {
                        "flight_number": flight["flight"]["iata"],
                        "airline": flight["airline"]["name"],
                        "departure_airport": flight["departure"]["iata"],
                        "arrival_airport": flight["arrival"]["iata"],
                        "scheduled_departure": datetime.fromisoformat(
                            flight["departure"]["scheduled"].replace("Z", "+00:00")
                        ),
                        "actual_departure": datetime.fromisoformat(
                            flight["departure"]["actual"].replace("Z", "+00:00")
                        ) if flight["departure"].get("actual") else None,
                        "status": flight["flight_status"]
                    }
                    
                    bulk_ops.append({
                        "updateOne": {
                            "filter": {"flight_number": flight_data["flight_number"]},
                            "update": {"$set": flight_data},
                            "upsert": True
                        }
                    })
                except KeyError:
                    continue

            if bulk_ops:
                db.flights.bulk_write(bulk_ops)
                
    except Exception as e:
        print(f"Error updating flights: {e}")

async def calculate_delays():
    try:
        db.flights.update_many(
            {"actual_departure": {"$exists": True}},
            [{"$set": {
                "delay_minutes": {
                    "$divide": [
                        {"$subtract": ["$actual_departure", "$scheduled_departure"]},
                        60000  # Convert milliseconds to minutes
                    ]
                }
            }}]
        )
    except Exception as e:
        print(f"Error calculating delays: {e}")

@app.get("/flights")
async def get_flights(departure: str = None, min_delay: int = None):
    query = {}
    if departure:
        query["departure_airport"] = departure
    if min_delay:
        query["delay_minutes"] = {"$gte": min_delay}
    
    return list(db.flights.find(query, {"_id": 0}))

@app.get("/airports")
async def get_airports():
    return list(db.airports.find({}, {"_id": 0}))

@app.get("/delay-stats")
async def get_delay_stats():
    pipeline = [
        {"$group": {
            "_id": None,
            "average_delay": {"$avg": "$delay_minutes"},
            "max_delay": {"$max": "$delay_minutes"},
            "delayed_flights": {
                "$sum": {"$cond": [{"$gte": ["$delay_minutes", 120]}, 1, 0]}
            }
        }}
    ]
    return list(db.flights.aggregate(pipeline))[0]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
