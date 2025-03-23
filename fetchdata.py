import requests
from pymongo import MongoClient
from datetime import datetime
from pymongo import UpdateOne
from datetime import datetime, timedelta

# MongoDB connection
try:
    client = MongoClient("mongodb://localhost:27017")
    client.server_info()  # Will raise an exception if connection fails
    print("Database connection successful!")
    db = client.eu_flight_system
    print(f"Connected to database: eu_flight_system")

    # AviationStack API configuration
    API_KEY = "abf0cd095c4117cf75b1c8f9b8a0d737"
    BASE_URL = "https://api.aviationstack.com/v1"

    def fetch_and_store_airports():
        print("\nFetching airports data...")
        params = {
            "access_key": API_KEY,
            "limit": 100,
            "country_code": "DE"  # Germany
        }
        
        response = requests.get(f"{BASE_URL}/airports", params=params)
        if response.status_code == 200:
            data = response.json().get("data", [])
            print(f"Found {len(data)} airports")
            
            operations = []
            for airport in data:
                airport_data = {
                    "name": airport.get("airport_name"),
                    "iata_code": airport.get("iata_code"),
                    "icao_code": airport.get("icao_code"),
                    "country": airport.get("country_name"),
                    "city": airport.get("city"),  # Changed from city_name to city
                    "latitude": float(airport.get("latitude", 0)),
                    "longitude": float(airport.get("longitude", 0)),
                    "timezone": airport.get("timezone")
                }
                
                # Remove any None values
                airport_data = {k: v for k, v in airport_data.items() if v is not None}
                
                if airport_data["iata_code"]:  # Only add if IATA code exists
                    operations.append(UpdateOne(
                        {"iata_code": airport_data["iata_code"]},
                        {"$set": airport_data},
                        upsert=True
                    ))
            
            if operations:
                result = db.airports.bulk_write(operations)
                print(f"Airports updated: {result.upserted_count}")
                print(f"Airports modified: {result.modified_count}")
            return True
        else:
            print(f"Error fetching airports: {response.status_code}")
            return False

    
    def fetch_and_store_airlines():
        print("\nFetching airlines data...")
        params = {
            "access_key": API_KEY,
            "limit": 50,
            "country_code": "DE"  # Germany
        }
        
        response = requests.get(f"{BASE_URL}/airlines", params=params)
        if response.status_code == 200:
            data = response.json().get("data", [])
            print(f"Found {len(data)} airlines")
            
            operations = []
            for airline in data:
                operations.append(UpdateOne(
                    {"iata_code": airline["iata_code"]},
                    {"$set": {
                        "name": airline["airline_name"],
                        "iata_code": airline["iata_code"],
                        "icao_code": airline["icao_code"],
                        "country": airline["country_name"],
                        "fleet_size": airline["fleet_size"],
                        "status": airline["status"]
                    }},
                    upsert=True
                ))
            
            if operations:
                result = db.airlines.bulk_write(operations)
                print(f"Airlines updated: {result.upserted_count}")
                print(f"Airlines modified: {result.modified_count}")
            return True
        else:
            print(f"Error fetching airlines: {response.status_code}")
            return False

    def fetch_and_store_flights():
        print("\nFetching flights data...")
        params = {
            "access_key": "abf0cd095c4117cf75b1c8f9b8a0d737",
            "limit": 100
        }
        
        response = requests.get(f"{BASE_URL}/flights", params=params)
        if response.status_code == 200:
            flights = response.json().get("data", [])
            print(f"Found {len(flights)} flights")
            
            operations = []
            delays = []
            for f in flights:
                try:
                    scheduled_departure = datetime.fromisoformat(f["departure"]["scheduled"].replace("Z", "+00:00"))
                    actual_departure = f["departure"].get("actual")
                    if actual_departure:
                        actual_departure = datetime.fromisoformat(actual_departure.replace("Z", "+00:00"))
                        delay = (actual_departure - scheduled_departure).total_seconds() / 60
                    else:
                        delay = 0

                    flight_data = {
                        "flight_number": f["flight"]["iata"],
                        "airline": f["airline"]["iata"],
                        "departure_airport": f["departure"]["iata"],
                        "arrival_airport": f["arrival"]["iata"],
                        "scheduled_departure": scheduled_departure,
                        "actual_departure": actual_departure,
                        "status": f["flight_status"],
                        "delay": delay,
                        "last_updated": datetime.utcnow()
                    }
                    operations.append(UpdateOne(
                        {"flight_number": flight_data["flight_number"]},
                        {"$set": flight_data},
                        upsert=True
                    ))

                    if delay > 120:  # More than 2 hours delay
                        delays.append(flight_data)

                except KeyError as e:
                    print(f"Skipping flight due to missing data: {e}")
                    continue
            
            if operations:
                result = db.flights.bulk_write(operations)
                print(f"Flights updated: {result.upserted_count}")
                print(f"Flights modified: {result.modified_count}")

            # Update delays collection
            if delays:
                delay_ops = [UpdateOne(
                    {"flight_number": d["flight_number"]},
                    {"$set": d},
                    upsert=True
                ) for d in delays]
                delay_result = db.delays.bulk_write(delay_ops)
                print(f"Delays updated: {delay_result.upserted_count}")
                print(f"Delays modified: {delay_result.modified_count}")
            else:
                print("No delays found")

            return True
        else:
            print(f"Error fetching flights: {response.status_code}")
            return False

    # Add this at the end of your script
    print(f"Total delays: {db.delays.count_documents({})}")



    def identify_and_store_delays():
        print("\nIdentifying delayed flights...")
        delayed_flights = db.flights.find({
            "actual_departure": {"$exists": True},
            "scheduled_departure": {"$exists": True}
        })
        
        operations = []
        for flight in delayed_flights:
            try:
                scheduled = datetime.fromisoformat(flight["scheduled_departure"].replace("Z", "+00:00"))
                actual = datetime.fromisoformat(flight["actual_departure"].replace("Z", "+00:00"))
                delay = (actual - scheduled).total_seconds() / 60  # Minutes
                
                if delay > 120:  # 2 hours
                    delay_data = {
                        "flight_number": flight["flight_number"],
                        "scheduled_departure": flight["scheduled_departure"],
                        "actual_departure": flight["actual_departure"],
                        "delay_minutes": delay,
                        "status": flight["status"],
                        "airline": flight["airline_iata"]
                    }
                    
                    operations.append(UpdateOne(
                        {"flight_number": delay_data["flight_number"]},
                        {"$set": delay_data},
                        upsert=True
                    ))
            except Exception as e:
                print(f"Error processing delay: {e}")
                continue
        
        if operations:
            result = db.delays.bulk_write(operations)
            print(f"Delays identified: {len(operations)}")
            print(f"Delays updated: {result.upserted_count}")
            print(f"Delays modified: {result.modified_count}")

    # Main execution flow
    if fetch_and_store_airports():
        print("Airports collection populated successfully!")
    
    if fetch_and_store_airlines():
        print("Airlines collection populated successfully!")
    
    if fetch_and_store_flights():
        print("Flights collection populated successfully!")
    
    print("\nDatabase population complete!")
    print(f"Total airports: {db.airports.count_documents({})}")
    print(f"Total airlines: {db.airlines.count_documents({})}")
    print(f"Total flights: {db.flights.count_documents({})}")
    print(f"Total delays: {db.delays.count_documents({})}")

except Exception as e:
    print(f"Database connection or other error: {e}")
