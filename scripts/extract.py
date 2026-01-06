import requests
from google.transit import gtfs_realtime_pb2
import json
import os
from datetime import datetime
import load as ld

# The public URL (no key needed)
MTA_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

def extract_mta_data():
    print(f"[{datetime.now()}] Fetching live data from MTA...")
    
    try:
        # 1. Get the binary data
        response = requests.get(MTA_URL)
        response.raise_for_status() # Check for errors
        
        # 2. Parse the binary Protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        
        # 3. Create a list to store our cleaned data
        processed_data = []
        
        for entity in feed.entity:
            # We want 'Trip Updates' (Arrival/Departure times)
            if entity.HasField('trip_update'):
                trip = entity.trip_update
                
                # Each update can have multiple stop arrival predictions
                for stop_update in trip.stop_time_update:
                    processed_data.append({
                        "trip_id": trip.trip.trip_id,
                        "route_id": trip.trip.route_id,
                        "stop_id": stop_update.stop_id,
                        # Convert timestamp to human-readable or keep as int
                        "arrival_time": stop_update.arrival.time if stop_update.HasField('arrival') else None,
                        "departure_time": stop_update.departure.time if stop_update.HasField('departure') else None,
                        "fetched_at": str(datetime.now())
                    }) 
        
        print(f"Successfully extracted {len(processed_data)} arrival predictions.")
        
        # 4. Save locally as a 'Bronze' JSON file
        # This acts as our backup before moving to S3
        os.makedirs(r'G:/DE/spark_practice/transit_gtfs/data/bronze', exist_ok=True)
        filename = f"mta_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(f"G:/DE/spark_practice/transit_gtfs/data/bronze/{filename}", "w") as f:
            json.dump(processed_data, f, indent=4)

        ld.load_json_to_s3(f"G:/DE/spark_practice/transit_gtfs/data/bronze/{filename}", "transit-gfts", f"bronze/{filename}")    
            
        return filename

    except Exception as e:
        print(f"Extraction failed: {e}")
        return None



if __name__ == "__main__":
    extract_mta_data()