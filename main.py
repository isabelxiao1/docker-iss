import requests
import logging
import os
from pymongo import MongoClient

"""
    This script fetches artist data from the MusicBrainz API
    and writes it to a MongoDB database.

    YOU MUST update two things before running this script:
    1. Populate a MONGOPASS environment variable with your MongoDB password.
    2. Update the db name to your UVA computing ID on line 60.
"""

# logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# core fcn
def get_musicbrainz_artists():
    url = "https://musicbrainz.org/ws/2/artist/?query=nirvana&fmt=json"
    headers = {
    'User-Agent': 'docker-iss/1.0 (jcs3qy@virginia.edu)'
    }
    try:
        response = requests.get(url, headers=headers)
        r = response.json()

        artists = r.get("artists", [])

        if not artists:
            logger.warning("No artist data found.")
            return

        for artist in artists:
            name = artist.get("name")
            country = artist.get("country", "N/A")
            disambiguation = artist.get("disambiguation", "N/A")

            logger.info(f"Artist: {name} | Country: {country} | Disambiguation: {disambiguation}")
            write_to_mongo(name, country, disambiguation)

    except Exception as e:
        logger.error(f"Failed to fetch data from MusicBrainz API: {e}")
        exit(1)

# db utility fcn
def write_to_mongo(name, country, disambiguation):
    try:
        dbpass = os.getenv('MONGOPASS')
        if not dbpass:
            raise ValueError("MONGOPASS environment variable is not set")

        connection_string = f'mongodb+srv://docker:{dbpass}@cluster0.m3fek.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
        client = MongoClient(connection_string)

        # use your UVA computing ID for the database name
        db = client['jcs3qy']
        collection = db['music_artists']
        collection.insert_one({'name': name, 'country': country, 'disambiguation': disambiguation})
        logger.info(f"Inserted {name} into MongoDB.")

    except Exception as e:
        logger.error(f"MongoDB write error: {e}")
        exit(1)

# entrypoint fcn
if __name__ == "__main__":
    get_musicbrainz_artists()
