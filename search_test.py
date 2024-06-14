from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Set the path to the Google Cloud credentials JSON file
credentials_path = "C:\\Users\\jyoti\\Downloads\\peak-dominion-426019-g8-8cf48bbab216.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Verify if the path is correct
if not os.path.exists(credentials_path):
    logger.error(f"Credentials file not found at: {credentials_path}")
    raise HTTPException(status_code=500, detail="Credentials file not found or not specified")
else:
    logger.info(f"Credentials file found at: {credentials_path}")

# Initialize BigQuery client
try:
    client = bigquery.Client()
    logger.info("BigQuery client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize BigQuery client: {e}")
    raise HTTPException(status_code=500, detail="Error initializing BigQuery client")

# Project and dataset information
PROJECT_ID = "peak-dominion-426019-g8"
DATASET_ID = "Problematic_Content"
TABLE_ID = "product_category"

# Construct the dataset string
dataset_str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
logger.info(f"Dataset string: {dataset_str}")

@app.get("/search_rude")
def search_rude():
    try:
        query = f"""
            SELECT *
            FROM `{dataset_str}`
        """
        logger.info(f"Running query: {query}")
        query_job = client.query(query)  # API request
        results = query_job.result()  # Waits for job to complete.

        
        return results
    except Exception as e:
        # Log the error with detailed information
        logger.error(f"Error executing query: {e}", exc_info=True)
        # Raise an HTTPException with status code 500
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/")
def read_root():
    return {"Hello": "World"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
