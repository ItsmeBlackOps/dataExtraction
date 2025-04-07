import re
import ast
import os
import json
import requests
from groq import Groq
from openai import OpenAI
from pymongo import MongoClient
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Environment variables
MOCK_API_URL = os.getenv("MOCK_API_URL")  # Currently not used since data is POSTed
MONGODB_URI = os.getenv("MONGODB_URI")
if not MONGODB_URI:
    raise Exception("Missing MONGODB_URI in environment variables.")

# Logflare configuration
LOGFLARE_API_URL = "https://api.logflare.app/logs/json?source=bae2ec8c-0bf7-4561-96b1-c3f13dc3beb5"
LOGFLARE_API_KEY = "kuvw1feGD8Yw"

# --- Candidate Data Extraction using OpenAI ---
def extract_candidate_data(xxo):
    """
    Uses OpenAI to extract candidate details from the provided text.
    The prompt instructs the model to extract entities (like candidate name, DOB, etc.)
    and return them as JSON.
    """
    client = OpenAI()
    response = client.responses.create(
        model="gpt-4o",
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "From this I want you to extract entities as following and return in JSON \n"
                            "Candidate Name Exact Name Word To Word But Capitalized\n"
                            "Date Of Birth: DD/MM\n"
                            "Gender:\n"
                            "Education:\n"
                            "University:\n"
                            "Total Experience: (in int)\n"
                            "State: (Abbr)\n"
                            "Technology:\n"
                            "End Client:\n"
                            "Interview Round:\n"
                            "Job Title:\n"
                            "Email ID:\n"
                            "Contact No:\n"
                            "Date of Interview:(MM/DD/YYYY)\n"
                            "Start Time Of Interview: (IN EASTERN TIME ZONE CONVERTED 12hrs AM/PM)\n"
                            "End Time Of Interview: (IN EASTERN TIME ZONE COVNERTED 12hrs AM/PM) If NOt available add duration into Start time\n"
                        )
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": xxo
                    }
                ]
            }
        ],
        text={
            "format": {
                "type": "text"
            }
        },
        reasoning={},
        tools=[],
        temperature=1,
        max_output_tokens=2048,
        top_p=1,
        store=True
    )
    x = response.output[0].content[0].text
    print("Raw OpenAI response:", x)
    clean_text = x.strip("```json\n").strip("```").strip()
    print("Cleaned JSON response:", clean_text)
    data = json.loads(clean_text)
    print("Extracted candidate data:", data)
    return data

# --- MongoDB and Flask Setup ---
app = Flask(__name__)

# Establish MongoDB connection
client = MongoClient(MONGODB_URI)
db = client['interviewSupport']
taskBody_collection = db['taskBody']
repliesBody_collection = db['repliesBody']

# --- Logflare Logging Function ---
def log_to_logflare(log_data):
    """
    Sends the provided log_data as JSON to Logflare.
    Extra metadata (timestamp and collection type) should already be added to log_data.
    """
    # Ensure the log entry has a timestamp
    if 'log_timestamp' not in log_data:
        log_data['log_timestamp'] = datetime.utcnow().isoformat() + "Z"
    try:
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'X-API-KEY': LOGFLARE_API_KEY
        }
        response = requests.post(LOGFLARE_API_URL, json=log_data, headers=headers)
        response.raise_for_status()
        print("Successfully logged to Logflare.")
    except Exception as e:
        print("Error logging to Logflare:", e)
        raise e

# --- API Endpoint ---
@app.route('/process', methods=['POST'])
def process_data():
    """
    Processes posted JSON data. For each item:
      - Checks if the subject already exists in MongoDB.
      - If not, extracts candidate data via OpenAI and merges it with the item.
      - Depending on the subject, inserts the record into either the taskBody or repliesBody collection.
      - Logs each processed item to Logflare with extra metadata (timestamp and collection type).
      - Aggregates detailed results for each item, providing sophisticated error responses.
    """
    results = []
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    for item in data:
        item_id = item.get('id', 'unknown')
        subject = item.get('subject', '').strip().lower()

        # Check if the subject already exists in either collection.
        exists_in_task = taskBody_collection.find_one({'subject': subject})
        exists_in_replies = repliesBody_collection.find_one({'subject': subject})
        if exists_in_task or exists_in_replies:
            msg = f"Subject '{subject}' already processed. Skipping item with id {item_id}."
            print(msg)
            results.append({'id': item_id, 'status': 'skipped', 'message': msg})
            continue

        try:
            # Extract candidate data using OpenAI
            candidate_data = extract_candidate_data(item.get('body', ''))
        except Exception as e:
            error_msg = f"Extraction error for item with id {item_id}: {str(e)}"
            print(error_msg)
            results.append({'id': item_id, 'status': 'error', 'message': error_msg})
            continue

        final_data = {**item, **candidate_data}
        collection_type = ''

        try:
            # Insert into the appropriate collection based on subject content.
            if 'interview support' in subject and not subject.startswith('re:'):
                collection_type = 'taskBody'
                taskBody_collection.insert_one(final_data)
                print(f"Inserted item with id {item_id} into taskBody.")
            else:
                collection_type = 'repliesBody'
                repliesBody_collection.insert_one(final_data)
                print(f"Inserted item with id {item_id} into repliesBody.")
            results.append({'id': item_id, 'status': 'success', 'collection': collection_type})
        except Exception as e:
            error_msg = f"Insertion error for item with id {item_id} into {collection_type}: {str(e)}"
            print(error_msg)
            results.append({'id': item_id, 'status': 'error', 'message': error_msg})
            continue

        # Add additional metadata before logging.
        final_data['log_timestamp'] = datetime.utcnow().isoformat() + "Z"
        final_data['collection_type'] = collection_type

        try:
            log_to_logflare(final_data)
        except Exception as e:
            error_msg = f"Logging error for item with id {item_id}: {str(e)}"
            print(error_msg)
            results.append({'id': item_id, 'status': 'warning', 'message': error_msg})

    overall_status = "completed"
    return jsonify({"status": overall_status, "results": results}), 200

if __name__ == '__main__':
    # Run the app in debug mode for detailed error output.
    app.run(debug=True)
