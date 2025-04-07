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

# --- Candidate Data Extraction ---
def extract_candidate_data(xxo):
    """
    Uses OpenAI to extract candidate details from the provided text.
    The prompt instructs the model to extract entities (like candidate name, DOB, gender, etc.)
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
                        "text": """From this I want you to extract entities as following and return in JSON
Candidate Name Exact Name Word To Word But Capitalized
Date Of Birth: DD/MM
Gender:
Education:
University:
Total Experience: (in int)
State: (Abbr)
Technology:
End Client:
Interview Round:
Job Title:
Email ID:
Contact No:
Date of Interview:(MM/DD/YYYY Consider the Day as well, match it with Date for upcoming 2-3 weeks current date is march 28, 2025)
Start Time Of Interview: (IN EASTERN TIME ZONE CONVERTED 12hrs AM/PM )
End Time Of Interview: (IN EASTERN TIME ZONE COVNERTED 12hrs AM/PM) If NOt available add duration into Start time"""
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
    # Debug prints can help during development (you can remove or comment them out later)
    x = response.output[0].content[0].text
    print("Raw OpenAI response:", x)
    clean_text = x.strip("```json\n").strip("```").strip()
    print("Cleaned JSON response:", clean_text)
    # Convert JSON string to Python dictionary
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
    This function uses the requests library to post the data.
    """
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

# --- API Endpoint ---
@app.route('/process', methods=['POST'])
def process_data():
    """
    Processes posted JSON data. For each item:
      - Checks if the subject already exists in MongoDB.
      - If not, extracts candidate data via OpenAI and merges it with the item.
      - Depending on the subject, inserts the record into either the taskBody or repliesBody collection.
      - Logs each processed item to Logflare.
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    for item in data:
        # Normalize the subject for case-insensitive comparison.
        subject = item.get('subject', '').strip().lower()
        
        # Check if the subject already exists in either collection.
        exists_in_task = taskBody_collection.find_one({'subject': subject})
        exists_in_replies = repliesBody_collection.find_one({'subject': subject})
        if exists_in_task or exists_in_replies:
            print(f"Subject '{subject}' already processed. Skipping...")
            continue

        try:
            # Extract candidate data from the item's body
            candidate_data = extract_candidate_data(item.get('body', ''))
        except Exception as e:
            print(f"Error extracting candidate data for item with id {item.get('id')}: {e}")
            continue

        # Merge the original item with the extracted candidate data.
        final_data = {**item, **candidate_data}

        # Decide which MongoDB collection to use based on subject content.
        if 'interview support' in subject and not subject.startswith('re:'):
            try:
                taskBody_collection.insert_one(final_data)
                print(f"Inserted item with id {item.get('id')} into taskBody.")
            except Exception as e:
                print(f"Error inserting item into taskBody with id {item.get('id')}: {e}")
        else:
            try:
                repliesBody_collection.insert_one(final_data)
                print(f"Inserted item with id {item.get('id')} into repliesBody.")
            except Exception as e:
                print(f"Error inserting item into repliesBody with id {item.get('id')}: {e}")

        # Log the processed item to Logflare.
        log_to_logflare(final_data)

    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    # Running the Flask app in debug mode for detailed error output (for development purposes)
    app.run(debug=True)
