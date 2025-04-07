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
MONGODB_URI = os.getenv("MONGODB_URI")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LOGFLARE_API_KEY = os.getenv("LOGFLARE_API_KEY")
LOGFLARE_SOURCE = os.getenv("LOGFLARE_SOURCE")


# Validate required envs
if not MONGODB_URI or not OPENAI_API_KEY or not LOGFLARE_API_KEY or not LOGFLARE_SOURCE:
    raise Exception("Missing one or more required environment variables.")

# Construct Logflare URL
LOGFLARE_API_URL = f"https://api.logflare.app/logs/json?source={LOGFLARE_SOURCE}"

# --- Candidate Data Extraction ---
def extract_candidate_data(xxo):
    client = OpenAI(api_key=OPENAI_API_KEY)
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
        text={"format": {"type": "text"}},
        reasoning={},
        tools=[],
        temperature=1,
        max_output_tokens=2048,
        top_p=1,
        store=True
    )
    x = response.output[0].content[0].text
    clean_text = x.strip("```json\n").strip("```").strip()
    return json.loads(clean_text)

# --- MongoDB and Flask Setup ---
app = Flask(__name__)
client = MongoClient(MONGODB_URI)
db = client['interviewSupport']
taskBody_collection = db['taskBody']
repliesBody_collection = db['repliesBody']

# --- Logflare Logging Function ---
def log_to_logflare(entry):
    """
    Sends a structured log entry to Logflare with metadata.
    """
    payload = {
        "log_entry": entry,
        "metadata": {
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    }
    try:
        headers = {
            'Content-Type': 'application/json',
            'X-API-KEY': LOGFLARE_API_KEY
        }
        response = requests.post(LOGFLARE_API_URL, json=payload, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Logflare error: {e}")

# --- API Endpoint ---
@app.route('/process', methods=['POST'])
def process_data():
    data = request.get_json()
    
    # If data is a dict (single item), wrap it in a list.
    if isinstance(data, dict):
        data = [data]
    
    if not data:
        log_to_logflare({
            "log_type": "error",
            "message": "No data provided to /process endpoint"
        })
        return jsonify({"error": "No data provided"}), 400

    for item in data:
        subject = item.get('subject', '').strip()
        # Create a case-insensitive regular expression for the subject
        regex_query = {'$regex': f'^{re.escape(subject)}$', '$options': 'i'}
        timestamp = item.get('receivedDateTime', datetime.utcnow().isoformat())
        reference = f"{subject} | {timestamp}"
        
        # Check if the subject already exists in taskBody or repliesBody using case-insensitive search
        if (taskBody_collection.find_one({'subject': regex_query}) or
            repliesBody_collection.find_one({'subject': regex_query})):
            msg = f"Duplicate subject found. Skipping item with subject: '{subject}'"
            print(msg)
            log_to_logflare({
                "log_type": "skip",
                "reference": reference,
                "subject": subject,
                "message": msg
            })
            continue

        try:
            candidate_data = extract_candidate_data(item.get('body', ''))
        except Exception as e:
            error_msg = f"Extraction failed for subject '{subject}': {e}"
            print(error_msg)
            log_to_logflare({
                "log_type": "error",
                "reference": reference,
                "subject": subject,
                "error": str(e)
            })
            continue

        final_data = {**item, **candidate_data}
        collection_type = 'taskBody' if 'interview support' in subject.lower() and not subject.lower().startswith('re:') else 'repliesBody'

        try:
            if collection_type == 'taskBody':
                taskBody_collection.insert_one(final_data)
            else:
                repliesBody_collection.insert_one(final_data)

            print(f"Inserted item with subject '{subject}' into {collection_type}")
            log_to_logflare({
                "log_type": "info",
                "reference": reference,
                "subject": subject,
                "collection": collection_type,
                "message": "Item successfully processed and stored."
            })

        except Exception as e:
            error_msg = f"Insertion failed for subject '{subject}' in {collection_type}: {e}"
            print(error_msg)
            log_to_logflare({
                "log_type": "error",
                "reference": reference,
                "subject": subject,
                "collection": collection_type,
                "error": str(e)
            })

    return jsonify({"status": "complete"}), 200

if __name__ == '__main__':
    app.run(debug=True)
