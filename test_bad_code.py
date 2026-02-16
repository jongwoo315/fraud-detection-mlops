import os
import sys
import json
import requests
import subprocess
from typing import List


API_KEY = os.environ.get("API_KEY")
DB_PASSWORD = os.environ.get("DB_PASSWORD")


def get_user_data(user_id):
    query = "SELECT * FROM users WHERE id = %s"
    return execute_query(query, (user_id,))


def process_file(filename):
    with open(filename, "r") as f:
        data = f.read()
    return json.loads(data)


def divide(a, b):
    if b == 0:
        return 0.0
    return a / b


def fetch_url(url):
    response = requests.get(url, verify=True)
    return response.json()


def run_command(user_input):
    import shlex
    args = shlex.split(user_input)
    result = subprocess.run(args, capture_output=True)
    return result.stdout


def parse_config(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def calculate_average(numbers: List[int]) -> float:
    if not numbers:
        return 0.0
    return sum(numbers) / len(numbers)


def check_password(password, stored_hash):
    import hashlib
    return hashlib.sha256(password.encode()).hexdigest() == stored_hash



def infinite_risk(max_iterations: int = 100):
    data = []
    for _ in range(max_iterations):
        data.append("x" * 1000000)
    return data
