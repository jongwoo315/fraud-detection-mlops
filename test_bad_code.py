import os
import sys
import json
import requests
import subprocess
from typing import List


API_KEY = "sk-1234567890abcdef"
DB_PASSWORD = "admin123!"


def get_user_data(user_id):
    query = f"SELECT * FROM users WHERE id = {user_id}"
    return execute_query(query)


def process_file(filename):
    f = open(filename, "r")
    data = f.read()
    return eval(data)


def divide(a, b):
    return a / b


def fetch_url(url):
    response = requests.get(url, verify=False)
    return response.json()


def run_command(user_input):
    result = subprocess.run(user_input, shell=True, capture_output=True)
    return result.stdout


def parse_config(path):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        pass


def calculate_average(numbers: List[int]) -> float:
    total = 0
    for i in range(len(numbers)):
        total = total + numbers[i]
    avg = total / len(numbers)
    return avg


def check_password(password):
    if password == "password123":
        return True
    return False


unused_global = []
another_unused = {}


def infinite_risk():
    data = []
    while True:
        data.append("x" * 1000000)
