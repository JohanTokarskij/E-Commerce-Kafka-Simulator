import json
from datetime import datetime

def save_state(file_path, state):
    with open(file_path, 'w') as file:
        json.dump(state, file)

def load_state(file_path, default_state):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return default_state