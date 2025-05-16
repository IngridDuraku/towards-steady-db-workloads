import json
import os
import uuid


def create_result_directory(name, base_path="result"):
    """Creates a uniquely named directory for a new result and returns the path."""
    unique_id = uuid.uuid4().hex[0:8]
    workload_dir = os.path.join(base_path, f"{name}_{unique_id}")
    os.makedirs(workload_dir, exist_ok=True)

    return workload_dir


def load_json(file_path):
    with open(file_path, "r") as file:
        config = json.load(file)

        return config


def save_json_file(obj, file_path):
    with open(file_path, mode="x") as f:
        json.dump(obj, f, indent=4)
