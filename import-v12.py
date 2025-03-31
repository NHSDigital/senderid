import time  # For tracking processing rates and timings
from lxml import etree
import yaml
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread, Lock
import sys
import json
import signal
import threading

# Log when the script starts
print("Script started.")

# Define the XML file path
xml_file_path = "HSCOrgRefData_Full_20250324.xml"  # Input file name

# Define the base output directory for YAML files
base_output_directory = "output_yaml_files"
if not os.path.exists(base_output_directory):
    os.makedirs(base_output_directory)
    print(f"Base output directory '{base_output_directory}' created.")
else:
    print(f"Base output directory '{base_output_directory}' already exists.")

# Define the base output directory for JSON files
base_output_directory_json = "output_json_files"
if not os.path.exists(base_output_directory_json):
    os.makedirs(base_output_directory_json)
    print(f"Base output directory '{base_output_directory_json}' created.")
else:
    print(f"Base output directory '{base_output_directory_json}' already exists.")

# Initialize counters for roles
primary_role_counts = {}  # Dictionary to track counts for each primary role ID
no_primary_role_count = 0  # Counter for organisations without a primaryRole=true

# Statistics
start_time = time.time()  # Track start time
org_processed_count = 0  # Total organisations processed

# Create a Queue to hold pre-loaded batches
batch_queue = Queue()

# Create a lock for thread-safe operations
data_lock = Lock()

# Global dictionary to store summary data for all batches
summary_data_global = {}

# Flag to indicate if the script should stop
stop_flag = threading.Event()

# Track the number of Ctrl+C presses
ctrl_c_count = 0

# Signal handler for graceful shutdown
def handle_sigint(signum, frame):
    global ctrl_c_count
    ctrl_c_count += 1

    if ctrl_c_count == 1:
        print("\nCtrl+C detected! Stopping gracefully...")
        stop_flag.set()  # Set the stop flag to signal threads to stop
    elif ctrl_c_count == 2:
        print("\nSecond Ctrl+C detected! Forcefully stopping...")
        sys.exit(1)  # Exit immediately

# Register the signal handler
signal.signal(signal.SIGINT, handle_sigint)

# Recursive function to process XML elements into a dictionary
def process_element(element):
    data = {}
    if element.attrib:  # Process attributes
        for attr_name, attr_value in element.attrib.items():
            data[attr_name] = attr_value
    for child in element:  # Process child elements
        child_tag = child.tag.split('}')[-1]  # Remove namespace if present
        child_data = process_element(child)  # Recursive call
        if child_tag in data:
            if not isinstance(data[child_tag], list):  # Convert to list
                data[child_tag] = [data[child_tag]]
            data[child_tag].append(child_data)
        else:
            data[child_tag] = child_data
    if not data and element.text and element.text.strip():  # Add text content
        data = element.text.strip()

    # Ensure Roles["Role"] is always a list
    if "Roles" in data and "Role" in data["Roles"]:
        if isinstance(data["Roles"]["Role"], dict):  # If a single role, wrap it in a list
            data["Roles"]["Role"] = [data["Roles"]["Role"]]

    return data

# Function to process each Organisation element and buffer YAML data
def generate_organisation_yaml(org_element, org_count, batch_yaml_data):
    global no_primary_role_count, primary_role_counts, org_processed_count

    org_id_element = org_element.find("./OrgId")
    status_element = org_element.find("./Status")
    roles_element = org_element.find("./Roles")
    geo_loc_element = org_element.find("./GeoLoc/Location")

    # Error if Roles collection is not found
    if roles_element is None:
        print(f"Error: 'Roles' element not found in Organisation {org_count}. Stopping script.")
        sys.exit(1)

    # Extract `id` attribute from Role where `primaryRole` is true
    role_id = "Unknown"
    for role in roles_element.findall("./Role"):
        if role.attrib.get("primaryRole") == "true":
            role_id = role.attrib.get("id", "Unknown")
            break

    # Count organisations with or without a valid primary role
    if role_id == "Unknown":
        no_primary_role_count += 1
        print(f"Warning: No Role with 'primaryRole=true' found in Organisation {org_count}.")
    else:
        primary_role_counts[role_id] = primary_role_counts.get(role_id, 0) + 1

    status_value = status_element.attrib.get("value") if status_element is not None else "Unknown"

    # Extract PostCode and UPRN from GeoLoc/Location
    post_code = geo_loc_element.findtext("PostCode", default="Unknown") if geo_loc_element is not None else "Unknown"
    uprn = geo_loc_element.findtext("UPRN", default="Unknown") if geo_loc_element is not None else "Unknown"

    if org_id_element is not None:
        extension = org_id_element.get("extension")

        if extension:
            organisation_dict = process_element(org_element)
            organisation_dict["metadata"] = {
                "org_id": extension,
                "status": status_value,
                "role_id": role_id,
                "post_code": post_code,
                "uprn": uprn
            }
            # Determine folder path based on Status and Role ID
            folder_path = os.path.join(base_output_directory, status_value, role_id)
            if folder_path not in batch_yaml_data:
                batch_yaml_data[folder_path] = []
            batch_yaml_data[folder_path].append((extension, organisation_dict))
    org_element.clear()  # Clear memory for the element

    # Increment processed count
    org_processed_count += 1

# Function to process a single batch of organisations and write YAML, JSON data, and summary files
def process_batch_elements(batch, org_count):
    global org_processed_count, summary_data_global
    batch_yaml_data = {}  # Buffer to collect YAML data grouped by folder structure
    batch_json_data = {}  # Buffer to collect JSON data grouped by folder structure

    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust thread count for efficiency
        futures = [executor.submit(generate_organisation_yaml, elem, org_count - len(batch) + index + 1, batch_yaml_data)
                   for index, elem in enumerate(batch)]
        for future in as_completed(futures):
            future.result()  # Wait for all tasks in the batch to complete

    # Copy YAML data to JSON data buffer and collect summary data
    for folder_path, yaml_data in batch_yaml_data.items():
        json_folder_path = folder_path.replace(base_output_directory, base_output_directory_json)
        batch_json_data[json_folder_path] = yaml_data

        # Collect summary data for the current folder
        for file_name, content in yaml_data:
            # Extract all role IDs, including non-primary roles
            roles = content.get("Roles", {}).get("Role", [])
            if isinstance(roles, dict):  # If a single role, wrap it in a list
                roles = [roles]

            role_ids = [role.get("id", "Unknown") for role in roles if isinstance(role, dict)]  # Ensure role is a dictionary

            # Extract metadata fields
            status = content["metadata"].get("status", "Unknown")
            post_code = content["metadata"].get("post_code", "Unknown")
            uprn = content["metadata"].get("uprn", "Unknown")

            summary_entry = {
                "org_id": content["metadata"]["org_id"],
                "name": content.get("Name", "Unknown"),
                "primary_role_id": content["metadata"]["role_id"],
                "role_ids": role_ids,
                "status": status,
                "post_code": post_code,
                "uprn": uprn
            }

            # Update the global summary data
            with data_lock:
                summary_data_global[summary_entry["org_id"]] = summary_entry

    # Write buffered YAML data for each folder
    for folder_path, yaml_data in batch_yaml_data.items():
        os.makedirs(folder_path, exist_ok=True)  # Ensure the folder exists
        for file_name, content in yaml_data:
            file_path = os.path.join(folder_path, f"{file_name}.yaml")
            with open(file_path, "w") as yaml_file:
                yaml.dump(content, yaml_file, default_flow_style=False)

    # Write buffered JSON data for each folder
    for folder_path, json_data in batch_json_data.items():
        os.makedirs(folder_path, exist_ok=True)  # Ensure the folder exists
        for file_name, content in json_data:
            file_path = os.path.join(folder_path, f"{file_name}.json")
            with open(file_path, "w") as json_file:
                json.dump(content, json_file, indent=4)

    print(f"Batch {org_count} written to corresponding folder structure")

# Write the accumulated summary data to files after all batches are processed
def write_summary_files():
    # Group summary entries by their status and role_id
    grouped_summary_data = {}

    for org_id, summary_entry in summary_data_global.items():
        # Extract status and role_id from the summary entry
        status = summary_entry.get("status", "Unknown")
        role_id = summary_entry.get("primary_role_id", "Unknown")

        # Construct the key for grouping
        group_key = (status, role_id)

        # Add the summary entry to the appropriate group
        if group_key not in grouped_summary_data:
            grouped_summary_data[group_key] = []
        grouped_summary_data[group_key].append(summary_entry)

    # Write grouped summary data to files
    for (status, role_id), summary_entries in grouped_summary_data.items():
        # Construct the folder paths for YAML and JSON summaries
        yaml_summary_dir = os.path.join(base_output_directory, status, role_id)
        json_summary_dir = os.path.join(base_output_directory_json, status, role_id)

        # Ensure the directories exist
        os.makedirs(yaml_summary_dir, exist_ok=True)
        os.makedirs(json_summary_dir, exist_ok=True)

        # Write YAML summary
        yaml_summary_path = os.path.join(yaml_summary_dir, "summary.yaml")
        with open(yaml_summary_path, "w") as yaml_summary_file:
            yaml.dump(summary_entries, yaml_summary_file, default_flow_style=False)
        print(f"YAML summary file created: {yaml_summary_path}")

        # Write JSON summary
        json_summary_path = os.path.join(json_summary_dir, "summary.json")
        with open(json_summary_path, "w") as json_summary_file:
            json.dump(summary_entries, json_summary_file, indent=4)
        print(f"JSON summary file created: {json_summary_path}")

# Function to pre-load batches into the queue
def preload_batches(context, batch_size):
    batch = []
    org_count = 0

    for event, elem in context:
        if stop_flag.is_set():  # Check if the stop flag is set
            print("Preloading stopped.")
            break

        if event == "end" and elem.tag.endswith("Organisation"):
            batch.append(elem)
            org_count += 1

            # Add the batch to the queue when it's full
            if len(batch) == batch_size:
                batch_queue.put((batch, org_count))
                print(f"Pre-loaded Batch: {org_count - len(batch) + 1} to {org_count}")
                batch = []  # Clear the batch after adding to the queue

    # Add remaining organisations in the final batch to the queue
    if not stop_flag.is_set() and batch:
        batch_queue.put((batch, org_count))
        print(f"Pre-loaded Final Batch: {org_count - len(batch) + 1} to {org_count}")

    # Signal that pre-loading is complete
    batch_queue.put(None)

# Function to process organisations from the queue
def process_batches_from_queue():
    global start_time, org_processed_count

    while not stop_flag.is_set():
        batch_info = batch_queue.get()
        if batch_info is None:  # End of queue signal
            break

        batch, org_count = batch_info
        print(f"Processing Batch: {org_count - len(batch) + 1} to {org_count}")
        process_batch_elements(batch, org_count)

        # Print regular status update
        current_time = time.time()
        elapsed_time = current_time - start_time
        processing_rate = org_processed_count / elapsed_time
        print(f"Status Update: {org_processed_count} organisations processed.")
        print(f"Current Processing Rate: {processing_rate:.2f} organisations per second.")

# Main script logic
try:
    # Initialize parser and log context
    print("Initializing parser context...")
    context = etree.iterparse(xml_file_path, events=("start", "end"), recover=True)
    print("Parser context initialized.")

    # Define batch size
    batch_size = 5000  # Adjust batch size to balance performance and memory usage

    # Start pre-loading batches in a separate thread
    preloading_thread = Thread(target=preload_batches, args=(context, batch_size))
    preloading_thread.start()

    # Start processing batches while pre-loading continues
    print("Starting batch processing...")
    process_batches_from_queue()

    # Ensure pre-loading has completed
    preloading_thread.join()
    print("Processing complete.")

finally:
    # Write the accumulated summary data to files
    print("Writing summary files...")
    write_summary_files()

    # Calculate total time taken
    end_time = time.time()
    total_time_taken = end_time - start_time
    average_processing_rate = org_processed_count / total_time_taken

    # Output summary
    print("\nSummary of Organisations Processing:")
    print(f"Total Organisations Processed: {org_processed_count}")
    print(f"Total Time Taken: {total_time_taken:.2f} seconds")
    print(f"Average Processing Rate: {average_processing_rate:.2f} organisations per second")
    print("\nSummary of Organisations by Primary Role:")
    for role_id, count in primary_role_counts.items():
        print(f"Role ID '{role_id}': {count} organisations")
    print(f"No primary role found: {no_primary_role_count} organisations")