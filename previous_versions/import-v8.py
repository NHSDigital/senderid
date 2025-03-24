import time  # For tracking processing rates and timings
from lxml import etree
import yaml
import os
from concurrent.futures import ThreadPoolExecutor
import sys

# Log when the script starts
print("Script started.")

# Define the XML file path
xml_file_path = "data.xml"  # Input file name

# Define the base output directory for YAML files
base_output_directory = "output_yaml_files"
if not os.path.exists(base_output_directory):
    os.makedirs(base_output_directory)
    print(f"Base output directory '{base_output_directory}' created.")
else:
    print(f"Base output directory '{base_output_directory}' already exists.")

# Initialize counters for roles
primary_role_counts = {}  # Dictionary to track counts for each primary role ID
no_primary_role_count = 0  # Counter for organisations without a primaryRole=true

# Statistics
start_time = time.time()  # Track start time
org_processed_count = 0  # Total organisations processed

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
    return data

# Function to process each Organisation element
def process_organisation(org_element, org_count):
    global no_primary_role_count, primary_role_counts, org_processed_count

    org_id_element = org_element.find("./OrgId")
    status_element = org_element.find("./Status")
    roles_element = org_element.find("./Roles")

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

    if org_id_element is not None:
        extension = org_id_element.get("extension")

        if extension:
            nested_dir = os.path.join(base_output_directory, status_value, role_id)
            if not os.path.exists(nested_dir):
                os.makedirs(nested_dir)
            
            organisation_dict = process_element(org_element)

            yaml_file_path = os.path.join(nested_dir, f"{extension}.yaml")
            with open(yaml_file_path, "w") as yaml_file:
                yaml.dump(organisation_dict, yaml_file, default_flow_style=False)
    org_element.clear()  # Clear memory for the element

    # Increment processed count
    org_processed_count += 1

# Function to process organisations in batches
def process_batch(context, batch_size):
    global no_primary_role_count, primary_role_counts, org_processed_count

    batch = []
    org_count = 0

    for event, elem in context:
        if event == "end" and elem.tag.endswith("Organisation"):
            batch.append(elem)
            org_count += 1

            # Process the batch when it's full
            if len(batch) == batch_size:
                print(f"Processing Batch: {org_count - len(batch) + 1} to {org_count}")
                process_batch_elements(batch, org_count)
                batch.clear()  # Clear the batch after processing

                # Print regular status update
                current_time = time.time()
                elapsed_time = current_time - start_time
                processing_rate = org_processed_count / elapsed_time
                print(f"Status Update: {org_processed_count} organisations processed.")
                print(f"Current Processing Rate: {processing_rate:.2f} organisations per second.")

    # Process remaining organisations in the final batch
    if batch:
        print(f"Processing Final Batch: {org_count - len(batch) + 1} to {org_count}")
        process_batch_elements(batch, org_count)

        # Print regular status update
        current_time = time.time()
        elapsed_time = current_time - start_time
        processing_rate = org_processed_count / elapsed_time
        print(f"Status Update: {org_processed_count} organisations processed.")
        print(f"Current Processing Rate: {processing_rate:.2f} organisations per second.")

# Helper function to process a single batch of organisations
def process_batch_elements(batch, org_count):
    with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust thread count for efficiency
        for index, elem in enumerate(batch):
            print(f"Processing Organisation {org_count - len(batch) + index + 1} in current batch.")
            executor.submit(process_organisation, elem, org_count)

# Initialize parser and log context
print("Initializing parser context...")
context = etree.iterparse(xml_file_path, events=("start", "end"), recover=True)
print("Parser context initialized.")

# Define batch size
batch_size = 1000  # Adjust batch size to balance performance and memory usage

print("Starting batched processing...")
process_batch(context, batch_size)
print("Processing complete.")

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