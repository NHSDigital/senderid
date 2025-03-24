from lxml import etree
import yaml
import os
from concurrent.futures import ThreadPoolExecutor
import sys  # To enable exiting the script

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
    global no_primary_role_count, primary_role_counts  # Access global counters

    org_id_element = org_element.find("./OrgId")  # No namespace for OrgId
    status_element = org_element.find("./Status")  # No namespace for Status
    roles_element = org_element.find("./Roles")  # Find the Roles collection
    
    # Error if Roles collection is not found
    if roles_element is None:
        print(f"Error: 'Roles' element not found in Organisation {org_count}. Stopping script.")
        sys.exit(1)

    # Extract `id` attribute from Role where `primaryRole` is true
    role_id = "Unknown"
    for role in roles_element.findall("./Role"):  # Iterate through Role elements
        if role.attrib.get("primaryRole") == "true":  # Match 'primaryRole'
            role_id = role.attrib.get("id", "Unknown")  # Use `id` or default to Unknown
            break

    # Count organisations with or without a valid primary role
    if role_id == "Unknown":
        no_primary_role_count += 1  # Increment the counter for missing primary role
        print(f"Warning: No Role with 'primaryRole=true' found in Organisation {org_count}.")
    else:
        primary_role_counts[role_id] = primary_role_counts.get(role_id, 0) + 1  # Increment count for this role ID

    status_value = status_element.attrib.get("value") if status_element is not None else "Unknown"

    if org_id_element is not None:
        extension = org_id_element.get("extension")

        if extension:
            print(f"Processing Organisation {org_count}: OrgId extension: {extension}, Status Value: {status_value}, Role ID: {role_id}")

            # Create nested directories based on the Status value and Role ID
            nested_dir = os.path.join(base_output_directory, status_value, role_id)
            if not os.path.exists(nested_dir):
                os.makedirs(nested_dir)
                print(f"Nested directory '{nested_dir}' created.")
            
            # Convert Organisation element to a dictionary
            organisation_dict = process_element(org_element)

            # Write directly to YAML file in the nested directory
            yaml_file_path = os.path.join(nested_dir, f"{extension}.yaml")
            with open(yaml_file_path, "w") as yaml_file:
                yaml.dump(organisation_dict, yaml_file, default_flow_style=False)
        else:
            print(f"Organisation {org_count} has an OrgId element, but no extension attribute.")
    else:
        print(f"Organisation {org_count} does not have an OrgId element.")
    org_element.clear()  # Clear memory for the element

# Initialize parser and log context
print("Initializing parser context...")
context = etree.iterparse(xml_file_path, events=("start", "end"), recover=True)
print("Parser context initialized.")

# Initialize organisation count
print("Initializing organisation count...")
org_count = 0
print("Organisation count initialized.")

# Configure multithreaded processing
print("Starting multithreaded processing...")
executor = ThreadPoolExecutor(max_workers=4)  # Reduced threads for memory efficiency

for event, elem in context:
    if event == "end" and elem.tag.endswith("Organisation"):
        org_count += 1
        executor.submit(process_organisation, elem, org_count)

executor.shutdown()
print(f"Processing complete. Total Organisations processed: {org_count}")

# Output summary of role counts
print("\nSummary of Organisations by Primary Role:")
for role_id, count in primary_role_counts.items():
    print(f"Role ID '{role_id}': {count} organisations")
print(f"No primary role found: {no_primary_role_count} organisations")