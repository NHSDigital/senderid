import xml.etree.ElementTree as ET
import yaml
import os
from concurrent.futures import ThreadPoolExecutor

# Log when the script starts
print("Script started.")

# Define the XML file path
xml_file_path = "data.xml"  # Input file name

# Define the output directory for YAML files
output_directory = "output_yaml_files"
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
    print(f"Output directory '{output_directory}' created.")
else:
    print(f"Output directory '{output_directory}' already exists.")

# Namespace used in the XML file
namespace = "{http://refdata.hscic.gov.uk/org/v2-0-0}"

# Recursive function to process XML elements into a dictionary
def process_element(element):
    data = {}
    # Process attributes of the current element
    if element.attrib:
        for attr_name, attr_value in element.attrib.items():
            data[attr_name] = attr_value
    # Process child elements
    for child in element:
        child_tag = child.tag.split('}')[-1]  # Remove namespace
        child_data = process_element(child)  # Recursive call
        if child_tag in data:
            # If the tag already exists, convert to a list
            if not isinstance(data[child_tag], list):
                data[child_tag] = [data[child_tag]]
            data[child_tag].append(child_data)
        else:
            data[child_tag] = child_data
    # Add text content if no child elements exist
    if not data and element.text and element.text.strip():
        data = element.text.strip()
    return data

# Function to process each Organisation element
def process_organisation(org_element, org_count):
    print(f"Starting to process Organisation {org_count}...")
    org_id_element = org_element.find(f"./{namespace}OrgId")  # Try with namespace
    if org_id_element is None:  # Try without namespace if not found
        org_id_element = org_element.find("./OrgId")
    if org_id_element is not None:
        extension = org_id_element.get("extension")
        if extension:
            print(f"Processing Organisation {org_count}: OrgId extension: {extension}")

            # Convert Organisation element to a dictionary
            organisation_dict = process_element(org_element)

            # Write to a YAML file
            yaml_file_path = os.path.join(output_directory, f"{extension}.yaml")
            with open(yaml_file_path, "w") as yaml_file:
                yaml.dump(organisation_dict, yaml_file, default_flow_style=False)
        else:
            print(f"Organisation {org_count} has an OrgId element, but no extension attribute.")
    else:
        print(f"Organisation {org_count} does not have an OrgId element.")
    # Clear the element from memory
    org_element.clear()

# Initialize parser and log context
print("Initializing parser context...")
context = ET.iterparse(xml_file_path, events=("start", "end"))
print("Parser context initialized.")

# Initialize organisation count
print("Initializing organisation count...")
org_count = 0
print("Organisation count initialized.")

# Multithreaded processing
print("Starting multithreaded processing...")
executor = ThreadPoolExecutor(max_workers=8)  # Configure the number of threads
futures = []

for event, elem in context:
    if event == "end" and elem.tag.endswith("Organisation"):
        org_count += 1
        futures.append(executor.submit(process_organisation, elem, org_count))

# Wait for all threads to complete
for future in futures:
    future.result()

executor.shutdown()
print(f"Processing complete. Total Organisations processed: {org_count}")