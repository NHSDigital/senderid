from lxml import etree
import yaml
import os
from concurrent.futures import ThreadPoolExecutor

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

# Namespace used in the XML file
namespace = "{http://refdata.hscic.gov.uk/org/v2-0-0}"

# Recursive function to process XML elements into a dictionary
def process_element(element):
    data = {}
    if element.attrib:  # Process attributes
        for attr_name, attr_value in element.attrib.items():
            data[attr_name] = attr_value
    for child in element:  # Process child elements
        child_tag = child.tag.split('}')[-1]  # Remove namespace
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
    org_id_element = org_element.find(f"./{namespace}OrgId")  # Try with namespace
    if org_id_element is None:  # Try without namespace if not found
        org_id_element = org_element.find("./OrgId")
    status_element = org_element.find(f"./Status")  # Find Status element
    org_record_class = org_element.attrib.get("orgRecordClass")  # Attribute orgRecordClass

    if org_id_element is not None:
        extension = org_id_element.get("extension")
        status_value = status_element.attrib.get("value") if status_element is not None else "Unknown"
        record_class_value = org_record_class if org_record_class is not None else "Unknown"

        if extension:
            print(f"Processing Organisation {org_count}: OrgId extension: {extension}, Status Value: {status_value}, Record Class: {record_class_value}")

            # Create nested directories based on the Status value and orgRecordClass
            nested_dir = os.path.join(base_output_directory, status_value, record_class_value)
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