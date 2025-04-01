import aiofiles.os
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
import psutil  # For monitoring system and process resource usage
import logging
import aiofiles
import asyncio
import queue

# Define a custom TRACE level (lower than DEBUG)
TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")

def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kwargs)

logging.Logger.trace = trace

# Configure logging
LOG_LEVELS = {
    "TRACE": TRACE_LEVEL,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

def configure_logging(log_level):
    logging.basicConfig(
        level=LOG_LEVELS.get(log_level.upper(), logging.INFO),  # Default to INFO if invalid level is provided
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    global logger
    logger = logging.getLogger(__name__)
    print(f"Logging configured. Current log level: {log_level.upper()}")  # Print the configured log level
    return logger

logger = None

# Global variables
primary_role_counts = {}  # Dictionary to track counts for each primary role ID
no_primary_role_count = 0  # Counter for organisations without a primaryRole=true
org_processed_count = 0  # Total organisations processed
batch_queue = Queue(maxsize=10)  # Limit the queue to hold a maximum of 10 batches
data_lock = Lock()  # Lock for thread-safe operations
summary_data_global = {}  # Global dictionary to store summary data for all batches
stop_flag = threading.Event()  # Flag to indicate if the script should stop
ctrl_c_count = 0  # Track the number of Ctrl+C presses

# Signal handler for graceful shutdown
def handle_sigint(signum, frame):
    global ctrl_c_count
    ctrl_c_count += 1

    if ctrl_c_count == 1:
        logger.info("[General] Ctrl+C detected! Stopping gracefully...")
        stop_flag.set()  # Set the stop flag to signal threads to stop
    elif ctrl_c_count == 2:
        logger.info("[General] Second Ctrl+C detected! Forcefully stopping...")
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
def generate_organisation_yaml(org_element, org_count, batch_yaml_data, base_output_directory):
    global no_primary_role_count, primary_role_counts, org_processed_count

    org_id_element = org_element.find("./OrgId")
    status_element = org_element.find("./Status")
    roles_element = org_element.find("./Roles")
    geo_loc_element = org_element.find("./GeoLoc/Location")

    # Error if Roles collection is not found
    if roles_element is None:
        logger.error(f"[Generate Org YAML] 'Roles' element not found in Organisation {org_count}. Stopping script.")
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
        logger.warning(f"[Generate Org YAML] No Role with 'primaryRole=true' found in Organisation {org_count}.")
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
            # Use a simpler key structure
            key = f"{status_value}/{role_id}"
            if key not in batch_yaml_data:
                batch_yaml_data[key] = []
            batch_yaml_data[key].append((extension, organisation_dict))
    org_element.clear()  # Clear memory for the element

    # Increment processed count
    org_processed_count += 1

async def write_yaml_file(file_path, content):
    """Asynchronously write YAML content to a file."""
    async with aiofiles.open(file_path, "w") as yaml_file:
        await yaml_file.write(yaml.dump(content, default_flow_style=False))

async def write_json_file(file_path, content):
    """Asynchronously write JSON content to a file."""
    async with aiofiles.open(file_path, "w") as json_file:
        await json_file.write(json.dumps(content, indent=4))

async def process_batch_elements(batch, org_count, base_output_directory, base_output_directory_json, max_workers=5):
    """Process a single batch of organisations and write YAML, JSON data, and summary files."""
    global org_processed_count, summary_data_global
    batch_yaml_data = {}  # Buffer to collect YAML data grouped by folder structure
    batch_json_data = {}  # Buffer to collect JSON data grouped by folder structure

    # Process YAML generation concurrently
    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(
            None,
            generate_organisation_yaml,
            elem,
            org_count - len(batch) + index + 1,
            batch_yaml_data,
            base_output_directory,
        )
        for index, elem in enumerate(batch)
    ]
    await asyncio.gather(*tasks)

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
                "uprn": uprn,
            }

            # Update the global summary data
            with data_lock:
                summary_data_global[summary_entry["org_id"]] = summary_entry

    # Write buffered YAML and JSON data asynchronously
    write_tasks = []
    start_time = time.time()
    
    for folder_path, yaml_data in batch_yaml_data.items():
        absolute_folder_path = os.path.join(base_output_directory, folder_path)  # Ensure absolute path
        await aiofiles.os.makedirs(absolute_folder_path, exist_ok=True)  # Ensure the folder exists
        for file_name, content in yaml_data:
            file_path = os.path.join(absolute_folder_path, f"{file_name}.yaml")
            write_tasks.append(write_yaml_file(file_path, content))

    for folder_path, json_data in batch_json_data.items():
        absolute_folder_path = os.path.join(base_output_directory_json, folder_path)  # Ensure absolute path
        await aiofiles.os.makedirs(absolute_folder_path, exist_ok=True)  # Ensure the folder exists
        for file_name, content in json_data:
            file_path = os.path.join(absolute_folder_path, f"{file_name}.json")
            write_tasks.append(write_json_file(file_path, content))

    # Log the number of files being written
    logger.info(f"[Processing] Waiting for {len(write_tasks)} files to be written...")

    # Measure the time taken for all write tasks to complete
  
    await asyncio.gather(*write_tasks)
    end_time = time.time()

    # Log the completion of file writes
    logger.info(f"[Processing] All {len(write_tasks)} files written successfully in {(end_time - start_time):.2f} seconds.")

    logger.info(f"[Processing] Batch {org_count} written to corresponding folder structure")

# Write the accumulated summary data to files after all batches are processed
async def write_summary_files(base_output_directory, base_output_directory_json):
    global summary_data_global

    if not summary_data_global:
        logger.info("[Summary] No summary data to write.")
        return

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

    # Write grouped summary data asynchronously
    write_tasks = []
    start_time = time.time()
    for (status, role_id), summary_entries in grouped_summary_data.items():
        # Construct the folder paths for YAML and JSON summaries
        yaml_summary_dir = os.path.join(base_output_directory, status, role_id)
        json_summary_dir = os.path.join(base_output_directory_json, status, role_id)

        # Ensure the directories exist asynchronously
        await aiofiles.os.makedirs(yaml_summary_dir, exist_ok=True)
        await aiofiles.os.makedirs(json_summary_dir, exist_ok=True)

        # Write YAML summary
        yaml_summary_path = os.path.join(yaml_summary_dir, "summary.yaml")
        write_tasks.append(write_yaml_file(yaml_summary_path, summary_entries))

        # Write JSON summary
        json_summary_path = os.path.join(json_summary_dir, "summary.json")
        write_tasks.append(write_json_file(json_summary_path, summary_entries))

    # Log the number of files being written
    logger.info(f"[Summary] Waiting for {len(write_tasks)} summary files to be written...")

    # Measure the time taken for all write tasks to complete
    await asyncio.gather(*write_tasks)
    end_time = time.time()

    # Log the completion of file writes
    logger.info(f"[Summary] All {len(write_tasks)} summary files written successfully in {(end_time - start_time):.2f} seconds.")

# Function to monitor and log resource usage
def monitor_resources(interval=5, preloading_thread=None, processing_thread=None):
    """Monitor and log system and thread-level resource usage."""
    try:
        process = psutil.Process(os.getpid())  # Get the current process
        thread_cpu_times = {}  # Store initial CPU times for threads
        thread_io_counters = {}  # Store initial I/O counters for threads

        while not stop_flag.is_set():
            # Get system-wide CPU usage
            cpu_usage = psutil.cpu_percent(interval=None)  # System-wide CPU usage
            memory_info = process.memory_info()  # Memory usage of the current process
            io_counters = process.io_counters()  # I/O stats of the current process

            # Log overall resource usage
            logger.info(f"[Resource Monitor] CPU Usage: {cpu_usage:.2f}%")
            logger.info(f"[Resource Monitor] Memory Usage: {memory_info.rss / (1024 * 1024):.2f} MB (RSS)")
            logger.info(f"[Resource Monitor] Disk Read: {io_counters.read_bytes / (1024 * 1024):.2f} MB")
            logger.info(f"[Resource Monitor] Disk Write: {io_counters.write_bytes / (1024 * 1024):.2f} MB")

            # Debug: Log all threads in the process
            process_threads = process.threads()
            logger.debug(f"[Resource Monitor] Process Threads: {[t.id for t in process_threads]}")

            # Log CPU usage and I/O activity for specific threads
            for thread_name, thread in [("Preloading Thread", preloading_thread), ("Processing Thread", processing_thread)]:
                if thread:
                    native_thread_id = threading.get_native_id() if hasattr(threading, "get_native_id") else thread.ident
                    logger.debug(f"[Resource Monitor] Looking for {thread_name} with Native ID {native_thread_id}")

                    # Check if the thread is still alive
                    if not thread.is_alive():
                        logger.warning(f"[Resource Monitor] {thread_name} with Native ID {native_thread_id} is no longer alive.")
                        continue

                    # Check if the native thread ID matches any process thread ID
                    found = False
                    for t in process_threads:
                        if t.id == native_thread_id:
                            found = True
                            # Get the current CPU time for the thread
                            current_cpu_time = process.cpu_times().user + process.cpu_times().system

                            # Calculate CPU usage since the last interval
                            if native_thread_id in thread_cpu_times:
                                previous_cpu_time = thread_cpu_times[native_thread_id]
                                cpu_usage = (current_cpu_time - previous_cpu_time) / interval * 100
                                logger.info(f"[Resource Monitor] {thread_name} (Native ID {native_thread_id}): CPU Usage: {cpu_usage:.2f}%")
                            else:
                                logger.info(f"[Resource Monitor] {thread_name} (Native ID {native_thread_id}): CPU Usage: Calculating...")

                            # Update the stored CPU time for the thread
                            thread_cpu_times[native_thread_id] = current_cpu_time

                            # Log I/O activity (read/write counts and bytes)
                            current_io_counters = process.io_counters()
                            if native_thread_id in thread_io_counters:
                                previous_io_counters = thread_io_counters[native_thread_id]
                                read_diff = current_io_counters.read_bytes - previous_io_counters.read_bytes
                                write_diff = current_io_counters.write_bytes - previous_io_counters.write_bytes
                                logger.info(f"[Resource Monitor] {thread_name} (Native ID {native_thread_id}): Disk Read: {read_diff / (1024 * 1024):.2f} MB")
                                logger.info(f"[Resource Monitor] {thread_name} (Native ID {native_thread_id}): Disk Write: {write_diff / (1024 * 1024):.2f} MB")
                            else:
                                logger.info(f"[Resource Monitor] {thread_name} (Native ID {native_thread_id}): I/O Activity: Calculating...")

                            # Update the stored I/O counters for the thread
                            thread_io_counters[native_thread_id] = current_io_counters
                            break

                    if not found:
                        logger.warning(f"[Resource Monitor] {thread_name} with Native ID {native_thread_id} not found in process threads.")

            # Wait for the specified interval
            stop_flag.wait(interval)
    except Exception as e:
        logger.error(f"[Resource Monitor] Error: {e}")

# Function to preload batches
def preload_batches(context, batch_size, log_interval=5):
    """Preload batches of XML elements into the queue."""
    global stop_flag
    batch = []
    org_count = 0  # Track the total number of organisations processed
    orgs_in_last_interval = 0  # Track the number of organisations added in the last interval
    interval_start_time = time.time()  # Track the start time of the interval

    try:
        for event, element in context:
            # Stop processing if the stop_flag is set
            if stop_flag.is_set():
                logger.info("[Preloading] Stop flag detected. Stopping preload_batches.")
                break

            if event == "end" and element.tag == "Organisation":
                batch.append(element)
                org_count += 1
                orgs_in_last_interval += 1

                # Log the number of organisations added every `log_interval` seconds
                current_time = time.time()
                if current_time - interval_start_time >= log_interval:
                    logger.info(f"[Preloading] {orgs_in_last_interval} organisations added to the batch in the last {log_interval} seconds.")
                    orgs_in_last_interval = 0
                    interval_start_time = current_time

                # If the batch size is reached, add the batch to the queue
                if len(batch) == batch_size:
                    logger.info(f"[Preloading] Adding batch to queue with {len(batch)} elements. Total organisations: {org_count}")
                    while not stop_flag.is_set():
                        try:
                            batch_queue.put((batch, org_count), timeout=5)
                            logger.info(f"[Preloading] Batch added to queue. Current queue size: {batch_queue.qsize()}")
                            break
                        except queue.Full:
                            logger.warning("[Preloading] Batch queue is full. Retrying...")
                            time.sleep(1)
                    batch = []

        # Add any remaining elements as the final batch
        if batch and not stop_flag.is_set():
            logger.info(f"[Preloading] Adding final batch to queue with {len(batch)} elements. Total organisations: {org_count}")
            while not stop_flag.is_set():
                try:
                    batch_queue.put((batch, org_count), timeout=5)
                    logger.info(f"[Preloading] Final batch added to queue. Current queue size: {batch_queue.qsize()}")
                    break
                except queue.Full:
                    logger.warning("[Preloading] Batch queue is full. Retrying...")
                    time.sleep(1)

        # Signal the end of preloading
        if not stop_flag.is_set():
            logger.info("[Preloading] Preloading complete. Sending end-of-queue signal.")
            batch_queue.put(None)

    except Exception as e:
        logger.error(f"[Preloading] Error in preload_batches: {e}")
        stop_flag.set()

# Function to process organisations from the queue
async def process_batches_from_queue(base_output_directory, base_output_directory_json, max_workers=5):
    """Process batches of organisations from the queue."""
    global start_time, org_processed_count

    while not stop_flag.is_set():
        try:
            # Attempt to get a batch from the queue with a timeout
            batch_info = batch_queue.get(timeout=5)
        except queue.Empty:
            logger.warning("[Processing] Batch queue is empty. Waiting for new batches...")
            continue

        if batch_info is None:  # End of queue signal
            logger.info("[Processing] End of queue signal received. Stopping batch processing.")
            break

        batch, org_count = batch_info
        start_org_count = org_count - len(batch) + 1  # Calculate the starting organisation number
        start_org_count = max(start_org_count, 1)  # Ensure it is at least 1
        logger.info(f"[Processing] Processing Batch: {start_org_count} to {org_count}")

        # Start timing the batch processing
        start_time = time.time()
        await process_batch_elements(batch, org_count, base_output_directory, base_output_directory_json, max_workers)
        end_time = time.time()

        # Log batch processing time
        logger.info(f"[Processing] Batch processed in {(end_time - start_time) * 1000:.2f} ms.")

        # Print regular status updates
        current_time = time.time()
        elapsed_time = current_time - start_time
        processing_rate = org_processed_count / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"[Processing] Status Update: {org_processed_count} organisations processed.")
        logger.info(f"[Processing] Current Processing Rate: {processing_rate:.2f} organisations per second.")

def run_process_batches_from_queue(base_output_directory, base_output_directory_json, max_workers):
    asyncio.run(process_batches_from_queue(base_output_directory, base_output_directory_json, max_workers))

# Main function
async def main(xml_file_path, base_output_path, batch_size, log_level, enable_monitoring=False, enable_processing=True, max_workers=5, monitor_interval=30, num_preload_threads=4, log_interval=5):
    global org_processed_count, summary_data_global, start_time

    # Configure logging
    configure_logging(log_level)

    # Initialize start_time to track processing start time
    start_time = time.time()

    # Define the base output directories
    base_output_directory = os.path.join(base_output_path, "yaml")
    base_output_directory_json = os.path.join(base_output_path, "json")

    # Ensure output directories exist
    await aiofiles.os.makedirs(base_output_directory, exist_ok=True)
    await aiofiles.os.makedirs(base_output_directory_json, exist_ok=True)

    logger.info("[General] Initializing parser context...")
    context = etree.iterparse(xml_file_path, events=("start", "end"), recover=True)
    logger.info("[General] Parser context initialized.")

    # Start resource monitoring in a separate thread if enabled
    monitoring_thread = None
    preloading_thread = None
    processing_thread = None

    try:
        # Start pre-loading batches in a separate thread
        preloading_thread = Thread(target=preload_batches, args=(context, batch_size, log_interval))
        preloading_thread.start()

        # Start processing batches in a separate thread if enabled
        if enable_processing:
            logger.info("[General] Starting batch processing...")
            processing_thread = Thread(target=run_process_batches_from_queue, args=(base_output_directory, base_output_directory_json, max_workers))
            processing_thread.start()

        # Start monitoring thread after preloading and processing threads are initialized
        if enable_monitoring:
            logger.info("[General] Starting resource monitoring...")
            monitoring_thread = Thread(target=monitor_resources, args=(monitor_interval, preloading_thread, processing_thread))
            monitoring_thread.start()

        # Wait for preloading to complete
        preloading_thread.join()
        logger.info("[General] Preloading complete.")

        # Wait for processing to complete if enabled
        if processing_thread:
            processing_thread.join()
            logger.info("[General] Batch processing complete.")

        # Write summary files asynchronously
        if enable_processing:
            await write_summary_files(base_output_directory, base_output_directory_json)

    except Exception as e:
        # Handle any errors during batch processing or preloading
        logger.error(f"[Error] Error occurred: {e}")
        stop_flag.set()  # Signal all threads to stop
        if monitoring_thread:
            monitoring_thread.join()  # Ensure the monitoring thread stops
        sys.exit(1)  # Exit the process with an error code

    finally:
        # Stop the monitoring thread if it was started
        if monitoring_thread:
            stop_flag.set()
            monitoring_thread.join()  # Ensure the monitoring thread exits

    # Calculate total time taken
    end_time = time.time()
    total_time_taken = end_time - start_time
    average_processing_rate = org_processed_count / total_time_taken if org_processed_count > 0 else 0

    # Output summary
    logger.info("[Summary] Summary of Organisations Processing:")
    logger.info(f"[Summary] Total Organisations Processed: {org_processed_count}")
    logger.info(f"[Summary] Total Time Taken: {total_time_taken:.2f} seconds")
    logger.info(f"[Summary] Average Processing Rate: {average_processing_rate:.2f} organisations per second")
    logger.info("\n[Summary] Summary of Organisations by Primary Role:")
    for role_id, count in primary_role_counts.items():
        logger.info(f"[Summary] Role ID '{role_id}': {count} organisations")
    logger.info(f"[Summary] No primary role found: {no_primary_role_count} organisations")

# Entry point for the script
if __name__ == "__main__":
    # Default values for XML file path, base output path, batch size, log level, monitoring flag, processing flag, max_workers, monitor_interval, num_preload_threads, and log_interval
    default_xml_file_path = "HSCOrgRefData_Full_20250324.xml"
    default_base_output_path = "output"
    default_batch_size = 1000
    default_log_level = "INFO"
    default_enable_monitoring = True
    default_enable_processing = True
    default_max_workers = 6
    default_monitor_interval = 5
    default_num_preload_threads = 4
    default_log_interval = 5

    # Allow overriding via command-line arguments
    xml_file_path = sys.argv[1] if len(sys.argv) > 1 else default_xml_file_path
    base_output_path = sys.argv[2] if len(sys.argv) > 2 else default_base_output_path
    batch_size = int(sys.argv[3]) if len(sys.argv) > 3 else default_batch_size
    log_level = sys.argv[4] if len(sys.argv) > 4 else default_log_level
    enable_monitoring = sys.argv[5].lower() == "true" if len(sys.argv) > 5 else default_enable_monitoring
    enable_processing = sys.argv[6].lower() == "true" if len(sys.argv) > 6 else default_enable_processing
    max_workers = int(sys.argv[7]) if len(sys.argv) > 7 else default_max_workers
    monitor_interval = int(sys.argv[8]) if len(sys.argv) > 8 else default_monitor_interval
    num_preload_threads = int(sys.argv[9]) if len(sys.argv) > 9 else default_num_preload_threads
    log_interval = int(sys.argv[10]) if len(sys.argv) > 10 else default_log_interval

    # Run the main function
    asyncio.run(main(
        xml_file_path,
        base_output_path,
        batch_size,
        log_level,
        enable_monitoring,
        enable_processing,
        max_workers,
        monitor_interval,
        num_preload_threads,
        log_interval,
    ))