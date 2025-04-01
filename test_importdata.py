import unittest
from unittest.mock import patch, MagicMock, mock_open, call
from lxml import etree
import os
import shutil
import yaml
import json
import threading

# Import functions from the script
from importdata import (
    process_element,
    generate_organisation_yaml,
    process_batch_elements,
    write_summary_files,
    preload_batches,
    process_batches_from_queue,
    main,
    monitor_resources,
)

class TestImportData(unittest.TestCase):
    def setUp(self):
        """Set up the test environment."""
        self.test_output_path = "testoutput"
        self.yaml_output_path = os.path.join(self.test_output_path, "yaml")
        self.json_output_path = os.path.join(self.test_output_path, "json")

        # Ensure the test output directory is clean
        if os.path.exists(self.test_output_path):
            shutil.rmtree(self.test_output_path)
        os.makedirs(self.test_output_path)

    def tearDown(self):
        """Clean up the test environment."""
        if os.path.exists(self.test_output_path):
            shutil.rmtree(self.test_output_path)

    def test_process_element(self):
        """Test the process_element function."""
        xml = """
        <Organisation>
            <OrgId extension="ORG123" />
            <Status value="Active" />
            <GeoLoc>
                <Location>
                    <PostCode>AB12 3CD</PostCode>
                    <UPRN>123456789</UPRN>
                </Location>
            </GeoLoc>
        </Organisation>
        """
        element = etree.fromstring(xml)
        result = process_element(element)

        # Assert the parsed data
        self.assertEqual(result["OrgId"]["extension"], "ORG123")
        self.assertEqual(result["Status"]["value"], "Active")
        self.assertEqual(result["GeoLoc"]["Location"]["PostCode"], "AB12 3CD")
        self.assertEqual(result["GeoLoc"]["Location"]["UPRN"], "123456789")

    def test_process_element_nested_xml(self):
        """Test the process_element function with nested XML."""
        xml = """
        <Organisation>
            <OrgId extension="ORG123" />
            <Details>
                <Contact>
                    <Phone>123456789</Phone>
                    <Email>test@example.com</Email>
                </Contact>
            </Details>
        </Organisation>
        """
        element = etree.fromstring(xml)
        result = process_element(element)

        # Assert the parsed data
        self.assertEqual(result["OrgId"]["extension"], "ORG123")
        self.assertEqual(result["Details"]["Contact"]["Phone"], "123456789")
        self.assertEqual(result["Details"]["Contact"]["Email"], "test@example.com")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_organisation_yaml(self, mock_open, mock_makedirs):
        """Test the generate_organisation_yaml function."""
        xml = """
        <Organisation>
            <OrgId extension="ORG123" />
            <Status value="Active" />
            <Roles>
                <Role id="RO126" primaryRole="true" />
            </Roles>
            <GeoLoc>
                <Location>
                    <PostCode>AB12 3CD</PostCode>
                    <UPRN>123456789</UPRN>
                </Location>
            </GeoLoc>
        </Organisation>
        """
        element = etree.fromstring(xml)
        batch_yaml_data = {}

        # Call the function
        generate_organisation_yaml(element, 1, batch_yaml_data, self.yaml_output_path)

        # Construct the expected key
        expected_key = "Active/RO126"

        # Assert the result
        self.assertIn(expected_key, batch_yaml_data)
        self.assertEqual(batch_yaml_data[expected_key][0][1]["metadata"]["post_code"], "AB12 3CD")
        self.assertEqual(batch_yaml_data[expected_key][0][1]["metadata"]["uprn"], "123456789")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_organisation_yaml_missing_roles(self, mock_open, mock_makedirs):
        """Test generate_organisation_yaml with missing Roles."""
        xml = """
        <Organisation>
            <OrgId extension="ORG123" />
            <Status value="Active" />
        </Organisation>
        """
        element = etree.fromstring(xml)
        batch_yaml_data = {}

        # Call the function and assert it raises an error
        with self.assertRaises(SystemExit):
            generate_organisation_yaml(element, 1, batch_yaml_data, self.yaml_output_path)

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_process_batch_elements(self, mock_open, mock_makedirs):
        """Test the process_batch_elements function."""
        xml = """
        <Organisation>
            <OrgId extension="ORG123" />
            <Status value="Active" />
            <Roles>
                <Role id="RO126" primaryRole="true" />
            </Roles>
            <GeoLoc>
                <Location>
                    <PostCode>AB12 3CD</PostCode>
                    <UPRN>123456789</UPRN>
                </Location>
            </GeoLoc>
        </Organisation>
        """
        element = etree.fromstring(xml)
        batch = [element]
        org_count = 1

        # Call the function
        process_batch_elements(batch, org_count, self.yaml_output_path, self.json_output_path)

        # Debugging: Print mock calls
        print("Mock makedirs calls:", mock_makedirs.mock_calls)

        # Construct the expected directory paths
        expected_yaml_dir = os.path.join(self.yaml_output_path, "Active", "RO126")
        expected_json_dir = os.path.join(self.json_output_path, "Active", "RO126")

        # Assert that directories were created
        mock_makedirs.assert_any_call(expected_yaml_dir, exist_ok=True)
        mock_makedirs.assert_any_call(expected_json_dir, exist_ok=True)

        # Assert that files were opened for writing
        mock_open.assert_any_call(os.path.join(expected_yaml_dir, "ORG123.yaml"), "w")
        mock_open.assert_any_call(os.path.join(expected_json_dir, "ORG123.json"), "w")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_write_summary_files(self, mock_open, mock_makedirs):
        """Test the write_summary_files function."""
        global summary_data_global
        summary_data_global = {
            "ORG123": {
                "org_id": "ORG123",
                "status": "Active",
                "primary_role_id": "RO126",
                "post_code": "AB12 3CD",
                "uprn": "123456789",
            }
        }

        # Call the function
        write_summary_files(self.yaml_output_path, self.json_output_path)

        # Assert that directories were created
        mock_makedirs.assert_any_call(os.path.join(self.yaml_output_path, "Active", "RO126"), exist_ok=True)
        mock_makedirs.assert_any_call(os.path.join(self.json_output_path, "Active", "RO126"), exist_ok=True)

        # Assert that files were opened for writing
        mock_open.assert_any_call(os.path.join(self.yaml_output_path, "Active", "RO126", "summary.yaml"), "w")
        mock_open.assert_any_call(os.path.join(self.json_output_path, "Active", "RO126", "summary.json"), "w")

    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_write_summary_files_empty_data(self, mock_open, mock_makedirs):
        """Test write_summary_files with empty summary_data_global."""
        global summary_data_global
        summary_data_global = {}

        # Call the function
        write_summary_files(self.yaml_output_path, self.json_output_path)

        # Assert that no directories or files were created
        mock_makedirs.assert_not_called()
        mock_open.assert_not_called()

    @patch("importdata.batch_queue")
    def test_preload_batches(self, mock_batch_queue):
        """Test the preload_batches function."""
        xml = """
        <Organisations>
            <Organisation>
                <OrgId extension="ORG123" />
                <Status value="Active" />
                <Roles>
                    <Role id="RO126" primaryRole="true" />
                </Roles>
                <GeoLoc>
                    <Location>
                        <PostCode>AB12 3CD</PostCode>
                        <UPRN>123456789</UPRN>
                    </Location>
                </GeoLoc>
            </Organisation>
            <Organisation>
                <OrgId extension="ORG124" />
                <Status value="Inactive" />
                <Roles>
                    <Role id="RO127" primaryRole="true" />
                </Roles>
                <GeoLoc>
                    <Location>
                        <PostCode>CD34 5EF</PostCode>
                        <UPRN>987654321</UPRN>
                    </Location>
                </GeoLoc>
            </Organisation>
        </Organisations>
        """
        context = iter([
            ("end", etree.fromstring("<Organisation><OrgId extension='ORG123'/></Organisation>")),
            ("end", etree.fromstring("<Organisation><OrgId extension='ORG124'/></Organisation>")),
        ])

        # Call the function
        preload_batches(context, batch_size=1)

        # Assert that batches were added to the queue
        self.assertEqual(mock_batch_queue.put.call_count, 2)
        mock_batch_queue.put.assert_any_call(([etree.Element("Organisation")], 1))
        mock_batch_queue.put.assert_any_call(([etree.Element("Organisation")], 2))

    @patch("importdata.batch_queue")
    @patch("importdata.process_batch_elements")
    def test_process_batches_from_queue(self, mock_process_batch_elements, mock_batch_queue):
        """Test the process_batches_from_queue function."""
        # Mock the batch queue
        mock_batch_queue.get.side_effect = [
            ([etree.Element("Organisation")], 1),
            ([etree.Element("Organisation")], 2),
            StopIteration,  # Simulate empty queue
        ]

        # Call the function
        with self.assertRaises(StopIteration):
            process_batches_from_queue(self.yaml_output_path, self.json_output_path)

        # Assert that batches were processed
        self.assertEqual(mock_process_batch_elements.call_count, 2)

    @patch("importdata.etree.iterparse")
    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_main(self, mock_open, mock_makedirs, mock_iterparse):
        """Test the main function."""
        # Mock XML data
        xml = """
        <Organisation>
            <OrgId extension="ORG123" />
            <Status value="Active" />
            <Roles>
                <Role id="RO126" primaryRole="true" />
            </Roles>
            <GeoLoc>
                <Location>
                    <PostCode>AB12 3CD</PostCode>
                    <UPRN>123456789</UPRN>
                </Location>
            </GeoLoc>
        </Organisation>
        """
        # Mock the iterparse to simulate XML parsing
        mock_iterparse.return_value = iter([("end", etree.fromstring(xml))])

        # Call the main function
        main("test_data.xml", self.test_output_path, 1)

        # Construct the expected directory paths
        expected_yaml_dir = os.path.join(self.yaml_output_path, "Active", "RO126")
        expected_json_dir = os.path.join(self.json_output_path, "Active", "RO126")

        # Assert that directories were created
        mock_makedirs.assert_any_call(expected_yaml_dir, exist_ok=True)
        mock_makedirs.assert_any_call(expected_json_dir, exist_ok=True)

        # Assert that files were opened for writing
        mock_open.assert_any_call(os.path.join(expected_yaml_dir, "summary.yaml"), "w")
        mock_open.assert_any_call(os.path.join(expected_json_dir, "summary.json"), "w")

    @patch("importdata.etree.iterparse")
    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_main_invalid_xml(self, mock_open, mock_makedirs, mock_iterparse):
        """Test the main function with invalid XML."""
        # Mock invalid XML data
        mock_iterparse.side_effect = etree.XMLSyntaxError("Invalid XML", "<string>", 0, 0)

        # Call the main function and assert it raises an error
        with self.assertRaises(etree.XMLSyntaxError):
            main("invalid_data.xml", self.test_output_path, 1)

    @patch("importdata.psutil.Process")
    @patch("importdata.psutil.cpu_percent")
    def test_monitor_resources(self, mock_cpu_percent, mock_process):
        """Test the monitor_resources function."""
        from importdata import stop_flag  # Import stop_flag

        mock_cpu_percent.return_value = 50.0
        mock_process.return_value.memory_info.return_value.rss = 1048576  # 1 MB
        mock_process.return_value.io_counters.return_value.read_bytes = 2097152  # 2 MB
        mock_process.return_value.io_counters.return_value.write_bytes = 3145728  # 3 MB

        # Clear the stop_flag to allow the thread to run
        stop_flag.clear()

        # Run the monitor_resources function in a thread
        monitor_thread = threading.Thread(target=monitor_resources, args=(0.1,))
        monitor_thread.start()

        # Allow the thread to run for a short time
        threading.Event().wait(0.2)

        # Set the stop_flag to stop the thread
        stop_flag.set()

        # Wait for the thread to finish
        monitor_thread.join()

        # Assert that resource usage was logged
        mock_cpu_percent.assert_called()
        mock_process.assert_called()


if __name__ == "__main__":
    unittest.main()