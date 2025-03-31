import unittest
from unittest.mock import patch, MagicMock, mock_open, call
from lxml import etree
import os
import shutil
import yaml
import json

# Import functions from the script
from importdata import (
    process_element,
    generate_organisation_yaml,
    process_batch_elements,
    write_summary_files,
    preload_batches,
    process_batches_from_queue,
    main,
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

        # Assert the result
        self.assertIn("Active", batch_yaml_data)
        self.assertIn("RO126", batch_yaml_data["Active"])
        self.assertEqual(batch_yaml_data["Active"]["RO126"][0][1]["metadata"]["post_code"], "AB12 3CD")
        self.assertEqual(batch_yaml_data["Active"]["RO126"][0][1]["metadata"]["uprn"], "123456789")

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

        # Assert that directories were created
        mock_makedirs.assert_any_call(os.path.join(self.yaml_output_path, "Active", "RO126"), exist_ok=True)
        mock_makedirs.assert_any_call(os.path.join(self.json_output_path, "Active", "RO126"), exist_ok=True)

        # Assert that files were opened for writing
        mock_open.assert_any_call(os.path.join(self.yaml_output_path, "Active", "RO126", "ORG123.yaml"), "w")
        mock_open.assert_any_call(os.path.join(self.json_output_path, "Active", "RO126", "ORG123.json"), "w")

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

    @patch("importdata.etree.iterparse")
    @patch("os.makedirs")
    @patch("builtins.open", new_callable=mock_open)
    def test_main(self, mock_open, mock_makedirs, mock_iterparse):
        """Test the main function."""
        # Mock XML data
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
        </Organisations>
        """
        mock_iterparse.return_value = iter([("end", etree.fromstring(xml))])

        # Call the main function
        main("test_data.xml", self.test_output_path, 1000)

        # Assert that directories were created
        mock_makedirs.assert_any_call(os.path.join(self.yaml_output_path, "Active", "RO126"), exist_ok=True)
        mock_makedirs.assert_any_call(os.path.join(self.json_output_path, "Active", "RO126"), exist_ok=True)

        # Assert that files were opened for writing
        mock_open.assert_any_call(os.path.join(self.yaml_output_path, "Active", "RO126", "summary.yaml"), "w")
        mock_open.assert_any_call(os.path.join(self.json_output_path, "Active", "RO126", "summary.json"), "w")


if __name__ == "__main__":
    unittest.main()