import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from xml.etree.ElementTree import Element
from fastapi_celery.processors.file_processors.xml_processor import (
    XMLProcessor,
)
from fastapi_celery.models.class_models import SourceType, PODataParsed
from fastapi_celery.models.tracking_models import TrackingModel


class TestXMLProcessor(unittest.TestCase):

    def setUp(self):
        self.dummy_path = Path("dummy.xml")
        self.xml_content = """
        <Invoice>
            <Header>
                <Number>PO123456</Number>
                <Date>2025-07-10</Date>
            </Header>
            <Items>
                <Item>
                    <Name>Widget A</Name>
                    <Quantity>10</Quantity>
                </Item>
            </Items>
        </Invoice>
        """
        self.expected_parsed_dict = {
            "Header": {"Number": "PO123456", "Date": "2025-07-10"},
            "Items": {"Item": {"Name": "Widget A", "Quantity": "10"}},
        }
        self.tracking_model = TrackingModel(
            request_id="req-001",
            file_path=str(self.dummy_path),
            project_name="unittest",
            source_name="local",
        )

    @patch(
        "fastapi_celery.processors.file_processors.xml_processor"
        ".ext_extraction.FileExtensionProcessor"
    )
    def test_extract_text_local_file(self, mock_processor):
        mock_file = MagicMock()
        mock_file.source = "local"
        mock_file.file_path = self.dummy_path
        mock_file._get_file_capacity.return_value = "small"
        mock_file._get_document_type.return_value = "invoice"

        mock_processor.return_value = mock_file

        # Simulate reading local file
        with patch(
            "builtins.open", unittest.mock.mock_open(read_data=self.xml_content)
        ):
            processor = XMLProcessor(tracking_model=self.tracking_model)
            text = processor.extract_text()
            self.assertIn("<Invoice>", text)

    def test_parse_element_and_find_po(self):
        root = Element("Root")
        header = Element("Header")
        number = Element("Number")
        number.text = "PO999888"
        header.append(number)
        root.append(header)

        processor = XMLProcessor(tracking_model=self.tracking_model)

        parsed = processor.parse_element(root)
        self.assertEqual(parsed, {"Header": {"Number": "PO999888"}})

        po = processor.find_po_in_xml(root)
        self.assertEqual(po, "PO999888")

    @patch(
        "fastapi_celery.processors.file_processors.xml_processor"
        ".ext_extraction.FileExtensionProcessor"
    )
    def test_parse_file_to_json(self, mock_processor):
        # Mock the file processor behavior
        mock_file = MagicMock()
        mock_file.source = "local"
        mock_file.file_path = self.dummy_path
        mock_file._get_file_capacity.return_value = "medium"
        mock_file._get_document_type.return_value = "order"
        mock_processor.return_value = mock_file

        with patch(
            "fastapi_celery.processors.file_processors.xml_processor.PODataParsed"
        ) as MockPODataParsed:
            dummy_parsed = MagicMock()
            MockPODataParsed.return_value = dummy_parsed

            with patch("builtins.open", unittest.mock.mock_open(read_data=self.xml_content)):
                processor = XMLProcessor(tracking_model=self.tracking_model)
                result = processor.parse_file_to_json()

                MockPODataParsed.assert_called_once()
                called_args = MockPODataParsed.call_args.kwargs

                self.assertEqual(called_args["po_number"], "PO123456")
                self.assertEqual(called_args["document_type"], "order")
                self.assertEqual(called_args["items"]["Header"]["Number"], "PO123456")
                self.assertEqual(result, dummy_parsed)
