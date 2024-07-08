import pytest
from unittest.mock import MagicMock, Mock
from dagster import build_op_context, resource
from dagster_etl.resources import StorageAccountIoManager
from dagster_etl.assets.staging import silversea
import pandas as pd
import os


# Create a mock resource definition for StorageAccountIoManager
@resource
def mock_storage_account_io_manager(_):
    mock_io_manager = Mock(spec=StorageAccountIoManager)

    # Read JSON data from file
    file_path = os.path.join(
        os.path.dirname(__file__), "examples", "ship_page_data.json"
    )
    with open(file_path, "r") as file:
        json_data = file.read()

    mock_io_manager.download_blob = MagicMock(return_value=json_data)
    return mock_io_manager


@pytest.fixture
def mock_context():
    partition_date = "2024-05-20"
    context = build_op_context(partition_key=partition_date)
    return context


def test_silversea_ships(mock_context):
    # Call the silversea_ships function
    result = silversea.silversea_ships(mock_context, mock_storage_account_io_manager)

    pandas_df = pd.DataFrame(
        {
            "name": [
                "Silver Cloud",
                "Silver Dawn",
                "Silver Endeavour",
                "Silver Moon",
                "Silver Muse",
                "Silver Nova",
                "Silver Origin",
                "Silver Ray",
                "Silver Shadow",
                "Silver Spirit",
                "Silver Whisper",
                "Silver Wind",
            ],
            "type": [
                "Expedition",
                "Classic",
                "Expedition",
                "Classic",
                "Classic",
                "Classic",
                "Expedition",
                "Classic",
                "Classic",
                "Classic",
                "Classic",
                "Expedition",
            ]
        }
    )

    # Assert that the "ships" element exists in the result
    pd.testing.assert_frame_equal(result, pandas_df, check_dtype=False)
