# tests/test_master_validation.py
import pytest
import types
import pandas as pd
from fastapi_celery.processors.workflow_processors.master_validation import MasterValidation
from fastapi_celery.models.class_models import MasterDataParsed, StatusEnum, StepOutput

# ====== Fixtures ======
@pytest.fixture
def masterdata_json():
    data = {
        "ID": ["1", "2"],
        "Name": ["Alice", "Bob"],
        "Date": ["20250101", "20250201"]
    }
    headers = ["ID", "Name", "Date"]
    return MasterDataParsed(
        original_file_path="dummy_path",
        headers=headers,
        document_type="master_data",
        items=data,
        step_status=StatusEnum.SUCCESS,
        messages=[],
        capacity="1KB",
    )

@pytest.fixture
def master_validation(masterdata_json):
    mv = MasterValidation(masterdata_json)
    mv.tracking_model = types.SimpleNamespace(file_path="dummy_path")
    return mv

# ====== Header validation tests ======
def test_header_validation_success(master_validation):
    header_ref = [{"name": "ID", "posidx": 0}, {"name": "Name", "posidx": 1}, {"name": "Date", "posidx": 2}]
    result = master_validation.header_validation(header_ref)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None

def test_header_validation_fail(master_validation):
    header_ref = [{"name": "ID", "posidx": 0}, {"name": "FullName", "posidx": 1}, {"name": "Date", "posidx": 2}]
    result = master_validation.header_validation(header_ref)
    assert result.step_status == StatusEnum.FAILED
    assert any("Mismatch at position" in m for m in result.messages)

# ====== Data validation tests ======
def test_data_validation_success(master_validation):
    data_ref = [
        {"name": "ID", "datatype": "int", "nullable": False},
        {"name": "Name", "datatype": "string", "nullable": False, "maxlength": 10},
        {"name": "Date", "datatype": "timestamp", "nullable": False},
    ]
    result = master_validation.data_validation(data_ref)
    assert result.step_status == StatusEnum.SUCCESS

def test_data_validation_missing_column(master_validation):
    data_ref = [
        {"name": "ID", "datatype": "int"},
        {"name": "NonExist", "datatype": "string"},
    ]
    result = master_validation.data_validation(data_ref)
    assert result.step_status == StatusEnum.FAILED
    assert "Missing column" in result.messages[0]

def test_data_validation_invalid_type(master_validation):
    master_validation.masterdata["ID"] = ["1", "x"]  # second row invalid int
    data_ref = [{"name": "ID", "datatype": "int"}]
    result = master_validation.data_validation(data_ref)
    assert result.step_status == StatusEnum.FAILED
    assert "invalid value" in result.messages[0]

# ====== Async masterdata header/data validation ======
import asyncio
from fastapi_celery.processors.workflow_processors.master_validation import masterdata_header_validation, masterdata_data_validation
from unittest.mock import patch, AsyncMock

class DummySelf:
    file_record = {"file_name": "dummy.txt"}

@pytest.mark.asyncio
async def test_masterdata_header_validation_success(masterdata_json):
    input_data = StepOutput(output=masterdata_json, step_status=StatusEnum.SUCCESS, step_failure_message=None)
    dummy_self = DummySelf()

    async_mock = AsyncMock(return_value=[{"name": "ID", "posidx": 0}, {"name": "Name", "posidx": 1}, {"name": "Date", "posidx": 2}])

    with patch("fastapi_celery.processors.workflow_processors.master_validation.BEConnector") as mock_conn, \
         patch("fastapi_celery.processors.workflow_processors.master_validation.MasterValidation") as mock_mv:
        mock_conn.return_value.get = async_mock

        # Mock MasterValidation instance, gán tracking_model
        instance = mock_mv.return_value
        instance.masterdata_json = masterdata_json
        instance.tracking_model = types.SimpleNamespace(file_path="dummy_path")
        instance.header_validation.return_value = masterdata_json

        result = await masterdata_header_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.SUCCESS

@pytest.mark.asyncio
async def test_masterdata_header_validation_fail(masterdata_json):
    input_data = StepOutput(output=masterdata_json, step_status=StatusEnum.SUCCESS, step_failure_message=None)
    dummy_self = DummySelf()

    async_mock = AsyncMock(return_value=[{"name": "ID", "posidx": 0}, {"name": "FullName", "posidx": 1}, {"name": "Date", "posidx": 2}])

    with patch("fastapi_celery.processors.workflow_processors.master_validation.BEConnector") as mock_conn, \
         patch("fastapi_celery.processors.workflow_processors.master_validation.MasterValidation") as mock_mv:
        mock_conn.return_value.get = async_mock

        instance = mock_mv.return_value
        instance.masterdata_json = masterdata_json
        instance.tracking_model = types.SimpleNamespace(file_path="dummy_path")
        instance.header_validation.return_value = masterdata_json.model_copy(update={"step_status": StatusEnum.FAILED})

        result = await masterdata_header_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.FAILED

@pytest.mark.asyncio
async def test_masterdata_data_validation_success(masterdata_json):
    input_data = StepOutput(output=masterdata_json, step_status=StatusEnum.SUCCESS, step_failure_message=None)
    dummy_self = DummySelf()

    async_mock = AsyncMock(return_value=[
        {"name": "ID", "datatype": "int", "nullable": False},
        {"name": "Name", "datatype": "string", "nullable": False, "maxlength": 10},
        {"name": "Date", "datatype": "timestamp", "nullable": False},
    ])

    with patch("fastapi_celery.processors.workflow_processors.master_validation.BEConnector") as mock_conn, \
         patch("fastapi_celery.processors.workflow_processors.master_validation.MasterValidation") as mock_mv:
        mock_conn.return_value.get = async_mock

        # gán tracking_model cho instance
        instance = mock_mv.return_value
        instance.masterdata_json = masterdata_json
        instance.tracking_model = types.SimpleNamespace(file_path="dummy_path")
        instance.data_validation.return_value = masterdata_json

        result = await masterdata_data_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.SUCCESS

@pytest.mark.asyncio
async def test_masterdata_data_validation_fail(masterdata_json):
    masterdata_json.items["ID"] = ["1", "x"]
    input_data = StepOutput(output=masterdata_json, step_status=StatusEnum.SUCCESS, step_failure_message=None)
    dummy_self = DummySelf()

    async_mock = AsyncMock(return_value=[
        {"name": "ID", "datatype": "int", "nullable": False}
    ])

    with patch("fastapi_celery.processors.workflow_processors.master_validation.BEConnector") as mock_conn, \
         patch("fastapi_celery.processors.workflow_processors.master_validation.MasterValidation") as mock_mv:
        mock_conn.return_value.get = async_mock

        instance = mock_mv.return_value
        instance.masterdata_json = masterdata_json
        instance.tracking_model = types.SimpleNamespace(file_path="dummy_path")
        instance.data_validation.return_value = masterdata_json.model_copy(update={"step_status": StatusEnum.FAILED})

        result = await masterdata_data_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.FAILED
