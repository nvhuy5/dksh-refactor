from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from pydantic import BaseModel
from fastapi_celery.models.class_models import StepOutput
from fastapi_celery.celery_worker.step_handler import (
    ALL_DEFINITIONS,
    build_s3_key_prefix,
    execute_step,
    extract,
    extract_to_wrapper,
    get_context_api,
    get_model_dump_if_possible,
    raise_if_failed,
    get_value,
    resolve_args,
)
from fastapi_celery.celery_worker.step_handler import StepDefinition, StatusEnum


# === get_model_dump_if_possible ===
def test_get_model_dump_if_possible_with_base_model():
    class DummyOutput(BaseModel):
        value: int

    class DummyStepOutput(BaseModel):
        output: DummyOutput

    obj = DummyStepOutput(output=DummyOutput(value=42))
    result = get_model_dump_if_possible(obj)
    assert result == {"value": 42}


def test_get_model_dump_if_possible_non_model():
    obj = {"output": "abc"}
    result = get_model_dump_if_possible(obj)
    assert result == obj


# === raise_if_failed ===
def test_raise_if_failed_success():
    class DummyResult(BaseModel):
        step_status: StatusEnum
        step_failure_message: list[str] | None = None

    result = DummyResult(step_status=StatusEnum.SUCCESS)
    # Không raise error
    raise_if_failed(result, "dummy_step")


def test_raise_if_failed_failed():
    class DummyResult(BaseModel):
        step_status: StatusEnum
        step_failure_message: list[str] | None = None

    result = DummyResult(step_status=StatusEnum.FAILED, step_failure_message=["error"])
    with pytest.raises(RuntimeError) as exc:
        raise_if_failed(result, "dummy_step")
    assert "dummy_step" in str(exc.value)
    assert "error" in str(exc.value)


# === get_value ===
def test_get_value_base_model_and_dict():
    class DummyCtx(BaseModel):
        foo: str = "bar"

    ctx_model = DummyCtx()
    ctx_dict = {"foo": "bar"}

    # Với BaseModel
    assert get_value(ctx_model, "foo") == "bar"
    assert get_value(ctx_model, "missing") is None

    # Với dict
    assert get_value(ctx_dict, "foo") == "bar"
    assert get_value(ctx_dict, "missing") is None


# === resolve_args ===
def test_resolve_args_with_args_and_kwargs():
    context = {"a": 1, "b": 2}

    step_def = StepDefinition(
        function_name="dummy_function",  # Bắt buộc trong Pydantic 2
        args=["a", "b"],
        kwargs={"x": "a", "y": 2},
    )

    args, kwargs = resolve_args(step_def, context, "dummy_step")
    # Positional args
    assert args == [1, 2]
    # Keyword args
    assert kwargs == {"x": 1, "y": 2}
    # input_data được gán trong context
    assert context["input_data"] == [1, 2]


# === extract / extract_to_wrapper ===
def test_extract_and_wrapper():
    ctx = {}
    result = {"foo": "bar"}
    extract(ctx, result, "ctx_key", "foo")
    assert ctx["ctx_key"] == "bar"

    # Test wrapper handles exceptions
    def fail_func(c, r, k, rk):
        raise ValueError("boom")

    wrapped = extract_to_wrapper(fail_func)
    ctx2 = {}
    wrapped(ctx2, {}, "key", "rk")
    assert ctx2["key"] is None


# === build_s3_key_prefix ===
def test_build_s3_key_prefix(monkeypatch):
    processor = MagicMock()
    processor.file_record = {"file_name": "file.xlsx"}
    processor.tracking_model = MagicMock()
    context_data = MagicMock()
    step = MagicMock()
    step.stepName = "STEP1"
    step.stepOrder = 1
    step_config = MagicMock()
    step_config.target_store_data = "target"
    filter_api = MagicMock()
    filter_api.response.isMasterDataWorkflow = True
    context_data.workflow_detail.filter_api = filter_api

    prefix = build_s3_key_prefix(processor, context_data, step, step_config)
    assert prefix.startswith("target/file")


# === get_context_api ===
def test_get_context_api_returns_list():
    result = get_context_api("FILE_PARSE", {})
    assert isinstance(result, list)


# === execute_step ===
@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.step_handler.get_context_api")
@patch("fastapi_celery.celery_worker.step_handler.BEConnector")
async def test_execute_step_basic(mock_be_connector, mock_get_context_api):
    # Setup mocks
    file_processor = MagicMock()
    file_processor.file_record = {"file_name": "dummy.xlsx", "file_extension": ".xlsx"}
    file_processor.workflow_step_ids = {}
    file_processor.tracking_model = MagicMock()

    step = MagicMock()
    step.stepName = "STEP1"
    step.workflowStepId = "step_1"
    step.stepOrder = 0

    context_data = MagicMock()
    context_data.request_id = "req_1"
    context_data.step_detail = []
    context_data.workflow_detail.filter_api.response.isMasterDataWorkflow = True

    step_def = MagicMock()
    step_def.require_data_output = True
    step_def.function_name = "func"
    step_def.data_input = None
    step_def.data_output = None
    step_def.extract_to = {}

    # Patch ALL_DEFINITIONS
    ALL_DEFINITIONS["STEP1"] = step_def
    # Patch processor method
    file_processor.func = AsyncMock(
        return_value=StepOutput(
            step_status=StatusEnum.SUCCESS, step_failure_message=[], output=None
        )
    )
    step_def.function_name = "func"

    # Patch get_context_api to return None
    mock_get_context_api.return_value = None

    result = await execute_step(file_processor, context_data, step)
    assert isinstance(result, StepOutput)
