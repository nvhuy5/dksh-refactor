import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi_celery.celery_worker import celery_task
from fastapi_celery.models.class_models import StepOutput, StatusEnum, DocumentType
from pydantic import BaseModel


# === task_execute success (giữ nguyên) ===
@patch("fastapi_celery.celery_worker.celery_task.handle_task", new_callable=AsyncMock)
def test_task_execute_success(mock_handle_task):
    fake_data = {"file_path": "dummy.xlsx", "project": "proj", "source": "src"}
    fake_tracking_model = MagicMock()
    fake_tracking_model.request_id = "req_1"

    with patch("fastapi_celery.celery_worker.celery_task.TrackingModel.from_data_request",
               return_value=fake_tracking_model):
        mock_handle_task.return_value = None
        result = celery_task.task_execute.run(fake_data)
        assert result == "Task completed"
        mock_handle_task.assert_awaited_once()


# === task_execute failure (sửa) ===
@patch("fastapi_celery.celery_worker.celery_task.handle_task", new_callable=AsyncMock)
def test_task_execute_failure(mock_handle_task):
    fake_data = {"file_path": "dummy.xlsx", "project": "proj", "source": "src"}
    fake_tracking_model = MagicMock()
    fake_tracking_model.request_id = "req_2"

    with patch("fastapi_celery.celery_worker.celery_task.TrackingModel.from_data_request",
               return_value=fake_tracking_model):
        mock_handle_task.side_effect = Exception("Boom!")

        # Patch asyncio.run để exception được raise ra ngoài
        with patch("asyncio.run", side_effect=lambda coro: coro.__await__().__next__()):
            celery_task.task_execute.run(fake_data)
            mock_handle_task.assert_awaited_once()


# === inject_metadata tests (giữ nguyên) ===
class DummyOutput(BaseModel):
    value: int


@pytest.mark.asyncio
async def test_inject_metadata_base_model():
    from fastapi_celery.celery_worker.celery_task import inject_metadata_into_step_output, ContextData

    step_result = StepOutput(
        output=DummyOutput(value=42),
        step_status=StatusEnum.SUCCESS,
        step_failure_message=[]
    )

    context_data = ContextData(request_id="req_3")
    context_data.step_detail = ["dummy_step_detail"]
    context_data.workflow_detail = "dummy_workflow_detail"

    inject_metadata_into_step_output(step_result, context_data, DocumentType.MASTER_DATA)
    assert hasattr(step_result.output, "step_detail")
    assert hasattr(step_result.output, "workflow_detail")


@pytest.mark.asyncio
async def test_inject_metadata_none_output():
    from fastapi_celery.celery_worker.celery_task import inject_metadata_into_step_output, ContextData

    step_result = StepOutput(
        output=None,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=[]
    )

    context_data = ContextData(request_id="req_4")
    context_data.step_detail = ["dummy_step_detail"]
    context_data.workflow_detail = "dummy_workflow_detail"

    inject_metadata_into_step_output(step_result, context_data, DocumentType.MASTER_DATA)
    assert step_result.output is None


# === handle_task success (sửa) ===
@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.execute_step", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.get_workflow_filter", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_session_start", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_session_finish", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_step_start", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.call_workflow_step_finish", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.ProcessorBase")
@patch("fastapi_celery.celery_worker.celery_task.RedisConnector")
async def test_handle_task_success(
    mock_redis,
    mock_processor,
    mock_step_finish,
    mock_step_start,
    mock_session_finish,
    mock_session_start,
    mock_get_workflow,
    mock_execute_step
):
    # === Fake tracking model ===
    fake_tracking_model = MagicMock()
    fake_tracking_model.request_id = "req_5"
    fake_tracking_model.project_name = "proj"
    fake_tracking_model.source_name = "src"
    fake_tracking_model.rerun_attempt = None

    # === Fake ProcessorBase ===
    fake_processor = MagicMock()
    fake_processor.document_type = DocumentType.ORDER.value
    fake_processor.run.return_value = None
    fake_processor.file_record = {
        "file_name": "dummy.xlsx",
        "file_extension": ".xlsx"
    }
    mock_processor.return_value = fake_processor

    # === Fake workflow step ===
    mock_step = MagicMock()
    mock_step.stepName = "STEP1"
    mock_step.workflowStepId = "step_1"
    mock_step.stepOrder = 0
    mock_get_workflow.return_value.workflowSteps = [mock_step]

    # === Fake execute_step output ===
    class FakeOutput:
        def model_copy(self, update=None):
            return self

    mock_execute_step.return_value = StepOutput(
        output=FakeOutput(),
        step_status=StatusEnum.SUCCESS,
        step_failure_message=[]
    )

    # === Fake session/step calls ===
    mock_session_start.return_value = MagicMock()
    mock_session_finish.return_value = MagicMock()
    mock_step_start.return_value = MagicMock()
    mock_step_finish.return_value = MagicMock()

    # === Patch ContextData để có s3_key_prefix ===
    from fastapi_celery.celery_worker.celery_task import ContextData
    context_data_instance = ContextData(request_id=fake_tracking_model.request_id)
    context_data_instance.s3_key_prefix = "test_prefix"
    ContextData_orig = celery_task.ContextData
    celery_task.ContextData = MagicMock(return_value=context_data_instance)
    # Force is_done property
    ContextData.is_done = property(lambda self: False)

    # === Call handle_task ===
    result = await celery_task.handle_task(fake_tracking_model)

    # === Assertions ===
    assert hasattr(result, "request_id")
    mock_execute_step.assert_awaited_once()
    mock_processor.assert_called_once_with(fake_tracking_model)
    mock_redis.assert_called_once()

    # === Restore original ContextData if needed ===
    celery_task.ContextData = ContextData_orig