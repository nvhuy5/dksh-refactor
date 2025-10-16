import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace

from fastapi_celery.celery_worker.step_handler import execute_step
from fastapi_celery.models.class_models import WorkflowStep


# Dummy model to simulate method return values
class DummyResult:
    def __init__(self, data):
        self.data = data
    def model_dump(self):
        return {"data": self.data}


class TestExecuteStep(unittest.IsolatedAsyncioTestCase):
    """Test case for the execute_step function."""

    def setUp(self) -> None:
        self.mock_file_processor = MagicMock()
        self.mock_step = WorkflowStep(
            workflowStepId="step1",
            stepName="TEMPLATE_FILE_PARSE",
            stepOrder=1,
            stepConfiguration=[]
        )

        self.mock_file_processor.file_record = {
            "file_name": "test_file.pdf",
            "file_extension": ".pdf",
        }

        tracking_mock = MagicMock()
        tracking_mock.request_id = "test-request-id"
        tracking_mock.rerun_attempt = None
        self.mock_file_processor.tracking_model = tracking_mock

        self.mock_context = SimpleNamespace(
            request_id="test-request-id",
            workflow_detail=SimpleNamespace(
                filter_api=SimpleNamespace(
                    response=SimpleNamespace(
                        isMasterDataWorkflow=False,
                        folderName="folder_test",
                        customerFolderName="customer_test",
                    )
                )
            ),
            step_detail=[],
            is_done=False,
        )

    # ====== 1. Coroutine method no input ======
    @patch("fastapi_celery.celery_worker.step_handler.get_model_dump_if_possible", lambda r: r.model_dump() if hasattr(r, "model_dump") else r)
    @patch("fastapi_celery.celery_worker.step_handler.BEConnector")
    @patch("fastapi_celery.celery_worker.step_handler.set_context_values")
    @patch("fastapi_celery.celery_worker.step_handler.get_context_value")
    async def test_execute_step_with_coroutine_method_and_no_input(
        self, mock_get_context_value, mock_set_context_values, mock_be_connector
    ):
        mock_connector_instance = AsyncMock()
        mock_connector_instance.get.return_value = [{"templateFileParse": {"id": "mock-id"}}]
        mock_be_connector.return_value = mock_connector_instance

        mock_get_context_value.side_effect = lambda key: {
            "request_id": "test-request-id",
            "workflow_id": "wf1",
            "document_number": "doc1",
            "file_path": "/tmp/testfile.pdf",
            "document_type": "PO",
        }.get(key)
        mock_set_context_values.return_value = None

        self.mock_step.stepName = "TEMPLATE_FILE_PARSE"
        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)
        self.mock_file_processor.parse_file_to_json = AsyncMock(return_value=DummyResult("parsed_data"))

        with patch("fastapi_celery.celery_worker.step_handler.get_value",
                   lambda ctx, key: getattr(ctx, key, None) if hasattr(ctx, key) else None):
            result = await execute_step(self.mock_file_processor, self.mock_context, self.mock_step)

        self.assertIsInstance(result, DummyResult)
        self.assertEqual(result.data, "parsed_data")

    # ====== 2. Coroutine method with input ======
    @patch("fastapi_celery.celery_worker.step_handler.get_model_dump_if_possible", lambda r: r.model_dump() if hasattr(r, "model_dump") else r)
    @patch("fastapi_celery.celery_worker.step_handler.BEConnector")
    @patch("fastapi_celery.celery_worker.step_handler.set_context_values")
    @patch("fastapi_celery.celery_worker.step_handler.get_context_value")
    async def test_execute_step_with_coroutine_method_and_input(
        self, mock_get_context_value, mock_set_context_values, mock_be_connector
    ):
        mock_get_context_value.return_value = "some_input"
        mock_set_context_values.return_value = None

        mock_connector_instance = AsyncMock()
        mock_connector_instance.get.return_value = [{"templateFileParse": {"id": "mock-id"}}]
        mock_be_connector.return_value = mock_connector_instance

        self.mock_step.stepName = "TEMPLATE_FORMAT_VALIDATION"
        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)
        self.mock_file_processor.template_format_validation = AsyncMock(return_value=DummyResult("validated_data"))

        with patch("fastapi_celery.celery_worker.step_handler.get_value",
                   lambda ctx, key: getattr(ctx, key, None) if hasattr(ctx, key) else None):
            result = await execute_step(self.mock_file_processor, self.mock_context, self.mock_step)

        self.assertIsInstance(result, DummyResult)
        self.assertEqual(result.data, "validated_data")

    # ====== 3. Sync method with input ======
    @patch("fastapi_celery.celery_worker.step_handler.get_model_dump_if_possible", lambda r: r.model_dump() if hasattr(r, "model_dump") else r)
    @patch("fastapi_celery.celery_worker.step_handler.set_context_values")
    @patch("fastapi_celery.celery_worker.step_handler.get_context_value")
    async def test_execute_step_with_sync_method_and_input(
        self, mock_get_context_value, mock_set_context_values
    ):
        mock_get_context_value.side_effect = lambda key: {
            "request_id": "test-request-id",
            "workflow_id": "wf1",
            "document_number": "doc1",
            "file_path": "/tmp/testfile.pdf",
            "document_type": "PO",
        }.get(key)
        mock_set_context_values.return_value = None

        self.mock_step.stepName = "write_json_to_s3"
        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)
        self.mock_file_processor.write_json_to_s3 = MagicMock(return_value=DummyResult("written"))

        with patch("fastapi_celery.celery_worker.step_handler.get_value",
                   lambda ctx, key: getattr(ctx, key, None) if hasattr(ctx, key) else None):
            result = await execute_step(self.mock_file_processor, self.mock_context, self.mock_step)

        self.assertEqual(result.data, "written")

    # ====== 4. Missing method ======
    async def test_execute_step_with_missing_method(self):
        self.mock_step.stepName = "non_existent_method"
        setattr(self.mock_file_processor, "non_existent_method", None)

        result = await execute_step(self.mock_file_processor, self.mock_context, self.mock_step)
        self.assertEqual(result.step_status.value, "5")
        self.assertIn("not yet defined", result.step_failure_message[0])

    # ====== 5. Method raises exception ======
    @patch("fastapi_celery.celery_worker.step_handler.get_model_dump_if_possible", lambda r: r.model_dump() if hasattr(r, "model_dump") else r)
    async def test_execute_step_raises_exception_in_method(self):
        self.mock_step.stepName = "TEMPLATE_FILE_PARSE"
        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)
        self.mock_file_processor.parse_file_to_json = AsyncMock(side_effect=RuntimeError("Boom"))

        with patch("fastapi_celery.celery_worker.step_handler.get_value",
                   lambda ctx, key: getattr(ctx, key, None) if hasattr(ctx, key) else None):
            with self.assertRaises(RuntimeError) as ctx:
                await execute_step(self.mock_file_processor, self.mock_context, self.mock_step)

        self.assertIn("Boom", str(ctx.exception))
