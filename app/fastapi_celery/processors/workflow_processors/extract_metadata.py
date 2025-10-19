import logging
import traceback
from datetime import datetime, timezone
from utils import file_extraction, log_helper
from models.tracking_models import ServiceLog, LogType
from models.class_models import StatusEnum, StepOutput

# === Logging setup ===
logger_name = f"Workflow Processor - {__name__}"
log_helper.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)
logger = log_helper.ValidatingLoggerAdapter(base_logger, {})
# ===


def extract_metadata(self) -> StepOutput:
    """
    Extract file metadata using FileExtensionProcessor.

    This function initializes FileExtensionProcessor to retrieve and validate
    file metadata such as document type, file name, and bucket information.

    Logs success or failure details. In case of error, it logs the full traceback
    and re-raises the exception to be handled by the upstream caller (e.g. Celery task).

    Returns:
        StepOutput: SUCCESS with True output if extraction succeeds.
                    FAILED with error details if extraction fails.
    Raises:
        Exception: Propagates any exception for higher-level retry or handling.
    """
    try:
        file_processor = file_extraction.FileExtensionProcessor(self.tracking_model)

        self.file_record = {
            "file_path": file_processor.file_path,
            "file_path_parent": file_processor.file_path_parent,
            "source_type": file_processor.source_type,
            "file_size": file_processor.file_size,
            "file_name": file_processor.file_name,
            "file_name_wo_ext": file_processor.file_name_wo_ext,
            "file_extension": file_processor.file_extension,
            "document_type": file_processor.document_type,
            "raw_bucket_name": file_processor.raw_bucket_name,
            "target_bucket_name": file_processor.target_bucket_name,
            "proceed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }

        logger.info(
            f"[{self.tracking_model.request_id}] Metadata extracted for file: {file_processor.file_path}",
            extra={
                "service": ServiceLog.FILE_EXTRACTION,
                "log_type": LogType.TASK,
                "data": self.file_record,
            },
        )

        return StepOutput(
            output=True,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:
        # Capture full traceback for ELK/Kibana
        full_tb = traceback.format_exception(type(e), e, e.__traceback__)
        error_type = type(e).__name__

        logger.error(
            f"[{self.tracking_model.request_id}] Error in extract_metadata: {error_type}: {e}",
            extra={
                "service": ServiceLog.FILE_EXTRACTION,
                "log_type": LogType.ERROR,
                "data": self.tracking_model.model_dump(),
                "traceback": full_tb,  # structured traceback for ELK
            },
        )

        # Raise again for Celery to catch and handle retry or fail
        raise
