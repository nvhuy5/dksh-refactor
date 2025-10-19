from typing import Optional
from pydantic import BaseModel
from enum import Enum

from models.class_models import FilePathRequest


class TrackingModel(BaseModel):
    """
    Traceability context for a file or workflow request.

    Attributes:
        request_id (str): Unique ID for tracing the request.
        file_path (str | None): File path, if applicable.
        project_name (str | None): Project associated with the file.
        source_name (str | None): Source of the file or data.
        workflow_id (str | None): Workflow ID.
        workflow_name (str | None): Workflow name.
        document_number (str | None): Optional document number.
        document_type (str | None): Type of document (e.g., MASTER_DATA, ORDER).
        sap_masterdata (bool | None): Flag indicating SAP master data.
        rerun_attempt (int | None): Retry attempt number, if any.
    """

    request_id: str
    file_path: str | None = None
    project_name: str | None = None
    source_name: str | None = None
    workflow_id: str | None = None
    workflow_name: str | None = None
    document_number: str | None = None
    document_type: str | None = None
    sap_masterdata: bool | None = None
    rerun_attempt: int | None = None

    @classmethod
    def from_data_request(cls, data: FilePathRequest) -> "TrackingModel":
        return cls(
            request_id=data.celery_id,
            file_path=data.file_path,
            project_name=data.project,
            source_name=data.source,
            rerun_attempt=data.rerun_attempt,
        )


class ServiceLog(str, Enum):
    """
    Enum representing the type of service or component emitting logs.
    Used to categorize logs based on their origin within the system.
    """

    API_GATEWAY = "api-gateway"
    DATABASE = "database"
    FILE_PROCESSOR = "file-processor"
    TASK_EXECUTION = "task-execution"
    NOTIFICATION = "notification-service"

    FILE_EXTRACTION = "file-extraction"
    METADATA_VALIDATION = "metadata-validation"
    DOCUMENT_PARSER = "document-parser"
    VALIDATION = "input-validation"
    MAPPING = "mapping"
    DATA_TRANSFORM = "data-transform"
    FILE_STORAGE = "file-storage"

    def __str__(self):
        return self.value


class LogType(str, Enum):
    """
    Enum representing the purpose of a log entry.
    """
    ACCESS = "access"
    TASK = "task"
    ERROR = "error"
    WARNING = "warning"

    def __str__(self):
        return self.value
