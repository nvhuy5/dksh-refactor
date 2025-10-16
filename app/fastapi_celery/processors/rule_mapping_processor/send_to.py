import logging
from utils import log_helpers
from models.class_models import GenericStepResult, StatusEnum, StepOutput

# ===
# Set up logging
logger_name = f"Workflow Processor - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


def send_to(self,input_data: StepOutput) -> StepOutput:

    failed_output = input_data.output.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": ["Processor 'SEND_TO' is not define"]}
        )
    return StepOutput(
            output=failed_output,
            step_status=StatusEnum.FAILED,
            step_failure_message=["Processor 'SEND_TO' is not define"],
        )
