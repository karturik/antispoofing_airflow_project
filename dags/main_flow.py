"""
main flow, import and run all dags
"""
from .create_det_tasks_dag import (
    create_det_tasks_main_flow
)

from .check_det_tasks_status_and_create_val_tasks_dag import (
    det_tasks_processing_and_create_val_tasks_main_flow
)

from .val_tasks_processing_dag import (
    check_val_tasks_status_main_flow
)


create_det_tasks_main_flow()
det_tasks_processing_and_create_val_tasks_main_flow()
check_val_tasks_status_main_flow()