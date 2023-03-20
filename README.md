## AIRFLOW ANTISPOOFING PROJECT
___
### Project based on Airflow and Python for ETL process in Yandex-Toloka.

> Create project on toloka for collecting sets with video, photo and worker phone model
> 
> Create another project, where workers validate collected media from first pool
> 
> Accept or reject sets and download results

### Structure:
- dags
  - create_det_tasks_dag.py
    - check, how much new sets needed and create new tasks to collect
  - check_det_tasks_status_and_create_val_tasks_dag.py
    - collect assignments from finished detection tasks and create new validation tasks
  - val_tasks_processing_dag.py
    - third dag, check validation tasks for answers aggregate all answers accept or reject sets