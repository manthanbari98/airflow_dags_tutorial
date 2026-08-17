# Apache Airflow DAGs Tutorial & Practice


## 📌 Overview


This repository contains hands-on Apache Airflow DAGs created while learning and practicing workflow orchestration and scheduling.


The project covers fundamental as well as intermediate Airflow concepts, including DAG creation, operators, task dependencies, XComs, branching, scheduling, incremental processing, assets, and event-driven workflows.


The purpose of this repository is to demonstrate practical experience with Apache Airflow and build a strong foundation in data pipeline orchestration.


---


## 🛠️ Technologies Used


- **Apache Airflow**
- **Python**
- **Docker**
- **AWS S3**
- **AWS EventBridge**
- **Git & GitHub**
- **uv** for Python environment and dependency management


---


## 📂 Project Structure


```text
airflow_dags_tutorial/
│
├── dags/
│   │
│   ├── 1_first_dag.py
│   ├── 2_dag_versioning.py
│   ├── 3_operators.py
│   ├── 4_xcoms_auto.py
│   ├── 5_xcoms_kwargs.py
│   ├── 6_parallel_dags.py
│   ├── 7_condition_branch_dags.py
│   ├── 8_schedule_dag.py
│   ├── 9_cron_schedule.py
│   ├── 9_cron_schedule copy.py
│   ├── 10_delta_schedule_dag.py
│   ├── 11_interval_incremental_dag.py
│   ├── 12_special_dates_dag.py
│   ├── 14_asset_dependency.py
│   │
│   ├── assets_dag.py
│   ├── dag_orchestrator_1.py
│   ├── dag_orchestrator_2.py
│   └── orchestratordag_parent.py
│
├── screenshots/
│
├── docker-compose.yaml
├── main.py
├── pyproject.toml
├── uv.lock
├── .python-version
├── .gitignore
└── README.md
```
## 📚 Topics Covered
### 1. DAG Creation

Learned how to create basic Airflow DAGs and define tasks and dependencies.
```
DAG
 ├── Task 1
 ├── Task 2
 └── Task 3
```
### 2. DAG Versioning

Practiced managing and organizing different DAG implementations and versions.

### 3. Airflow Operators

Practiced working with different operators to execute tasks within DAGs.

Examples include:

- Python-based tasks
- Bash-based tasks
- Task execution and dependencies
### 4. XComs

Practiced passing information between Airflow tasks using XCom.

Covered:

- Automatic XCom handling
- Passing task results
- Using kwargs
- Retrieving values from upstream tasks

Example:
```
Task A
  │
  │ XCom
  ▼
Task B
```
### 5. Parallel DAG Execution

Practiced creating workflows where multiple tasks can execute independently or in parallel.
```
        ┌── Task A ──┐
Start ──┼── Task B ──┼── End
        └── Task C ──┘
```
### 6. Conditional Branching

Practiced using branching logic to determine which task should execute based on conditions.
```
             ┌── Path A
Start ───────┤
             └── Path B

```
### 7. Scheduling

Practiced different Airflow scheduling concepts, including:

- Cron schedules
- Timed schedules
- Data intervals
- Delta-based scheduling
- Special date-based scheduling
### 8. Incremental Processing

Practiced concepts related to incremental data processing using Airflow scheduling and data intervals.

### 9. Airflow Assets

Practiced Airflow's asset-based workflow dependencies.

Assets allow downstream tasks or DAGs to be triggered based on the availability or update of upstream data.

### 10. DAG Dependencies & Orchestration

Practiced creating dependencies between DAGs and orchestrating multiple workflows.

Example:
```
DAG 1
  │
  ▼
DAG 2
  │
  ▼
DAG 3
```
## ⚙️ Running Airflow Locally

This project uses Docker to run Apache Airflow locally.

### 1. Clone the repository
```
git clone https://github.com/manthanbari98/airflow_dags_tutorial.git
```
```
cd airflow_dags_tutorial
```
### 2. Start Airflow
```
docker compose up -d
```
### 3. Check running containers
```
docker compose ps
```
### 4. Access Airflow

Open the Airflow web interface in your browser using the local Airflow URL configured by your Docker Compose setup.

## 🧪 Practice DAGs
| DAG                              | Concept                        |
| -------------------------------- | ------------------------------ |
| `1_first_dag.py`                 | First Airflow DAG              |
| `2_dag_versioning.py`            | DAG versioning                 |
| `3_operators.py`                 | Airflow operators              |
| `4_xcoms_auto.py`                | Automatic XCom                 |
| `5_xcoms_kwargs.py`              | XCom with kwargs               |
| `6_parallel_dags.py`             | Parallel task execution        |
| `7_condition_branch_dags.py`     | Conditional branching          |
| `8_schedule_dag.py`              | DAG scheduling                 |
| `9_cron_schedule.py`             | Cron scheduling                |
| `9_cron_schedule copy.py`        | Cron scheduling practice       |
| `10_delta_schedule_dag.py`       | Delta/data interval scheduling |
| `11_interval_incremental_dag.py` | Incremental processing         |
| `12_special_dates_dag.py`        | Special date scheduling        |
| `14_asset_dependency.py`         | Asset dependencies             |
| `assets_dag.py`                  | Airflow Assets                 |
| `dag_orchestrator_1.py`          | DAG orchestration              |
| `dag_orchestrator_2.py`          | DAG orchestration              |
| `orchestratordag_parent.py`      | Parent DAG orchestration       |


## 🐳 Docker

Docker is used to run the Airflow environment locally.

The project includes:
```
docker-compose.yaml
```
This makes it easier to reproduce the Airflow development environment.

## 🔐 Security

Sensitive configuration files and generated files are intentionally excluded from the repository.

The `.gitignore` file excludes items such as:
```
.env
logs/
__pycache__/
*.pyc
airflow.db
config/airflow.cfg
```
AWS credentials, passwords, API tokens, and other secrets should never be committed to GitHub.

## 🎯 Key Learnings

Through this project, I gained hands-on experience with:

- Creating and managing Airflow DAGs
- Defining task dependencies
- Using Airflow operators
- Passing data between tasks using XCom
- Running tasks in parallel
- Implementing conditional branching
- Working with cron and time-based schedules
- Understanding data intervals
- Building incremental workflows
- Working with Airflow Assets
- Orchestrating workflows
- Running Airflow using Docker
- Understanding event-driven data pipelines


## 👨‍💻 Author

Manthan Bari


## Skills:

SQL • Python • PySpark • Apache Spark • Databricks • Apache Airflow • dbt • Power BI

## 📄 Disclaimer

This repository is a hands-on learning and practice project created while studying Apache Airflow and workflow orchestration concepts.