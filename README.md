# Workflow Automation Using Apache Airflow

This project demonstrates how to set up and automate an **ETL (Extract, Transform, Load)** pipeline using **Apache Airflow** and **Docker**. It involves extracting data from a mock API, transforming it, and loading it into a **PostgreSQL** database. The project also showcases **failure detection** and **automatic retries** in Airflow.

## Prerequisites

Before setting up the project, ensure that you have the following installed:

- **Docker Desktop**: Used to run Airflow and PostgreSQL containers.
  - [Download Docker Desktop](https://www.docker.com/products/docker-desktop)
  
- **VS Code** ( for editing the project files):
  - [Download VS Code](https://code.visualstudio.com/)

- **Git** (optional, for version control):
  - [Download Git](https://git-scm.com/)

- **Python 3.8+**: Required for Airflow tasks inside the container.

## Setup Instructions

### 1. Docker and Airflow Setup

1. **Ensure Docker is Running**: Open Docker Desktop and make sure the Docker Engine is running.

2. **Docker Compose Configuration**: The project uses **Docker Compose** to orchestrate the multi-container setup. This setup includes:
   - Airflow webserver
   - Airflow scheduler
   - PostgreSQL database
   - Airflow initialization

3. **Airflow DAG**: The Airflow Directed Acyclic Graph (DAG) will define the workflow for the ETL pipeline. Tasks in the DAG include:
   - **Extract**: Fetches data from a mock API.
   - **Transform**: Cleans and structures the data.
   - **Load**: Loads the transformed data into PostgreSQL.

### 2. Build and Start Docker Containers

1. **Build the Containers**:
   - Navigate to your project folder and run the following command to build the Docker containers:
     docker-compose build

2. **Start the Containers**:
   - Once the build completes, run the following command to start the services in the background:
     docker-compose up -d

3. **Check the Container Status** (Optional):
   - To check the status of running containers, use the following command:
     docker ps

### 3. Access the Airflow UI

1. Open your browser and navigate to `http://localhost:8080`.

2. **Log in to the Airflow UI** with the default credentials:
   - **Username**: admin
   - **Password**: admin

3. On the Airflow UI home page, you should see the `etl_dags_postgres` DAG listed.

### 4. Force a Failure & Retry

1. To simulate a failure, modify the URL in the DAG’s `extract()` task to an incorrect one:
   - Change the API URL to: `https://jsonplaceholder.typicode.com/WRONG`.

2. **Save the Changes** and **Restart the Containers**:
   - Restart the webserver and scheduler services:
     docker-compose restart webserver scheduler

3. **Trigger the DAG**:
   - In the Airflow UI, trigger the DAG manually, and the **extract task** will fail (indicated by a red status).

4. **Verify Retry Behavior**:
   - Clear the task and allow Airflow to retry automatically. Airflow will retry the task based on the retry configuration defined in the DAG.

### 5. Monitor Task Execution

1. **Task Status**: In the Airflow UI, check the status of the tasks. The task will be shown as:
   - Green = Success
   - Red = Failed
   - Yellow = Running

2. **DAG Run History**: In the Airflow UI, navigate to **Browse > DAG Runs** to view the history of the DAG runs.

3. **Task Logs**: Click on any task (e.g., `extract`, `transform`, `load`) and check the logs for detailed execution information.

### 6. Set Up Auto-Scheduling (Optional)

1. Modify the `schedule_interval` in the DAG to run more frequently (e.g., every 2 minutes):
   ```yaml
   schedule_interval="*/2 * * * *"

2. Restart the Containers:

After modifying the schedule, restart the webserver and scheduler services:

docker-compose restart webserver scheduler


3. Verify Auto-Scheduled Runs:

In the Airflow UI, check Browse > DAG Runs. You should see the Run ID starting with scheduled__, indicating that the DAG runs automatically based on the defined schedule.