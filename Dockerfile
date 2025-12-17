# Use an official Airflow image as the base
FROM apache/airflow:2.3.0-python3.8

# Switch to the root user to install additional packages
USER root

# Update the package list and install Python package manager
RUN apt-get update && apt-get install -y python3-pip

# Install Airflow using pip (Python package manager)
RUN pip install apache-airflow
