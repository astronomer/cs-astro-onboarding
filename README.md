Overview
========

Welcome to Astronomer! This project was generated with `astrocloud dev init` using the Astronomer CLI. This README describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astronomer project contains the following files and folders:

- `dags`: This folder contains the Python files for your Airflow DAGs.
- `Dockerfile`: This file contains a versioned Astronomer Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- `include`: This folder contains any additional files that you want to include as part of your project (empty by default).
- `packages.txt`: Install OS-level packages needed for your project by adding them to this file (empty by default).
- `requirements.txt`: Install Python packages needed for your project by adding them to this file (empty by default).
- `plugins`: Add custom or community plugins for your project to this file (empty by default).
- `airflow_settings.yaml`: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running `astrocloud dev start`.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for running Triggers and signaling tasks to resume when their conditions have been met. The Triggerer is used exclusively for tasks that are run with deferrable operators

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running `astrocloud dev start` will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/astro/deploy-code

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support team: https://support.astronomer.io/
