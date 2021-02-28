True Film Data Engineer Challenge
======================
Repo for True Layer Data Engineer Challenge

Language & Framework/Tools Used
======================
### **Languages**
* Python 3.7 (Anaconda)
### **Frameworks/Tools**
* Docker
* Airflow v1.10
* Dbeaver
* VS Code

Setup Overview
======================
* Airflow has been used for scheduling and automating.
* Airflow is running with Local Executor
* Postgres Database (Two instances) have been used , one for Airflow metadata DB and other one for saving output.
* Both Airflow and Postgres instances are run in docker containers using docker compose.

How to Run
======================
### Pre Requisites:
* Docker/Docker Desktop needs to be installed on the running machine.
	- Windows
		- https://docs.docker.com/docker-for-windows/install/
	- Mac
		- https://docs.docker.com/docker-for-mac/install/
* GIT - Required to clone this repository to local machine.
* Create two external docker volumes for postgres
	- `docker volume create --name pg_data_local`
    - `docker volume create --name pg_data_af`

### Steps
* Open CMD/Terminal and navigate to cloned repository directory
	- Windows:
		- `scripts/docker/windows/start_af.bat`
	- Mac:
		- `sh scripts/docker/mac/start_af.sh`
* Once airflow docker container is up and running , execute below to create admin user and postgres db connection in airflow.
	* `docker exec trulayer_de_webserver_1 airflow create_user -r Admin -u admin -p admin -e airflow@email.com -f airflow -l airflow`
	* `docker exec trulayer_de_webserver_1 airflow connections -a --conn_id 'pg_local' --conn_type 'postgres' --conn_host 'postgres_local' --conn_schema 'truefilm_db' --conn_login 'truefilm_user' --conn_password "truefilmpass" --conn_port "5432"`
* Navigate to Airflow UI via `http://localhost:8081/` and use the interface to run and test the DAGs.
* Main pipeline DAG - **tf_profit_movies_pl**
* Unit Test DAG - **tf_profit_movies_ut**

Why Zone
======================
* **Python**
	- Python is a goto language for Data analysis due to its rich set of libraries and its simplicity as well as readability.
	- Pandas is the most populat library for data analysis under python for its dataframe API's and features associated with it.
* **Docker**
	- It's now the de facto for developers to build , develop and deploy apps to anywhere.
	- With containers , keeps your system clean of installations of multiple softwares.
	- It is scalable and simple to deploy.
* **Airflow**
	- Programmatic workflow management
	- Written completely in **Python!!!**
	- Awesome UI
	- Native support with most of cloud providers
	- List can go on...
* **Dbeaver**
	- An one shop IDE for all database engines.
	- Its **Free!!**
* **VS Code**
	- No explanation needed for this awesome IDE
	- One higlight is how you can remote connect into the docker container and develop on the fly is awesome...!

Design/Algorithmic Decisions
======================
* An overview of the datasets IMDB dataset is of ~220 MB and Wiki dumps is of ~700 MB(Compressed) and around ~7.5GB (Uncompressed)
* At first we dont need the entire columns of IMDB dataset , so while reading the csv we just extract only required columns.
* For wiki dumps , its not a good approach to load entire data into memory unless you want to use Spark and have a distributed cluster up and running. The code is written in a way that it can run on any machines by parsing the xml via events from ElementTree python package.
* After calculating ratio , since we require only top 1000 rows , its ideal to join only those 1000 rows from IMDB dataset with the wiki info.
* Added **load_timestamp** column to final output to identify different loads.

Unit Testing
======================
* **pytest** package has been used for unit testing
* Basic count and column checks are incorporated for imdb and wiki datasets
* Since there were some wrong info in the movie dataset, for checking correctness used the below article to get top 20 movies and validated against the output
	- https://www.imdb.com/news/ni3067938
* Have included tests to verify only at the requirement level and not airflow DAG level

Screenshots
======================
### **Airflow Dashboard**

![Alt text](screenshots/dag_dashboard.png?raw=true "Airflow Dashboard")

### **TrueFilm Main DAG**

![Alt text](screenshots/main_pipeline_dag_graph.png?raw=true "TrueFilm DAG")


### **TrueFilm Unit Test DAG**

![Alt text](screenshots/unittest_dag_graph.png?raw=true "TrueFilm Unittest DAG")


### **Results from DBeaver**

![Alt text](screenshots/dbeaver_results.png?raw=true "Dbeaver Results")