# ETL_Project

Airflow commands in WSL

Only required first time
- airflow db init

copy + paste for webserver + scheduler

source ~/airflow/venv/bin/activate
airflow db migrate
airflow scheduler & airflow webserver


- port 8793 in use kill sequence

netstat -tulnp | grep 8793

pkill -9 airflow
kill -9 <PID>


