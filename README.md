# ETL_Project

> [!NOTE]
> Airflow commands to initialize and start the scheduler:
> 
> ```bash
> airflow db init
> source ~/airflow/venv/bin/activate
> airflow db migrate
> airflow scheduler & airflow webserver
> ```

> [!IMPORTANT]
> PostgreSQL was configured to use all IP addresses and subnet mask: `0.0.0.0/24`
> Additionally, the listening of the server to make it compatible with Airflow from my WSL was set to `Listen_address = "*"`

> [!CAUTION]
> The IP address should be set to the IP of the VM running Airflow for security.



