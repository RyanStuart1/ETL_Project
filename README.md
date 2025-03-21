# ETL_Project

<p style="color:blue; border-bottom: 2px solid blue; padding-bottom: 10px;">Airflow</p>
<p>USEFUL commands for Airflow in Linux:</p>
<pre><code>airflow db init
source ~/airflow/venv/bin/activate
airflow db migrate
airflow scheduler & airflow webserver
</code></pre>

<p style="color:red; border-bottom: 2px solid green; padding-bottom: 10px;">PostgreSQL</p>
<p>PostgreSQL was configured to use all IP addresses and subnet mask: 0.0.0.0/24</p>
<p>Additionally, the listening of the server to make it compatible with Airflow from my WSL was set to <code>Listen_address = "*"</code></p>

<p style="color:green; border-bottom: 2px solid orange; padding-bottom: 10px;">NOTE</p>
<p>The IP address should be set to the IP of the VM running Airflow for security.</p>




