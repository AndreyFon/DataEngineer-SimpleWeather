#imports
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator

from datetime import datetime, timedelta
import pendulum


default_args = dict(
start_date=pendulum.datetime(year=2024, month=9, day=9, tz="local"),
owner="airflow"
)

#args with day early to get "2024-09-07" data
default_args_day = dict(
start_date=pendulum.datetime(year=2024, month=9, day=8, tz="local"),
owner="airflow"
)


#each dag runs task which is a docker python script container with relevant functions
with DAG(
dag_id="Getting_places_data",
schedule="@once",
catchup=False,
default_args=default_args
) as dag:
    
    #calling places function inside script
    task0 = DockerOperator(
        task_id="Docker_Python_ETL_Script_get_places",
        image="etl:script",
        network_mode="localdb_default",
        command="-f api_get_places"
    )        

    task0

with DAG(
dag_id="current_weather_every_30_min",
schedule="*/30 * * * *",
catchup=False,
default_args=default_args
) as dag:

    #calling now function inside script
    task0 = DockerOperator(
        task_id="Docker_Python_ETL_Script_current_weather",
        image="etl:script",
        network_mode="localdb_default",
        command="-f api_get_weather_now"
    )

    task0

with DAG(
dag_id="yesterday_day_weather",
schedule="@daily",
default_args=default_args_day
) as dag:

    #calling day function inside script
    task0 = DockerOperator(
        task_id="Docker_Python_ETL_Script_day_weather",
        image="etl:script",
        network_mode="localdb_default",
        command="-f api_get_weather_day "+(datetime.now()-timedelta(1)).strftime("%Y-%m-%d")
    )

    task0

