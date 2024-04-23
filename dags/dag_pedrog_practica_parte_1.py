from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Para evitar problemas de timeout, realizaremos la importación de varios módulos dentro de las funciones que los utilizan
    # import requests

# Ponemos una etiquetas para poder filtrar nuestros dags en la interfaz de Airflow
etiquetas = ['pedrog_practica', 'tweets'] 

# Ponemos una configuración predeterminada para todas las tareas
default_args = {
    'owner': 'airflow',
}

# Definimos el dataset que vamos a utilizar entre los dags parte_1 y parte_2 
my_file_12 = Dataset('/tmp/pedrog_dataset_12.txt')


# Definimos el DAG de la parte_1 de la práctica 
@dag(
    'dag_pedrog_practica_parte_1', default_args=default_args, 
    schedule='@daily', start_date=datetime(2024, 4, 23), catchup=True, tags=etiquetas,
    description='Creamos las tablas tweets_url y tweets_local \
                 Descargamos un CSV de una url y lo guardamos en un archivo local /dags/tweets_data_local.csv'
)

def dag_unificada_parte_1():

    # Hacemos un grupo de tareas para la creación de las dos tablas: tweets_url y tweets_local
    with TaskGroup('grupo_creacion_tablas') as grupo_creacion_tablas_task:
        
        # Creamos la tabla tweets_url, que más adelante se cargara cogiendo los datos de un repositorio en una url
        create_table_tweets_url = PostgresOperator(
            task_id='create_table_tweets_url',
            postgres_conn_id='conexion_postgres_practica',
            sql='''
            CREATE TABLE IF NOT EXISTS tweets_url(
                "tweet_id" text,
                "airline_sentiment" text,
                "airline_sentiment_confidence" text,
                "negativereason" text NULL,
                "negativereason_confidence" text NULL,
                "airline" text,
                "airline_sentiment_gold" text NULL,
                "name" text,
                "negativereason_gold" text NULL,
                "retweet_count" text,
                "text" text,
                "tweet_coord" text NULL,
                "tweet_created" text,
                "tweet_location" text NULL,
                "user_timezone" text
            );
            '''
        )

        # Creamos la tabla tweets_local, que más adelante se cargara cogiendo los datos de un archivo en local 
        create_table_tweets_local = PostgresOperator(
            task_id='create_table_tweets_local',
            postgres_conn_id='conexion_postgres_practica',
            sql='''
            CREATE TABLE IF NOT EXISTS tweets_local(
                "tweet_id" text,
                "airline_sentiment" text,
                "airline_sentiment_confidence" text,
                "negativereason" text NULL,
                "negativereason_confidence" text NULL,
                "airline" text,
                "airline_sentiment_gold" text NULL,
                "name" text,
                "negativereason_gold" text NULL,
                "retweet_count" text,
                "text" text,
                "tweet_coord" text NULL,
                "tweet_created" text,
                "tweet_location" text NULL,
                "user_timezone" text
            );
            '''
        )


    # Verificamos la disponibilidad del archivo csv en la url utilizando HttpSensor 
    sensor_csv_disponible = HttpSensor(
        task_id='sensor_csv_disponible',
        http_conn_id='conexion_http_practica',
        endpoint='pedrokkdownload/airflow_practica/main/tweets_data.csv',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20
    )

    # Descargamos el archivo tweets_data.csv desde la url y lo guardamos en el directorio local /opt/airflow/dags con el nombre tweets_data_local.csv 
    def descargar_y_guardar_tweets_data_local(**kwargs):
        
        # Importamos módulo
        import requests
        
        # Guardamos el archivo 
        url = 'https://raw.githubusercontent.com/pedrokkdownload/airflow_practica/main/tweets_data.csv'
        respuesta = requests.get(url)
        path_local = '/opt/airflow/dags/tweets_data_local.csv'

        if respuesta.status_code == 200:
            with open(path_local, 'wb') as archivo_local:
                archivo_local.write(respuesta.content)
            print(f'Archivo descargado y guardado en {path_local}')
        else:
            raise ValueError('No se pudo descargar el archivo, verifique la URL y la conexión.')

    # Utilizamos el PythonOperator para descargar el archivo csv desde una url
    crear_tweets_data_local_task = PythonOperator(
        task_id='crear_tweets_data_local',
        python_callable=descargar_y_guardar_tweets_data_local,
    )
        
        
    # Escribimos el dataset_12   
    @task(outlets=[my_file_12])
    def update_dataset_12():
        with open(my_file_12.uri, 'a+') as f:
            f.write('Producer update!')

    dataset_12_escrito = update_dataset_12()
    
    # Definimos las dependencias y el orden de las tareas en el DAG
    grupo_creacion_tablas_task >> sensor_csv_disponible >> crear_tweets_data_local_task >> dataset_12_escrito


# Instanciamos el DAG
dag_instancia = dag_unificada_parte_1()
    