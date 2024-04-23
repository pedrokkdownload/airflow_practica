from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Para evitar problemas de timeout, realizaremos la importación de varios módulos dentro de las funciones que los utilizan
    # import requests
    # import csv
    # import os

# Ponemos una etiquetas para poder filtrar nuestros dags en la interfaz de Airflow
etiquetas = ['pedrog_practica', 'tweets'] 

# Ponemos una configuración predeterminada para todas las tareas
default_args = {
    'owner': 'airflow',
}

# Definimos el dataset que vamos a utilizar entre los dags parte_1 y parte_2 
my_file_12 = Dataset('/tmp/pedrog_dataset_12.txt')

# Definimos el dataset que vamos a utilizar entre los dags parte_2 y parte_3
my_file_23 = Dataset('/tmp/pedrog_dataset_23.txt')


# Definimos el DAG de la parte_2 de la práctica 
@dag(
    'dag_pedrog_practica_parte_2', default_args=default_args, 
    schedule=[my_file_12], start_date=datetime(2024, 4, 23), catchup=True, tags=etiquetas,
    description='Cargamos la tabla tweets_url desde una descarga de un archivo de una url \
                 Cargamos la tabla tweets_local desde un archivo local'
)

def dag_unificada_parte_2():

    # Leemos el dataset_12
    @task
    def read_dataset_12():
        with open(my_file_12.uri, 'r') as f:
            print(f.read())

    dataset_12_leido=read_dataset_12()
    
 
    # Descargamos el archivo CSV desde una url
    def descargar_csv_desde_url():
        
        # Importamos módulo
        import requests
        
        # Guardamos el archivo 
        url = 'https://raw.githubusercontent.com/pedrokkdownload/airflow_practica/main/tweets_data.csv'
        response = requests.get(url)
        path = '/tmp/tweets_data.csv'
        with open(path, 'wb') as file:
            file.write(response.content)
        return path

    # Utilizamos el PythonOperator para descargar el archivo csv desde una url
    descargar_csv_desde_url_task = PythonOperator(
        task_id='descargar_csv_desde_url',
        python_callable=descargar_csv_desde_url,
    )

    # Cargamos en la tabla tweets_url de postgres el archivo descargado anteriormente desde una url
    def cargar_tweets_url(**kwargs):
        ti = kwargs['ti']
        path_csv = ti.xcom_pull(task_ids='descargar_csv_desde_url')
        
        # Establecemos la conexión a postgres con PostgresHook y usando la conexión definida en Airflow
        pg_hook = PostgresHook(postgres_conn_id='conexion_postgres_practica')
        
        # Obtenemos el cursor de la conexión
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Importamos módulo
        import csv
    
        # Abrimos el archivo CSV
        with open(path_csv, 'r') as f:
            
            # Leemos el contenido del csv saltando el encabezado
            reader = csv.reader(f)
            next(reader)  
            
            # Preparamos la consulta para insertar datos
            insert_query = """
                INSERT INTO tweets_url (
                    tweet_id, airline_sentiment, airline, airline_sentiment_confidence, negativereason,
                    negativereason_confidence, airline_sentiment_gold, name, negativereason_gold,
                    retweet_count, text, tweet_coord, tweet_created, tweet_location, user_timezone
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            # Insertamos cada fila del csv en la base de datos
            for row in reader:
                # Convertimos la lista 'row' a una tupla
                data_tuple = tuple(row)
                # Imprimmos un mensaje para posible necesidad de depurar
                print("Intentando insertar:", data_tuple) 
                cursor.execute(insert_query, data_tuple)
        
        # Confirmamos los cambios
        conn.commit()
        
        # Cerramos el cursor y la conexión
        cursor.close()
        conn.close()
    
    # Utilizamos el PythonOperator para cargar en la tabla tweets_url el archivo csv que hemos descargado desde una url
    cargar_tweets_url_task = PythonOperator(
        task_id='cargar_tweets_url_desde_url',
        python_callable=cargar_tweets_url,
    )

 

    # Cargamos el contenido del archivo local tweets_data_local.csv a la tabla tweets_local de postgres
    def cargar_tweets_local():
        # Establecemos la conexión a postgres con PostgresHook y usando la conexión definida en Airflow
        pg_hook = PostgresHook(postgres_conn_id='conexion_postgres_practica')
        
        # Obtenemos el cursor de la conexión
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Importamos módulo
        import os

        # Definimos la ruta absoluta al directorio donde se encuentra el archivo csv
        ruta_dags = '/opt/airflow/dags/'

        # Definimos el nombre del archivo csv
        nombre_archivo_csv = 'tweets_data_local.csv'

        # Definimos la ruta absoluta al archivo csv
        ruta_completa_csv = os.path.join(ruta_dags, nombre_archivo_csv)

        # Abrimos el archivo csv con la ruta completa
        with open(ruta_completa_csv, 'r') as f: 
            # Saltamos el encabezado del csv
            next(f)
            # Cargamos el csv ejecutando el comando COPY
            cursor.copy_expert("COPY tweets_local FROM STDIN WITH CSV", f)
    
        # Confirmamos los cambios
        conn.commit()
        
        # Cerramos el cursor y la conexión
        cursor.close()
        conn.close()

    # Utilizamos el PythonOperator para cargar en la tabla tweets_local el archivo csv que tenemos en local
    cargar_tweets_local_task = PythonOperator(
        task_id='cargar_tweets_local_desde_csv',
        python_callable=cargar_tweets_local,
    )
    
       
    # Escribimos el dataset_23   
    @task(outlets=[my_file_23])
    def update_dataset_23():
        with open(my_file_23.uri, 'a+') as f:
            f.write('Producer update!')

    dataset_23_escrito = update_dataset_23()
    
    # Definimos las dependencias y el orden de las tareas en el DAG
    dataset_12_leido >> descargar_csv_desde_url_task >> [cargar_tweets_url_task, cargar_tweets_local_task] >> dataset_23_escrito


# Instanciamos el DAG
dag_instancia = dag_unificada_parte_2()
    