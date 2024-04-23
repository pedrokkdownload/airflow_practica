from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.utils.task_group import TaskGroup
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Ponemos una etiquetas para poder filtrar nuestros dags en la interfaz de Airflow
etiquetas = ['pedrog_practica', 'tweets'] 

# Ponemos una configuración predeterminada para todas las tareas
default_args = {
    'owner': 'airflow',
}

# Definimos el dataset que vamos a utilizar entre los dags parte_2 y parte_3 
my_file_23 = Dataset('/tmp/pedrog_dataset_23.txt')


# Definimos el DAG de la parte_3 de la práctica 
@dag(
    'dag_pedrog_practica_parte_3', default_args=default_args, 
    schedule=[my_file_23], start_date=datetime(2024, 4, 23), catchup=True, tags=etiquetas,
    description='Ejecutamos unas consultas en las tablas cargadas tweets_url y tweets_local \
                 En la tabla tweets_url utilizamos el operador PostgresOperator \
                 En la tabla tweets_local utilizamos el operador PythonOperator y una función para realizar las consultas y mostrar resultados' 
)

def dag_unificada_parte_3():
    
    # Leemos el dataset_23
    @task
    def read_dataset_23():
            with open(my_file_23.uri, 'r') as f:
                print(f.read())

    dataset_23_leido=read_dataset_23()


    # Hacemos un grupo de tareas para las consultas de la tabla tweets_url
    with TaskGroup('grupo_consultas_tweets_url') as grupo_consultas_tweets_url_task:

        # Consulta q1_tweets_url: Consulta GROUP BY de tweets_url utilizando PostgresOperator
        q1_tweets_url = PostgresOperator(
            task_id='q1_tweets_url_group_by',
            sql="""SELECT airline, COUNT(*) FROM tweets_url GROUP BY airline;""",
            postgres_conn_id='conexion_postgres_practica')
     
        # Consulta q2_tweets_url: Consulta COUNT de tweets_url utilizando PostgresOperator
        q2_tweets_url = PostgresOperator(
            task_id='q2_tweets_url_count',
            sql="""SELECT COUNT(*) FROM tweets_url;""",
            postgres_conn_id='conexion_postgres_practica')

        # Consulta q3_tweets_url: Consulta MAX de tweets_url utilizando PostgresOperator
        q3_tweets_url = PostgresOperator(
            task_id='q3_tweets_url_max',
            sql="""SELECT MAX(airline_sentiment_confidence) FROM tweets_url;""",
            postgres_conn_id='conexion_postgres_practica')

        # Consulta q4_tweets_url: Consulta MIN de tweets_url utilizando PostgresOperator
        q4_tweets_url = PostgresOperator(
            task_id='q4_tweets_url_min',
            sql="""SELECT MIN(airline_sentiment_confidence) FROM tweets_url;""",
            postgres_conn_id='conexion_postgres_practica')


    # Función que utilizaremos en la tabla tweets_local para ejecutar consultas y mostrar resultados
    def ejecutar_consulta(sql):
        pg_hook = PostgresHook(postgres_conn_id='conexion_postgres_practica')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for result in results:
            print(result)
        cursor.close()
        connection.close()

    # Hacemos un grupo de tareas para las consultas de la tabla tweets_local
    with TaskGroup('grupo_consultas_tweets_local') as grupo_consultas_tweets_local_task:

        # Consulta q1_tweets_local: Consulta GROUP BY de tweets_local utilizando PythonOperator con función para ejecutar consulta y mostrar resultados 
        q1_tweets_local = PythonOperator(
            task_id='q1_tweets_local_group_by',
            python_callable=ejecutar_consulta,
            op_kwargs={'sql': "SELECT airline, COUNT(*) FROM tweets_local GROUP BY airline;"})

         # Consulta q2_tweets_local: Consulta COUNT de tweets_local utilizando PythonOperator con función para ejecutar consulta y mostrar resultados
        q2_tweets_local = PythonOperator(
            task_id='q2_tweets_local_count',
            python_callable=ejecutar_consulta,
            op_kwargs={'sql': "SELECT COUNT(*) FROM tweets_local;"})
            
        # Consulta q3_tweets_local: Consulta MAX de tweets_local utilizando PythonOperator con función para ejecutar consulta y mostrar resultados
        q3_tweets_local = PythonOperator(
            task_id='q3_tweets_local_max',
            python_callable=ejecutar_consulta,
            op_kwargs={'sql': "SELECT MAX(airline_sentiment_confidence) FROM tweets_local;"})
      
        # Consulta q4_tweets_local: Consulta MIN de tweets_local utilizando PythonOperator con función para ejecutar consulta y mostrar resultados
        q4_tweets_local = PythonOperator(
            task_id='q4_tweets_local_min',
            python_callable=ejecutar_consulta,
            op_kwargs={'sql': "SELECT MIN(airline_sentiment_confidence) FROM tweets_local;"})
            

    # Definimos las dependencias y el orden de las tareas en el DAG
    dataset_23_leido >> grupo_consultas_tweets_url_task >> grupo_consultas_tweets_local_task

# Instanciamos el DAG
dag_instancia = dag_unificada_parte_3()