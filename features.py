import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

conn = BaseHook.get_connection('dashboard-data-model')
database_params = {
    'dbname': conn.schema,
    'user': conn.login,
    'password': conn.password,
    'host': conn.host,
    'port': conn.port,
}


class Features:


    def __init__(self):
        self.biomes = ['amazon']
        self.years = [2024]
        self.lois = [1, 2, 3, 4]


    def compute_years(self, **kwargs):
        dates = []
        
        for biome in self.biomes:
            for year in self.years:
                end_year = year
                sql_data = f"""
                    SELECT start_date from public.period WHERE end_date = '{end_year}-07-31'::date AND 
                    id_data=(SELECT id FROM public.data WHERE name ILIKE 'PRODES {biome.upper()}');
                """
                with psycopg2.connect(**database_params) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(sql_data)
                        dates.append({'start_date': cursor.fetchone()[0], 'end_date': f'{end_year}-07-31'})
        
        # Using Xcom to pass data between tasks
        kwargs['ti'].xcom_push(key='dates', value=dates)
        print(f"Dates computed: {dates}")
        return 'query_by_year'
    
    
    def query_by_year(self, **kwargs):
        # Receives data passed by XCom
        ti = kwargs['ti']
        dates = ti.xcom_pull(task_ids='compute_years', key='dates')
        
        try:
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    for date in dates:
                        for biome in self.biomes:
                            for year in self.years:
                                for loi in self.lois:
                                    data = f'PRODES {biome.upper()}'
                                    query=f"""
                                            INSERT INTO features (id_period, id_data_loi_loinames, id_data_class, created_at, gid_polygon, geom)
                                            SELECT
                                                ( SELECT per.id FROM period as per
                                                INNER JOIN data ON (per.start_date = '{date['start_date']}'::date
                                                AND per.end_date = '{date['end_date']}'::date
                                                AND data.id = per.id_data
                                                AND data.name = '{data}') ) as id_period,
                                                dll.id as id_data_loi_loinames,
                                                ( SELECT dc.id FROM data_class as dc
                                                INNER JOIN class ON (class.id = dc.id_class AND class.name = 'deforestation')
                                                INNER JOIN data ON (data.id = dc.id_data AND data.name = '{data}') ) as id_data_class,
                                                now() as created_at,
                                                mask.fid as gid_polygon,
                                                CASE WHEN ST_CoveredBy(mask.geom, l.geom)
                                                THEN ST_Multi(ST_MakeValid(mask.geom))
                                                ELSE ST_Multi(ST_CollectionExtract(ST_Intersection(mask.geom, l.geom), 3))
                                                END AS geom
                                            FROM private.{biome}_{year}_subdivided AS mask
                                            INNER JOIN loinames l ON ( (mask.geom && l.geom) AND ST_Intersects(mask.geom, l.geom) )
                                            INNER JOIN loi_loinames ll ON (l.gid = ll.gid_loinames AND ll.id_loi = {loi})
                                            INNER JOIN data_loi_loinames dll ON (dll.id_loi_loinames = ll.id)
                                            INNER JOIN data d ON (d.id = dll.id_data AND d.name = '{data}')
                                            ON CONFLICT DO NOTHING;"""
                                    print(f"Executing query: {query}")
                                    cursor.execute(query)
                                    conn.commit()
            return '✅ Features updated successfully 🚀'
        except Exception as e:
            raise Exception(f'❌ Error in Query Task: {e}')

with DAG(
    'features',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='''features process''',
) as dag:
    
    db_manager = Features()
    
    log_start = BashOperator(
        task_id='log_start',
        bash_command='echo "Starting FEATURES data update process..."',
    )
    
    compute_years = PythonOperator(
        task_id='compute_years',
        python_callable=db_manager.compute_years,
        provide_context=True,  # Necessário para passar o contexto
    )
    
    query_by_year = PythonOperator(
        task_id='query_by_year',
        python_callable=db_manager.query_by_year,
        provide_context=True,  # Necessário para passar o contexto
    )
    
    log_start >> compute_years >> query_by_year
