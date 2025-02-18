import json
import itertools
import psycopg2

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.providers.smtp.operators.smtp import EmailOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

# Database connection
conn = BaseHook.get_connection('dashboard-data-model')
database_params = {
    'dbname': conn.schema,
    'user': conn.login,
    'password': conn.password,
    'host': conn.host,
    'port': conn.port,
}

email_receiver = Variable.get("receiver")

class Features:
    def __init__(self):
        self.biomes = [biome.strip() for biome in Variable.get("export_data_biome").strip("'").split(",")]
        self.config = [json.loads(Variable.get("features_config"))]

    def compute_years(self, **kwargs):
        try:
            dates = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    biome_found = False
                    for biome, item in itertools.product(self.biomes, self.config):
                        if biome in item:
                            biome_found = True
                            years = item[biome]['years']

                            for year in years:
                                cursor.execute(f"""
                                    SELECT start_date from public.period WHERE end_date = '{year}-07-31'::date AND 
                                    id_data=(SELECT id FROM public.data WHERE name ILIKE 'PRODES {biome.upper()}');
                                """)
                                start_date = cursor.fetchone()[0]
                                dates.append({'biome': biome, 'start_date': start_date, 'end_date': f'{year}-07-31'})
                                print(f"Dates computed: {biome} - 'start_date': {start_date}, 'end_date': f'{year}-07-31'")
                    if not biome_found:
                        raise AirflowFailException('❌ Biome not found in config')
                conn.commit()
            
            # Using Xcom to pass data between tasks
            kwargs['ti'].xcom_push(key='dates', value=dates)
            return 'query_by_year'
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')
    
    def query_by_year(self, **kwargs):
        # Receives data passed by XCom
        ti = kwargs['ti']
        dates = ti.xcom_pull(task_ids='compute_years', key='dates')
        try:
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    # for date in dates:
                        biome_found = False
                        for date, biome, item in itertools.product(dates, self.biomes, self.config):
                            if biome in item:
                                if date['biome'] == biome:
                                    biome_found = True
                                    years = item[biome]['years']
                                    lois = item[biome]['lois']
                                    for year, loi in itertools.product(years, lois):
                                        data = f'PRODES {biome.upper()}'
                                        query = f"""
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
                                        cursor.execute(query)
                                        print(f"Query executed: {query}")
                        if not biome_found:
                            raise AirflowFailException('❌ Biome not found in config')
                conn.commit()
            return '✅ Features updated successfully 🚀'
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')

with DAG(
    'features',
    default_args = default_args,
    schedule_interval = None,
    catchup = False,
    description = '''features process''',
) as dag:
    
    db_manager = Features()
        
    compute_years = PythonOperator(
        task_id = 'compute_years',
        python_callable = db_manager.compute_years,
        provide_context = True,  # Necessary to pass the context
    )
    
    query_by_year = PythonOperator(
        task_id = 'query_by_year',
        python_callable = db_manager.query_by_year,
        provide_context = True,  # Necessary to pass the context
    )

    process_success = EmailOperator(
        task_id='process_success',
        to=email_receiver,
        subject='Export Prodes data process completed successfully',
        html_content='<h3>Congratulations, Features updated successfully!</h3>',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    process_fail = EmailOperator(
        task_id='process_fail',
        to=email_receiver,
        subject='Export Prodes data process failed',
        html_content='<h3>Sorry, an error occurred while updating the Features</h3>',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    compute_years >> query_by_year
    [query_by_year, compute_years] >> process_fail
    query_by_year >> process_success