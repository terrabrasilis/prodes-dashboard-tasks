import json
import itertools
import psycopg2

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

# Currently the complete biomes list are: "pampa, caatinga, mata_atlantica,
# pantanal, amazon, amazon_nf, cerrado, legal_amazon"

# If you want to generate for Amazon biomes, always check the
# consolidated_data object because if you only have data in
# the priority table, the name must be adjusted

class DatabaseManager:
    def __init__(self):
        self.biomes = [biome.strip() for biome in Variable.get("export_data_biome").strip("'").split(",")]
        self.config = [json.loads(Variable.get("export_data_config"))]

    def create_periodes(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    biome_found = False
                    for biome, item in itertools.product(self.biomes, self.config):    
                        if biome in item:
                            biome_found = True 
                            year = item[biome]['year']
                            id_data = item[biome]['id_data']
                            
                            cursor.execute(f'''SELECT max(id), max(end_date) FROM public.period WHERE id_data = {id_data}''')
                            periodes = cursor.fetchall()
                            
                            max_end_date = periodes[0][1]
                            next_id = int(periodes[0][0]) + 1
                            
                            if str(max_end_date.year) == str(year):
                                messages.append(f'✅ Periodes already created for {biome} 🚀')
                                continue
                            
                            cursor.execute(f'''INSERT INTO public.period(id, id_data, start_date,
                                        end_date) VALUES ({next_id}, {id_data}, '{max_end_date}', '{year}-07-31');''')
                            messages.append(f'✅ Periodes created for {biome} 🚀')

                    if not biome_found:
                        messages.append(f'❌ Biome {biome} not found in config file')

                conn.commit()            
            return "\n".join(messages)
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')
                          
    def create_views(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    biome_found = False
                    for biome, item in itertools.product(self.biomes, self.config):    
                        if biome in item:
                            biome_found = True
                            view = item[biome]['view_name']
                            consolidated_data = item[biome]['consolidated_data']
                            db_name = item[biome]['db_name']
                            year = item[biome]['year']
                            
                            cursor.execute(f"DROP VIEW IF EXISTS {view};")
                            cursor.execute(f"""
                                CREATE OR REPLACE VIEW {view} AS 
                                SELECT * FROM dblink(
                                    'dbname={db_name} user=postgres password=postgres', 
                                    'SELECT geom FROM {consolidated_data} WHERE class_name = ''d{year}'' ORDER BY uid'
                                ) AS t(geom geometry(MultiPolygon, 4674));
                            """)
                            messages.append(f'✅ View created for {biome} 🚀')
                    if not biome_found:
                        messages.append(f'❌ Biome {biome} not found in config file')
                conn.commit()
                
            return "\n".join(messages)
        
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')            
                
    def create_tables(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    biome_found = False
                    for biome, item in itertools.product(self.biomes, self.config):    
                        if biome in item:
                            biome_found = True
                            table_name = item[biome]['table_name']
                            cursor.execute(f'''
                                DROP TABLE IF EXISTS private.{table_name} CASCADE;
                                CREATE TABLE private.{table_name} (
                                    gid serial4 NOT NULL,
                                    geom public.geometry(multipolygon, 4674) NULL,
                                    CONSTRAINT {table_name}_pkey PRIMARY KEY (gid)
                                );
                            ''')

                            cursor.execute(f'''
                                DO $$
                                BEGIN
                                    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = '{table_name}' AND indexname = '{table_name}_geom_idx') THEN
                                        CREATE INDEX {table_name}_geom_idx ON private.{table_name} USING gist (geom);
                                    END IF;
                                END $$;
                            ''')
                            messages.append(f'✅ Table created for {biome} 🚀')
                if not biome_found:
                    messages.append(f'❌ Biome {biome} not found in config file')
                conn.commit()
                
            return "\n".join(messages)
            
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Create Table Task: {e}')
                
    def update_tables(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    biome_found = False
                    for biome, item in itertools.product(self.biomes, self.config):    
                        if biome in item:
                            biome_found = True
                            table_name = item[biome]['table_name']
                            view_name = item[biome]['view_name']
                            cursor.execute(f'''INSERT INTO private.{table_name} (geom) SELECT geom FROM {view_name};''')
                            messages.append(f'✅ Table updated for {biome} 🚀')
                if not biome_found:
                    messages.append(f'❌ Biome {biome} not found in config file')
                conn.commit()
                
            return "\n".join(messages)
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Update Table Task: {e}')
                        
    def create_subdivided_tables(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:                    
                    biome_found = False
                    for biome, item in itertools.product(self.biomes, self.config):    
                        if biome in item:
                            biome_found = True
                            table_name = item[biome]['table_name']               
                            cursor.execute
                            (f'''
                                DROP TABLE IF EXISTS private.{table_name}_subdivided CASCADE;
                                CREATE TABLE private.{table_name}_subdivided AS SELECT gid || '_' || gen_random_uuid() AS fid, 
                                st_subdivide(geom) AS geom FROM private.{table_name};
                                CREATE INDEX {table_name}_subdivided_geom_idx ON private.{table_name}_subdivided USING GIST (geom);
                            ''')
                            messages.append(f'✅ Subdivided tables created for {biome} 🚀')
                if not biome_found:
                    messages.append(f'❌ Biome {biome} not found in config file')
                conn.commit()
            return "\n".join(messages)
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Create Subdivided Table Task: {e}')
                
       
with DAG(
    'export_data',
    default_args = default_args,
    schedule_interval = None, # None to run manually
    catchup = False,
    description = '''DAG to export data from prodes biome database to dashboard-data-model database.''',
) as dag:
    
    db_manager = DatabaseManager()
           
    create_periodes = PythonOperator(
        task_id = 'create_periodes',
        python_callable = db_manager.create_periodes,
    )

    create_views = PythonOperator(
        task_id = 'create_views',
        python_callable = db_manager.create_views,
    )
    
    create_tables = PythonOperator(
        task_id = 'create_tables',
        python_callable = db_manager.create_tables,
    )
    
    update_tables = PythonOperator(
        task_id = 'update_tables',
        python_callable = db_manager.update_tables,
    )  
    
    create_subdivided_tables = PythonOperator(
        task_id = 'create_subdivided_tables',
        python_callable = db_manager.create_subdivided_tables,
    )
    
    process_success = EmailOperator(
        task_id='process_success',
        to=email_receiver,
        subject='Export Prodes data process completed successfully',
        html_content='<h3>Congratulations, Prodes data processing has been completed successfully!</h3>',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    process_fail = EmailOperator(
        task_id='process_fail',
        to=email_receiver,
        subject='Export Prodes data process failed',
        html_content='<h3>Sorry, the Prodes data processing failed!</h3>',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    trigger_dag_features = TriggerDagRunOperator(
        task_id="trigger_dag_features",
        trigger_dag_id="features",
        wait_for_completion=False,
    )
        
    # Order of tasks
    create_periodes >> create_views
    create_views >> create_tables >> update_tables >> create_subdivided_tables >> process_success >> trigger_dag_features

    # Failure capture
    [create_periodes, create_views, create_tables, update_tables, create_subdivided_tables, trigger_dag_features] >> process_fail

    # Successful capture
    create_subdivided_tables >> process_success