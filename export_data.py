import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule
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


# Se quiser realizar a geração para biomas da amazonia, sempre verificar o objeto consolidated_data pois se tiver apenas o dado na tabala prioritaria o nome deve ser ajustado
config = [
    {
        "amazon":{
            "db_name": "prodes_amazonia_nb_p2024_11_02_2025",
            "consolidated_data": "yearly_deforestation_2024_pri_biome",
            "view_name": "amazon_nb_p2024",
            "table_name": "amazon_2024",
            "year": 2024,
        },
        "legal_amazon":{
            "db_name": "prodes_amazonia_nb_p2024_11_02_2025",
            "consolidated_data": "yearly_deforestation_2024_pri",
            "view_name": "legal_amazon_nb_p2024",
            "table_name": "legal_amazon_2024",
            "year": 2024,
        },
        "cerrado":{
            "db_name": "prodes_cerrado_nb_p2024",
            "consolidated_data": "yearly_deforestation",
            "view_name": "cerrado_nb_p2024",
            "table_name": "cerrado_2024",
            "year": 2024,
        }
    },
]


class DatabaseManager:
    
    def __init__(self):
        self.biomes = ['legal_amazon']


    def verify_connection_with_database(self):
        try:
            with psycopg2.connect(**database_params) as conn:
                conn.isolation_level
            return('✅ Connection successful 🚀')
        except psycopg2.OperationalError as e:
            raise AirflowFailException(f'❌ Connection failed: {e}')
        except psycopg2.Error as e:
            raise AirflowFailException(f'⚠️ Database error: {e}')
                
                
    def create_views(self):
        try:
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    for biome in self.biomes:
                        
                        for item in config:
                            for region, data in item.items():
                                if region == biome:  # Confere se a região atual é o bioma que está sendo processado
                                    
                                    view = data['view_name']
                                    consolidated_data = data['consolidated_data']
                                    db_name = data['db_name']
                                    year = data['year']
                                    
                                    cursor.execute(f"DROP VIEW IF EXISTS {view};")
                                    cursor.execute(f"""
                                        CREATE OR REPLACE VIEW {view} AS 
                                        SELECT * FROM dblink(
                                            'dbname={db_name} user=postgres password=postgres', 
                                            'SELECT geom FROM {consolidated_data} WHERE class_name = ''d{year}'' ORDER BY uid'
                                        ) AS t(geom geometry(MultiPolygon, 4674));
                                    """)
                conn.commit()
            return f'✅ Views created successfully 🚀 ({len(self.biomes)} views criadas)'
        
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')
              
                
    def create_tables(self):
        try:
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    
                    for biome in self.biomes:
                        for item in config:
                            for region, data in item.items():
                                if region == biome:
                                        
                                    table_name = data['table_name']
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
                conn.commit()
            return f'✅ Tables and indexes created successfully 🚀 ({len(self.biomes)} tables created)'
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Create Table Task: {e}')
        
        
    def update_tables(self):
        try:
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    
                    for biome in self.biomes:
                        for item in config:
                            for region, data in item.items():
                                if region == biome:
                                        
                                    table_name = data['table_name']
                                    view_name = data['view_name']
                                    print((f'''INSERT INTO private.{table_name} (geom) SELECT geom FROM {view_name};'''))
                                    cursor.execute(f'''INSERT INTO private.{table_name} (geom) SELECT geom FROM {view_name};''')
                conn.commit()
            return f'✅ Tables updatedes successfully 🚀 ({len(self.biomes)} tables updated)' 
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Update Table Task: {e}')
            
            
    def create_subdivided_tables(self):
        try:
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    
                    for biome in self.biomes:
                        for item in config:
                            for region, data in item.items():
                                if region == biome:
                                    table_name = data['table_name']                
                                    cursor.execute
                                    (f'''
                                        DROP TABLE IF EXISTS private.{table_name}_subdivided CASCADE;
                                        CREATE TABLE private.{table_name}_subdivided AS SELECT gid || '_' || gen_random_uuid() AS fid, 
                                        st_subdivide(geom) AS geom FROM private.{table_name};
                                        CREATE INDEX {table_name}_subdivided_geom_idx ON private.{table_name}_subdivided USING GIST (geom);
                                    ''')
                    conn.commit()
                return f'✅ Subdivided tables created successfully 🚀 ({len(self.biomes)} Subdivided tables created)'
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Create Subdivided Table Task: {e}')
   
    
with DAG(
    'export_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='''DAG to export data from database prodes_cerrado_nb_p2024 
    table yearly_deforestation to database dashboard-data-model table private.cerrado_2024''',
) as dag:
    
    db_manager = DatabaseManager()
    
    log_start = BashOperator(
        task_id='log_start',
        bash_command='echo "Starting PRODES data update process..."',
    )
    
    verify_connection_with_database = PythonOperator(
        task_id='connect_2_dashboard',
        python_callable=db_manager.verify_connection_with_database,
    )
    
    create_views = PythonOperator(
        task_id='create_views',
        python_callable=db_manager.create_views,
    )
    
    create_tables = PythonOperator(
        task_id='create_tables',
        op_kwargs={'message_text': 'PRODES data update process finished.'},
        python_callable=db_manager.create_tables,
    )
    
    update_tables = PythonOperator(
        task_id='update_tables',
        python_callable=db_manager.update_tables,
    )  
    
    create_subdivided_tables = PythonOperator(
        task_id='create_subdivided_tables',
        python_callable=db_manager.create_subdivided_tables,
    )
                
    log_finish_fail = BashOperator(
        task_id='log_finish_fail',
        bash_command='echo "PRODES data update process failed."',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    log_finish_success = BashOperator(
        task_id='log_finish_success',
        bash_command='echo "PRODES data update process finished."',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
        
    # Order of tasks
    log_start >> verify_connection_with_database >> create_views
    create_views >> create_tables >> update_tables >> create_subdivided_tables >> log_finish_success

    # Failure capture
    [verify_connection_with_database, create_views, create_tables, update_tables, create_subdivided_tables] >> log_finish_fail

    # Successful capture
    create_subdivided_tables >> log_finish_success