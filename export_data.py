import smtplib
import psycopg2

from airflow import DAG
from datetime import datetime
from email.mime.text import MIMEText
from airflow.hooks.base import BaseHook
from email.mime.multipart import MIMEMultipart
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator


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

# SMTP email connection
email_config = BaseHook.get_connection('mailtrap')
host = email_config.host
login = email_config.login
password = email_config.password
schema = email_config.schema
port = email_config.port
sender_email = "guilherme.anderson34@outlook.com"
receiver_email = "guilherme.anderso1@gmail.com"

# Currently the complete biomes list are: "pampa, caatinga, mata_atlantica,
# pantanal, amazon, amazon_nf, cerrado, legal_amazon"

# If you want to generate for Amazon biomes, always check the
# consolidated_data object because if you only have data in
# the priority table, the name must be adjusted

config = [
    {
        "cerrado": {
            "id_data": 1,
            "db_name": "prodes_cerrado_nb_p2024",
            "consolidated_data": "yearly_deforestation",
            "view_name": "cerrado_nb_p2024",
            "table_name": "cerrado_2024",
            "year": 2024,
        },
        "amazon": {
            "id_data": 2,
            "db_name": "prodes_amazonia_nb_p2024_11_02_2025",
            "consolidated_data": "yearly_deforestation_2024_pri_biome",
            "view_name": "amazon_nb_p2024",
            "table_name": "amazon_2024",
            "year": 2024,
        },
        "legal_amazon": {
            "id_data": 3,
            "year": 2024,
            "db_name": "prodes_amazonia_nb_p2024_11_02_2025",
            "consolidated_data": "yearly_deforestation_2024_pri",
            "view_name": "legal_amazon_nb_p2024",
            "table_name": "legal_amazon_2024",
        },
        "pampa": {
            "id_data": 4,
            "year": 2024,
            "db_name": "prodes_pampa_nb_p2023-14-02-2025",
            "consolidated_data": "yearly_deforestation",
            "view_name": "pampa_nb_p2024",
            "table_name": "pampa_2024",
        },
        "mata_atlantica": {
            "id_data": 5,
            "year": 2024,
            "db_name": "prodes_mata_atlantica_nb_p2023-14-02-2025",
            "consolidated_data": "yearly_deforestation",
            "view_name": "mata_atlantica_nb_p2024",
            "table_name": "mata_atlantica_2024",
        },
        "caatinga": {
            "id_data": 6,
            "db_name": "prodes_caatinga_nb_p2023-14-02-2025",
            "consolidated_data": "yearly_deforestation",
            "view_name": "caatinga_nb_p2024",
            "table_name": "caatinga_2024",
            "year": 2024,
        },
        "pantanal": {
            "id_data": 7,
            "db_name": "prodes_pantanal_nb_p2023-14-02-2025",
            "consolidated_data": "yearly_deforestation",
            "view_name": "pantanal_nb_p2024",
            "table_name": "pantanal_2024",
            "year": 2024,
        },
        "amazon_nf": {
            "id_data": 8,
            "db_name": "prodes_amazonia_nb_p2024_11_02_2025",
            "consolidated_data": "yearly_deforestation",
            "view_name": "amazon_nf_nb_p2024",
            "table_name": "amazon_nf_2024",
            "year": 2024,
        }
    },
]


class DatabaseManager:
    def __init__(self):
        self.biomes = ['caatinga', 'pantanal', 'cerrado']

    def create_periodes(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    for biome in self.biomes:
                        for item in config:
                            for region, data in item.items():
                                if region == biome:
                                    year = data['year']
                                    id_data = data['id_data']
                                                                        
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
            
            conn.commit()
            
            if not messages:
                return f'✅ Periodes created successfully 🚀 ({len(self.biomes)} periodes created)'
            
            return "\n".join(messages)

        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')
                
                
    def create_views(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    for biome in self.biomes:
                        
                        for item in config:
                            for region, data in item.items():
                                if region == biome:  # Verify if the current region is the biome being processed
                                    
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
                                    messages.append(f'✅ View created for {biome} 🚀')
                conn.commit()
                
            return "\n".join(messages)
        
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')
                  
                
    def create_tables(self):
        try:
            messages = []
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
                                    messages.append(f'✅ Table created for {biome} 🚀')
                conn.commit()
                
            return "\n".join(messages)
            
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Create Table Task: {e}')
        
        
    def update_tables(self):
        try:
            messages = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    
                    for biome in self.biomes:
                        for item in config:
                            for region, data in item.items():
                                if region == biome:
                                    table_name = data['table_name']
                                    view_name = data['view_name']
                                    cursor.execute(f'''INSERT INTO private.{table_name} (geom) SELECT geom FROM {view_name};''')
                                    messages.append(f'✅ Table updated for {biome} 🚀')
                
                conn.commit()
                
            return "\n".join(messages)
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Update Table Task: {e}')
            
            
    def create_subdivided_tables(self):
        try:
            messages = []
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
                                    messages.append(f'✅ Subdivided tables created for {biome} 🚀')
                conn.commit()
                
            return "\n".join(messages)
        except Exception as e:
            raise AirflowFailException(f'❌ Error in Create Subdivided Table Task: {e}')
        
        
    def send_email(self, subject, body):
        try:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = receiver_email
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            # Connect to the SMTP server
            server = smtplib.SMTP(host, port)
            server.starttls()  # Secure the connection
            server.login(login, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

            server.quit()
            return 'Email sent successfully!'

        except Exception as e:
            raise AirflowFailException(f'❌ Error in Send Email Task: {e}')
            
       
with DAG(
    'export_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='''DAG to export data from database prodes_cerrado_nb_p2024 
    table yearly_deforestation to database dashboard-data-model table private.cerrado_2024''',
) as dag:
    
    db_manager = DatabaseManager()
           
    create_periodes = PythonOperator(
        task_id='create_periodes',
        python_callable=db_manager.create_periodes,
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
    
    send_success_message = PythonOperator(
        task_id='send_success_message',
        python_callable=db_manager.send_email,
        op_kwargs={
            "subject": "Success on Export Prodes Data",
            "body": "Hello, Prodes data export process, carried out successfully!"
        },
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
        
    send_fail_message = PythonOperator(
        task_id='send_fail_message',
        python_callable=db_manager.send_email,
        op_kwargs={
            "subject": "Fail on Export Prodes Data",
            "body": "Hello, An error occurred in the process of exporting the prodes data."
        },
        trigger_rule=TriggerRule.ONE_FAILED
    )
        
    # Order of tasks
    create_periodes >> create_views
    create_views >> create_tables >> update_tables >> create_subdivided_tables >> send_success_message

    # Failure capture
    [create_periodes, create_views, create_tables, update_tables, create_subdivided_tables] >> send_fail_message

    # Successful capture
    create_subdivided_tables >> send_success_message