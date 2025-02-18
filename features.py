import smtplib
import psycopg2

from airflow import DAG
from datetime import datetime
from email.mime.text import MIMEText
from airflow.hooks.base import BaseHook
from email.mime.multipart import MIMEMultipart
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


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

# SMTP email connection
email_config = BaseHook.get_connection('mailtrap')
host = email_config.host
login = email_config.login
password = email_config.password
schema = email_config.schema
port = email_config.port
sender_email = "guilherme.anderson34@outlook.com"
receiver_email = "guilherme.anderso1@gmail.com"

config = [
    {
        "cerrado": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "amazon": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "legal_amazon": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "pampa": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "mata_atlantica": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "caatinga": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "pantanal": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
        "amazon_nf": {
            "years": [2024],
            "lois": [1, 2, 3, 4],
        },
    }
]

class Features:
    def __init__(self):
        self.biomes = ['amazon', 'cerrado']

    def compute_years(self, **kwargs):
        try:
            dates = []
            with psycopg2.connect(**database_params) as conn:
                with conn.cursor() as cursor:
                    for biome in self.biomes:
                        for item in config:
                            for region, data in item.items():
                                if region == biome:

                                    years = data['years']
                                    for year in years:
                                        cursor.execute(f"""
                                                    SELECT start_date from public.period WHERE end_date = '{year}-07-31'::date AND 
                                                    id_data=(SELECT id FROM public.data WHERE name ILIKE 'PRODES {biome.upper()}');
                                                """)
                                        dates.append({'start_date': cursor.fetchone()[0], 'end_date': f'{year}-07-31'})
                conn.commit()
            print(f"Dates computed: {dates}")
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
                    for biome in self.biomes:
                        for date in dates:
                            for item in config:
                                for region, data in item.items():
                                    if region == biome:
                                        years = data['years']
                                        lois = data['lois']
                                        for year in years:
                                            for loi in lois:
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
                conn.commit()
            return '✅ Features updated successfully 🚀'
        except Exception as e:
            raise AirflowFailException(f'❌ Error: {e}')
        
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
        provide_context = True,  # Necessário para passar o contexto
    )
    
    query_by_year = PythonOperator(
        task_id = 'query_by_year',
        python_callable = db_manager.query_by_year,
        provide_context = True,  # Necessário para passar o contexto
    )

    send_success_message = PythonOperator(
        task_id = 'send_success_message',
        python_callable = db_manager.send_email,
        op_kwargs = {
            "subject": "Success on Features Process",
            "body": "Hello, Features process, carried out successfully!"
        },
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
        
    send_fail_message = PythonOperator(
        task_id = 'send_fail_message',
        python_callable = db_manager.send_email,
        op_kwargs = {
            "subject": "Fail on Features Process",
            "body": "Hello, An error occurred in the Features process!"
        },
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    compute_years >> query_by_year
    [query_by_year, compute_years] >> send_fail_message
    [query_by_year] >> send_success_message