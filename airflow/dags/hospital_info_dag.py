import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import unquote
import pendulum  

# from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from psycopg2.extras import execute_values 


API_KEY       = unquote(os.getenv("HOSPITAL_API_KEY", ""))
BASE_URL      = "http://apis.data.go.kr/B552657/ErmctInfoInqireService"
SUCCESS_CODES = {"00", "0000"}

def parse_float(val,default=None):
    try:
        return float(val) if val else default
    except (ValueError,TypeError):
        return default

def parse_int(val,default=None):
    try:
        return int(val) if val else default
    except (ValueError,TypeError):
        return default

def get_region(addr:str)->str:
    return addr[:2] if addr else ""

def strip_str(val,default=""):
    return str(val).strip() if val else default


default_args = {
    "owner": "hospital",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(minutes=10),
    "depends_on_past": False,
    "email_on_failure": True
}
@dag(
    dag_id="hospital_info_dag",
    default_args=default_args,
    description="응급의료기관 기본정보 1일 1회 수집",
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2026, 3, 23, tz="Asia/Seoul"),
    catchup=False,
    tags=["hospital", "batch"],
)


def hospital_info_pipeline():
    
    @task
    def fetch_hospitals():
        params = {
            "serviceKey": API_KEY,
            "numOfRows": 1000,
            "pageNo": 1
        }
        print(params)
        try:
            res = requests.get(
                f"{BASE_URL}/getEgytBassInfoInqire",
                params=params, timeout=30
            )
            res.raise_for_status()
            root = ET.fromstring(res.content)
            result_code = root.findtext(".//resultCode", "")
            if result_code not in SUCCESS_CODES:
                raise Exception(f"API 에러 :{result_code}")
        except Exception as e:
            print(f"[에러] 에러 확인 해주세요 {e}")
            raise
        
        rows = []
        for item in root.findall(".//item"):
            duty_addr = item.findtext("dutyAddr", "").strip()
            row_hospital = (
                strip_str(item.findtext("hpid")),
                strip_str(item.findtext("dutyName")),
                duty_addr,
                strip_str(item.findtext("dutyTel3")),
                strip_str(item.findtext("dutyEryn")),
                parse_float(item.findtext("wgs84Lat")),
                parse_float(item.findtext("wgs84Lon")),
                parse_int(item.findtext("hpbdn")),
                strip_str(item.findtext("MKioskTy1")),
                strip_str(item.findtext("MKioskTy3")),
                strip_str(item.findtext("MKioskTy25")),
                strip_str(item.findtext("MKioskTy10")),
                get_region(duty_addr),
            )
            rows.append(row_hospital)
        
        print(f"[수집] {len(rows)}개 병원 수집 완료")
        
        # ── PostgreSQL 적재 ──
        pg_hook = PostgresHook(postgres_conn_id="postgres_hospital")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        print(f"conn:{conn}, cur:{cur}")
        
        sql = """
            INSERT INTO er_hospitals (
                hpid, hpname, duty_addr, duty_tel, duty_eryn,
                wgs84_lat, wgs84_lon, hpbdn,
                mk_stroke, mk_cardiac, mk_trauma, mk_pediatric,
                region, updated_at
            ) VALUES %s
            ON CONFLICT (hpid) DO UPDATE SET
                hpname      = EXCLUDED.hpname,
                duty_addr   = EXCLUDED.duty_addr,
                duty_tel    = EXCLUDED.duty_tel,
                duty_eryn   = EXCLUDED.duty_eryn,
                wgs84_lat   = EXCLUDED.wgs84_lat,
                wgs84_lon   = EXCLUDED.wgs84_lon,
                hpbdn       = EXCLUDED.hpbdn,
                mk_stroke   = EXCLUDED.mk_stroke,
                mk_cardiac  = EXCLUDED.mk_cardiac,
                mk_trauma   = EXCLUDED.mk_trauma,
                mk_pediatric= EXCLUDED.mk_pediatric,
                region      = EXCLUDED.region,
                updated_at  = NOW()
        """
        
        rows_with_ts = [r + (datetime.now(),) for r in rows]
        
        print("=== 💡 데이터 샘플 확인 (3건) ===")
        for row in rows_with_ts[:3]:
            print(row)
        print("================================")
        
        execute_values(cur, sql, rows_with_ts)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"[적재] {len(rows)}개 병원 → er_hospitals UPSERT 완료")

    fetch_hospitals()

hospital_info_pipeline()