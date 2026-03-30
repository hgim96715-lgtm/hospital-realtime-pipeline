import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook 

default_args={
    "owner":"hospital",
    "retries":1,
    "retry_delay":pendulum.duration(minutes=3),
}

@dag(
    dag_id="er_hourly_status_dag",
    default_args=default_args,
    description="er_realtime 시간대별집계 ",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2026,3,23,tz="Asia/Seoul"),
    catchup=False,
    tags=["hospital","batch","mart"],
)

def hourly_stats_pipeline():
    @task
    def aggregate_hourly(data_interval_start, data_interval_end):
        pg_hook=PostgresHook(postgres_conn_id="postgres_hospital")
        
        
        # ⭐️ .start_of('hour') 를 붙여서 수동 실행 시에도 무조건 정각으로 맞춥니다!
        start_time = data_interval_start.in_timezone("Asia/Seoul").start_of('hour').strftime("%Y-%m-%d %H:%M:%S")
        end_time   = data_interval_end.in_timezone("Asia/Seoul").start_of('hour').strftime("%Y-%m-%d %H:%M:%S")


        print(f"[집계 구간] {start_time} ~ {end_time} 데이터 처리 시작...")
        
        sql="""
            INSERT INTO er_hourly_status(
                stat_hour,region,
                avg_beds,zero_count,total_hospitals,saturation_pct,
                created_at
            )
            SELECT
                DATE_TRUNC('hour',r.created_at) AS stat_hour,
                h.region,
                ROUND(AVG(r.hvec)::NUMERIC,2) AS avg_beds,
                COUNT(*) FILTER(WHERE r.hvec <=0) AS zero_count,
                COUNT(*) AS total_hospitals,
                ROUND(
                    COUNT(*) FILTER(WHERE r.hvec <=0)::NUMERIC
                    /NULLIF(COUNT(*),0)*100
                    ,1
                )AS saturation_pct,
                NOW() AT TIME ZONE 'Asia/Seoul' as created_at
            FROM er_realtime r
            JOIN er_hospitals h ON r.hpid=h.hpid
            WHERE r.created_at >= %s::timestamp
                AND r.created_at < %s::timestamp
            GROUP BY DATE_TRUNC('hour',r.created_at), h.region
            ON CONFLICT DO NOTHING
            
        """
    
        pg_hook.run(sql, parameters=(start_time, end_time))
    
        print("[집계 완료] er_hourly_status 테이블 업데이트 성공!")
        
    aggregate_hourly()
    
hourly_stats_pipeline()