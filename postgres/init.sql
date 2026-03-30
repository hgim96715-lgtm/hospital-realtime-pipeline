CREATE DATABASE airflow;

CREATE USER airflow WITH PASSWORD 'airflow';

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

ALTER DATABASE airflow OWNER TO airflow;

-- er_realtime: 응급실 실시간 병상 현황
CREATE TABLE IF NOT EXISTS er_realtime (
    id SERIAL PRIMARY KEY,
    hpid VARCHAR(20),
    hpname VARCHAR(100),
    hvec INTEGER,
    hvoc INTEGER,
    hvgc INTEGER,
    hvctayn VARCHAR(10),
    hvmriayn VARCHAR(10),
    hvangioayn VARCHAR(10),
    hvventiayn VARCHAR(10),
    notice_msg TEXT,
    data_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    duty_tel VARCHAR(20),
    region VARCHAR(20)
);

-- Airflow

-- er_hourly_stats : 시간대별 집계

CREATE TABLE IF NOT EXISTS er_hourly_status (
    id SERIAL PRIMARY KEY,
    stat_hour TIMESTAMP,
    region VARCHAR(20),
    avg_beds NUMERIC(5, 2),
    zero_count INTEGER,
    total_hospitals INTEGER,
    saturation_pct NUMERIC(5, 2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- er_hospitals 병원 기본정보

CREATE TABLE IF NOT EXISTS er_hospitals (
    hpid VARCHAR(20) PRIMARY KEY,
    hpname VARCHAR(100),
    duty_addr VARCHAR(200),
    duty_tel VARCHAR(20),
    duty_eryn VARCHAR(10),
    wgs84_lat NUMERIC(10, 7),
    wgs84_lon NUMERIC(10, 7),
    hpbdn INTEGER,
    mk_stroke VARCHAR(10),
    mk_cardiac VARCHAR(10),
    mk_trauma VARCHAR(10),
    mk_pediatric VARCHAR(10),
    region VARCHAR(50),
    updated_at TIMESTAMP DEFAULT NOW()
);