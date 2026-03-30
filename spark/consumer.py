import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col,current_timestamp,from_utc_timestamp,when
from pyspark.sql.types import (StructType,StructField,StringType,IntegerType)

load_dotenv()


# 설정 

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")


POSTGRES_DB=os.getenv("POSTGRES_DB","hospital_db")
POSTGRES_USER=os.getenv("POSTGRES_USER","hospital_user")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD","hospital_password")

POSTGRES_HOST="postgres"
POSTGRES_PORT="5432"

# jdbc:postgresql://localhost:5434/hospital_db
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


JDBC_PROPS={
    "user":POSTGRES_USER,
    "password":POSTGRES_PASSWORD,
    "driver":"org.postgresql.Driver"
}


# er_realtime 스키마 

schema = StructType([
    StructField("hpid",       StringType()),
    StructField("hpname",     StringType()),
    StructField("hvec",       IntegerType()),
    StructField("hvoc",       IntegerType()),
    StructField("hvgc",       IntegerType()),
    StructField("hvctayn",    StringType()),
    StructField("hvventiayn", StringType()),
    StructField("hvmriayn", StringType()),
    StructField("hvangioayn", StringType()),
    StructField("notice_msg", StringType()),
    StructField("duty_tel",   StringType()),
    StructField("data_type",  StringType()),
    StructField("created_at", StringType()),
])


# sparkSession

spark=(
    SparkSession
    .builder
    .appName("HospitalConsumer")
    .config("spark.sql.shuffle.partitions","2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# kafka 읽기
df_raw=(
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP)
    .option("subscribe","er-realtime")
    .option("startingOffsets","latest")
    .load()
)

# json parsing
df=(
    df_raw
    .select(col("value").cast("string").alias("json_str"))
    .select(from_json(col("json_str"),schema).alias("data"))
    .select("data.*")
    .withColumn("created_at", from_utc_timestamp(current_timestamp(), "Asia/Seoul")) # 안바꾸면 에러가남!
    .withColumn("region",
        when(col("duty_tel").startswith("02"),  "서울")
        .when(col("duty_tel").startswith("031"), "경기")
        .when(col("duty_tel").startswith("032"), "인천")
        .when(col("duty_tel").startswith("033"), "강원")
        .when(col("duty_tel").startswith("041"), "충남")
        .when(col("duty_tel").startswith("042"), "대전")
        .when(col("duty_tel").startswith("043"), "충북")
        .when(col("duty_tel").startswith("044"), "세종")
        .when(col("duty_tel").startswith("051"), "부산")
        .when(col("duty_tel").startswith("052"), "울산")
        .when(col("duty_tel").startswith("053"), "대구")
        .when(col("duty_tel").startswith("054"), "경북")
        .when(col("duty_tel").startswith("055"), "경남")
        .when(col("duty_tel").startswith("061"), "전남")
        .when(col("duty_tel").startswith("062"), "광주")
        .when(col("duty_tel").startswith("063"), "전북")
        .when(col("duty_tel").startswith("064"), "제주")
        .otherwise("기타")
    )
)

# postgresql 적재 
def write_to_postgres(batch_df,batch_id):
    batch_count=batch_df.count()
    
    if batch_df.count()==0:
        return
    
    batch_df.write.jdbc(
        url=POSTGRES_URL,
        table="er_realtime",
        mode="append",
        properties=JDBC_PROPS
    )
    print(f"[배치 {batch_id}] {batch_count}건 → er_realtime DB 적재 완료!")
    
    
query=(
    df
    .writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation","/tmp/hospital_checkpoint")
    .trigger(processingTime="30 seconds")
    .start()
)

print("🏥 Spark Consumer 시작 — er-realtime → PostgreSQL 적재 대기 중...")
query.awaitTermination()