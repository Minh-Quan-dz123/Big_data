from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2026, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="batch_pipeline",
    schedule="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    def spark_job(task, file):
        return SparkSubmitOperator(
            task_id=task,
            application=f"/opt/spark/jobs/batch/{file}",
            conn_id="spark_default",
            conf={
                "spark.master": "spark://spark-master:7077",

                "spark.executorEnv.PYTHONPATH": "/opt/spark/jobs/batch",
                "spark.driverEnv.PYTHONPATH": "/opt/spark/jobs/batch",


                "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                "spark.hadoop.fs.s3a.access.key": "minioadmin",
                "spark.hadoop.fs.s3a.secret.key": "minioadmin",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",

            }
        )
    job1 = spark_job("job_1_user_segment", "1_user_segment_job.py")
    job2 = spark_job("job_2_user_consumption", "2_user_consumption.py")
    job3 = spark_job("job_3_trending_products", "3_trending_products.py")
    job4 = spark_job("job_4_product_similarity", "4_product_similarity.py")
    job5 = spark_job("job_5_product_complementary", "5_product_complementary.py")
    job6 = spark_job("job_6_user_recommendations", "6_user_recommendations.py")

    job1 >> job2 >> job3 >> job4 >> job5 >> job6

