from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="batch_pipeline",
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    def job(task, file):
        return SparkSubmitOperator(
            task_id=task,
            application=f"/opt/spark/batch/jobs/{file}",
            conn_id="spark_default",
            conf={
                "spark.master": "spark://spark-master:7077",

                "spark.executorEnv.PYTHONPATH": "/opt/spark/batch/jobs",
                "spark.driverEnv.PYTHONPATH": "/opt/spark/batch/jobs",


                "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
                "spark.hadoop.fs.s3a.access.key": "minioadmin",
                "spark.hadoop.fs.s3a.secret.key": "minioadmin",
                "spark.hadoop.fs.s3a.path.style.access": "true"
            }
        )
    job1 = job("job_1","job1/main.py")
    job2 = job("job_2","job2/main.py")
    job3 = job("job_3","job3/main.py")
    job4 = job("job_4","job4/main.py")
    job5 = job("job_5","job5/main.py")
    job6 = job("job_6","job6/main.py")

    job1 >> job2 >> job3 >> job4 >> job5 >> job6

# trong 1 job ví dụ job1 thì dùng: "from job1.helper import do_something"
# job1/
# │
# ├── __init__.py
# ├── main.py
# ├── helper.py
# ├── config.py
# │
# ├── utils/
# │   ├── __init__.py
# │   ├── io.py
# │   └── transform.py
# │
# └── services/
#     ├── __init__.py
#     └── process.py 



# ----- main.py -----
# from job1.helper import do_something

# if __name__ == "__main__":
#     do_something()



# ----- helper.py -----
# Chứa logic chính (ETL, xử lý data)

# def do_something():
#     print("processing job1...")
    