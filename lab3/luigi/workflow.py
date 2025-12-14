import luigi
from pathlib import Path
import subprocess
import sys
import glob
import pandas as pd
import datetime
import os
import shutil

PROJECT_ROOT = Path("/home/adymeda/code/bigdata").resolve()

RAW_DATA_PATH = PROJECT_ROOT / "data" / "steam_reviews.csv"

TMP_SPARK_DIR = PROJECT_ROOT / "data" / "tmp" / "spark"
TMP_HADOOP_DIR = PROJECT_ROOT / "data" / "tmp" / "hadoop"

MART_DIR = PROJECT_ROOT / "data" / "marts"
MART_SPARK_PATH = MART_DIR / "steam_reviews_spark_mart.csv"
MART_HADOOP_PATH = MART_DIR / "steam_reviews_hadoop_mart.csv"

SPARK_SCRIPT = PROJECT_ROOT / "lab3" / "luigi" / "luigi_spark.py"
HADOOP_DIR = PROJECT_ROOT / "lab3" / "hadoop_streaming"
MAPPER_PATH = HADOOP_DIR / "mapper.py"
REDUCER_PATH = PROJECT_ROOT / "lab3" / "luigi" / "luigi_reducer.py"

PYTHON_EXE = sys.executable

HADOOP_HOME = os.environ.get("HADOOP_HOME", "/DATA/AppData/hadoop")
HADOOP_STREAMING_JAR = Path(HADOOP_HOME) / "share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar"
HADOOP_CMD = "hadoop"

class RunHadoopJob(luigi.Task):
    run_date = luigi.DateParameter(default=datetime.date.today())
    
    def output(self):
        return luigi.LocalTarget(str(TMP_HADOOP_DIR / "_SUCCESS"))
    
    def run(self):
        print("[RunHadoopJob] Запуск Hadoop Streaming...")

        if TMP_HADOOP_DIR.exists():
            shutil.rmtree(TMP_HADOOP_DIR)
        
        cmd = [
            HADOOP_CMD,
            "jar",
            str(HADOOP_STREAMING_JAR),
            "-D", "mapreduce.framework.name=local",
            "-D", "fs.defaultFS=file:///",
            "-D", "mapreduce.job.reduces=1",
            "-input", str(RAW_DATA_PATH),
            "-output", str(TMP_HADOOP_DIR),
            "-mapper", f"python {MAPPER_PATH}",
            "-reducer", f"python {REDUCER_PATH}",
        ]
        
        print(f"[RunHadoopJob] Команда:\n{' '.join(str(c) for c in cmd)}\n")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(HADOOP_DIR)
        )
        
        if result.returncode != 0:
            print("[RunHadoopJob] ОШИБКА!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            raise RuntimeError("Hadoop job failed")
        
        print("[RunHadoopJob] Успешно завершено!")

class RunSparkJob(luigi.Task):
    run_date = luigi.DateParameter(default=datetime.date.today())
    
    def output(self):
        return luigi.LocalTarget(str(TMP_SPARK_DIR / "_SUCCESS"))
    
    def run(self):
        print("[RunSparkJob] Запуск Spark...")
        
        if TMP_SPARK_DIR.exists():
            shutil.rmtree(TMP_SPARK_DIR)
        TMP_SPARK_DIR.mkdir(parents=True, exist_ok=True)
        
        cmd = [
            PYTHON_EXE,
            str(SPARK_SCRIPT),
            str(RAW_DATA_PATH),
            str(TMP_SPARK_DIR),
        ]
        
        print(f"[RunSparkJob] Команда: {' '.join(cmd)}\n")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT)
        )
        
        if result.returncode != 0:
            print("[RunSparkJob] ОШИБКА!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            raise RuntimeError("Spark job failed")
        
        print("[RunSparkJob] Успешно завершено!")
        print(result.stdout)

class BuildHadoopMart(luigi.Task):
    run_date = luigi.DateParameter(default=datetime.date.today())
    
    def requires(self):
        return RunHadoopJob(run_date=self.run_date)
    
    def output(self):
        return luigi.LocalTarget(str(MART_HADOOP_PATH))
    
    def run(self):
        print("[BuildHadoopMart] Сохранение витрины Hadoop Streaming результатов...")
        
        MART_DIR.mkdir(parents=True, exist_ok=True)
        
        pattern = str(TMP_HADOOP_DIR / "part-*")
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError(f"Не найдено файлов: {pattern}")
        
        df = pd.read_csv(
            files[0],
            sep="\t",
            header=None,
            names=["app_id", "app_name", "helpful_reviews_count", "helpful_percentage"],
            encoding="utf-8"
        )
        
        print(f"[BuildHadoopMart] Загружено {len(df)} записей")
        
        df["source"] = "Hadoop"
        df["calc_date"] = self.run_date
        
        df = df.sort_values("helpful_reviews_count", ascending=False)
        
        df.to_csv(self.output().path, index=False, encoding="utf-8")
        
        print(f"[BuildHadoopMart] Витрина сохранена: {MART_HADOOP_PATH}")
        print(f"\nТоп-10 игр по полезным отзывам:")
        print(df.head(10).to_string(), "\n\n")


class BuildSparkMart(luigi.Task):
    run_date = luigi.DateParameter(default=datetime.date.today())
    
    def requires(self):
        return RunSparkJob(run_date=self.run_date)
    
    def output(self):
        return luigi.LocalTarget(str(MART_SPARK_PATH))
    
    def run(self):
        print("[BuildSparkMart] Сохранение витрины Spark результатов...")
        
        MART_DIR.mkdir(parents=True, exist_ok=True)
        
        pattern = str(TMP_SPARK_DIR / "part-*.csv")
        files = glob.glob(pattern)
        
        if not files:
            raise FileNotFoundError(f"Не найдено файлов: {pattern}")
        
        df = pd.read_csv(files[0])
        
        print(f"[BuildSparkMart] Загружено {len(df)} записей")
        print(f"[BuildSparkMart] Колонки: {df.columns.tolist()}")
        
        df["source"] = "Spark"
        df["calc_date"] = self.run_date
        
        df = df.sort_values("helpful_reviews_count", ascending=False)
        
        df.to_csv(self.output().path, index=False, encoding="utf-8")
        
        print(f"[BuildSparkMart] Витрина сохранена: {MART_SPARK_PATH}")
        print(f"\nТоп-10 игр по полезным отзывам:")
        print(df.head(10).to_string(), "\n\n")

class BuildAllMarts(luigi.WrapperTask):
    run_date = luigi.DateParameter(default=datetime.date.today())
    
    def requires(self):
        return [
            BuildSparkMart(run_date=self.run_date),
            BuildHadoopMart(run_date=self.run_date),
        ]

if __name__ == "__main__":
    luigi.run()