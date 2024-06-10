import configparser
import requests
import time

from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from logger import Logger
from preprocess import assemble, scale

SHOW_LOG = True

class KmeansPredictor:
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self._evaluator = ClusteringEvaluator(featuresCol='scaled')
        
        self.log.info('KmeansEvaluator initialized.')

    def fit_predict(self, df: DataFrame) -> DataFrame:
        kmeans = KMeans(featuresCol='scaled', k=2)
        model = kmeans.fit(df)
        preds = model.transform(df)

        self.log.info('Kmeans training finished.')

        return preds

if __name__ == '__main__':
    logger = Logger(SHOW_LOG)
    log = logger.get_logger(__name__)

    config = configparser.ConfigParser()
    config.read('config.ini')

    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
            .master(config['spark']['deploy_mode']) \
                .config('spark.driver.cores', config['spark']['driver_cores']) \
                    .config('spark.executor.cores', config['spark']['executor_cores']) \
                        .config('spark.driver.memory', config['spark']['driver_memory']) \
                            .config('spark.executor.memory', config['spark']['executor_memory']) \
                                .config('spark.driver.extraClassPath', config['spark']['clickhouse_connector']) \
                                    .getOrCreate()
    
    host = config['datamart']['host']
    port = config['datamart']['http_port']
    url = f'http://{host}:{port}/get_food_data'

    for i in range(100):
        time.sleep(6)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                break
        except requests.ConnectionError:
            continue
    
    if response.status_code != 200:
        log.error(f'No data from DataMart. Got {response.status_code} code.')
        spark.stop()
        exit(1)

    df = spark.createDataFrame(response.json())
    df = assemble(df)
    df = scale(df)
    log.info(f"Got processed dataset with schema: {df.schema}")

    kmeans = KmeansPredictor()
    preds = kmeans.fit_predict(df)
    log.info(f"Kmeans fitted and predicted.")

    # spark.stop()