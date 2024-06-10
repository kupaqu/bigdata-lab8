from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import VectorAssembler, StandardScaler

def assemble(df: DataFrame) -> DataFrame:
    vecAssembler = VectorAssembler(inputCols=df.columns, outputCol='features')
    data = vecAssembler.transform(df)

    return data

def scale(df: DataFrame) -> DataFrame:
    standardScaler = StandardScaler(inputCol='features', outputCol='scaled')
    model = standardScaler.fit(df)
    data = model.transform(df)

    return data