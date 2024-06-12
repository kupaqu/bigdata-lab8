import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

object DataMart {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .appName("DataMart")
                            .master("local")
                            .config("extraClassPath", "lib/clickhouse-native-jdbc-shaded-2.7.1.jar")
                            .getOrCreate()

    val df = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      csv("data/openfoodfacts.csv")

    val numericalDf = drop_columns(df)
    val nonNaDf = dropna(numericalDf)
    val trimmedDf = trunc(nonNaDf)
    val castedDf = cast(trimmedDf)
    val filledDf = fillna(castedDf)

    println("Processed dataframe: ")
    filledDf.printSchema()
   
    write(filledDf, "default.food")
    println(s"DataMart initialized.")

    startServer(spark)
  }

  def assemble(df: DataFrame): DataFrame = {
    val inputCols = df.columns
    val outputCol = "features"

    val vectorAssembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(outputCol)
    val assembled = vectorAssembler.transform(df)

    println("Assembled vector schema: " + assembled.schema)

    return assembled
  }

  def scale(df: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled")
    val scaled = scaler.fit(df).transform(df)

    println("Scaled dataset schema: " + scaled.schema)

    return scaled
  }

  def write(df: DataFrame, dbtable: String): Unit = {
    println("Writing dataframe to database.")

    val url = "jdbc:clickhouse://clickhouse:9000"
    df.select("*").withColumn("id", monotonically_increasing_id())
      .write
      .mode("overwrite")
      .format("jdbc")
      .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", "default")
      .option("password", "")
      .option("createTableOptions", "engine=MergeTree() order by (id)")
      .save()

    println("Dataframe written to database.")
  }

  def read(spark: SparkSession, dbtable: String): DataFrame = {
    println("Reading dataframe from database.")

    val url = "jdbc:clickhouse://clickhouse:9000"
    val jdbcDF = spark.read
                  .format("jdbc")
                  .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
                  .option("url", url)
                  .option("dbtable", dbtable)
                  .option("user", "default")
                  .option("password", "")
                  .load()
                  .drop("id")

    println("Dataframe readed from database.")

    return jdbcDF
  }

  def drop_columns(df: DataFrame): DataFrame = {
    println("Dropping columns that do not contain '100g' in their names.")
  
    val columnsToKeep = df.columns.filter(colName => colName.contains("100g"))
    
    return df.select(columnsToKeep.map(col): _*)
  }

  def dropna(df: DataFrame): DataFrame = {
    println("Dropping na values.")
    val dfWithDroppedNa = df.na.drop(10)

    return dfWithDroppedNa
  }

  def trunc(df: DataFrame): DataFrame = {
    println("Trimmimg dataframe down to 1000 samples.")
    val dfTrimmed = df.limit(1000)

    return dfTrimmed
  }

  def cast(df: DataFrame): DataFrame = {
    println("Casting all columns to double type.")
    val dfCastedToDouble = df.select(df.columns.map(c => col(c).cast(DoubleType)) : _*)

    return dfCastedToDouble
  }

  def fillna(df: DataFrame): DataFrame = {
    println("Filling na values with zeros.")
    val dfWithFilledNa = df.na.fill(0)

    return dfWithFilledNa
  }

  def startServer(spark: SparkSession): Unit = {
    import akka.actor.ActorSystem
    import akka.http.scaladsl.Http
    import akka.http.scaladsl.server.Directives._
    import akka.stream.ActorMaterializer

    implicit val system = ActorSystem("DataServer")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val route = path("get_food_data") {
      get {
        var df = read(spark, "default.food")
        // df = assemble(df)
        // df = scale(df)

        println("Loaded and processed: ")
        df.printSchema()
        complete(df.toJSON.collect().mkString("[", ",", "]"))
      }
    }

    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 27015)
    println("Server running at http://localhost:27015/")
  }
}