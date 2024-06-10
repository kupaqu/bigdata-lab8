import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.functions._
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
   
    write(df, "default.food")
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
  }

  def read(spark: SparkSession, dbtable: String): DataFrame = {
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

    return jdbcDF
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