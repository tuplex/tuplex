import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import Helpers._

object VerifyApp {

  // original python code because it's so slow translated to scala
  // spark = SparkSession.builder.appName("flight_query_output_validator").getOrCreate()
  //df = spark.read.csv(os.path.join(folderA, '*.csv'), header=True).cache()
  //df_spark = spark.read.csv(os.path.join(folderB, '*.csv'), header=True).cache()
  //
  //# compute diff in count and actual rows
  //countdiff = abs(df_spark.count() - df.count())
  //diff_rows = df.union(df_spark).subtract(df.intersect(df_spark)).cache()
  //diff_row_cnt = diff_rows.count()
  //if diff_row_cnt > 0:
  //    diff_rows.coalesce(1).write.csv(diff_output_path, mode='overwrite', sep=',', header=True, escape='"', nullValue='\u0000',
  //               emptyValue='\u0000')

  val flightOutputSchema = StructType(Array(StructField("CarrierName", StringType, true),
    StructField("CarrierCode", StringType, true),
    StructField("FlightNumber", IntegerType, true),
    StructField("Day", IntegerType, true),
    StructField("Month", IntegerType, true),
    StructField("Year", IntegerType, true),
    StructField("DayOfWeek", IntegerType, true),
    StructField("OriginCity", StringType, true),
    StructField("OriginState", StringType, true),
    StructField("OriginAirportIATACode", StringType, true),
    StructField("OriginLongitude", DoubleType, true),
    StructField("OriginLatitude", DoubleType, true),
    StructField("OriginAltitude", DoubleType, true),
    StructField("DestCity", StringType, true),
    StructField("DestState", StringType, true),
    StructField("DestAirportIATACode", StringType, true),
    StructField("DestLongitude", DoubleType, true),
    StructField("DestLatitude",  DoubleType, true),
    StructField("DestAltitude",  DoubleType, true),
    StructField("Distance", DoubleType, true),
    StructField("CancellationReason", StringType, true),
    StructField("Cancelled", StringType, true),
    StructField("Diverted", StringType, true),
    StructField("CrsArrTime", StringType, true),
    StructField("CrsDepTime", StringType, true),
    StructField("ActualElapsedTime", IntegerType, true),
    StructField("AirTime", IntegerType, true),
    StructField("ArrDelay", IntegerType, true),
    StructField("CarrierDelay", IntegerType, true),
    StructField("CrsElapsedTime", IntegerType, true),
    StructField("DepDelay", IntegerType, true),
    StructField("LateAircraftDelay", IntegerType, true),
    StructField("NasDelay", IntegerType, true),
    StructField("SecurityDelay", IntegerType, true),
    StructField("TaxiIn", IntegerType, true),
    StructField("TaxiOut", IntegerType, true),
    StructField("WeatherDelay", IntegerType, true),
    StructField("AirlineYearFounded", IntegerType, true),
    StructField("AirlineYearDefunct", IntegerType, true)))

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("usage: spark-submit VerifyApp.jar folderA folderB")
    } else {
      val folderA = args(0)
      val folderB = args(1)
      val spark = SparkSession.builder
        .appName("Simple Application")
        .master("local[*]") getOrCreate()


      // round all double columns for comparison
      val doubleCols = flightOutputSchema.fields.filter(s => s.dataType == DoubleType).map(s => s.name)

      // remap schema to have double cols as strings and parse manually to compare different representations
      val schema = StructType(flightOutputSchema.fields.map(s => s.dataType match {
        case DoubleType => StructField(s.name, StringType, s.nullable)
        case _ => s
      } ).toArray)

      val keyCols = Array("Day", "Month", "Year", "FlightNumber", "CarrierCode", "OriginState", "OriginLongitude", "DestLongitude")

      val df_A = spark.read.option("header", "true").schema(schema).csv(folderA + "/*.csv").roundColumns(doubleCols).lower("Cancelled").lower("Diverted").cache()
      val df_B = spark.read.option("header", "true").schema(schema).csv(folderB + "/*.csv").roundColumns(doubleCols).lower("Cancelled").lower("Diverted").cache()
      val not_in_B = df_A.except(df_B).withColumn("info", lit("not in " + folderB)).cache()
      val not_in_A = df_B.except(df_A).withColumn("info", lit("not in " + folderA)).cache()
      val diff_rows = not_in_B.union(not_in_A).dropDuplicates()

      val rows_A = df_A.count()
      val rows_B = df_B.count()
      val rows_not_in_B = not_in_B.count()
      val rows_not_in_A = not_in_A.count()
      val rows_diff = diff_rows.count()
      diff_rows.sort(keyCols(0), keyCols.slice(1, keyCols.length):_*).coalesce(1).write.mode("overwrite").option("header", "true").csv("diff_rows.csv")
      spark.stop()

      println("Verification script summary:")
      println("----------------------------")
      println("#rows " + folderA + ": " + rows_A.toString)
      println("#rows " + folderB + ": " + rows_B.toString)
      println("#missing rows in " + folderA + " from " + folderB + " : " + rows_not_in_B.toString)
      println("#missing rows in " + folderB + " from " + folderA + " : " + rows_not_in_A.toString)
      println("#total diff rows: " + rows_diff.toString)
      println("output saved to diff_rows.csv")
      if(rows_diff == 0)
        println(" => successfully verified output to be identical!")
      else
        println(" => output differs, verification failed!")
    }

    println("job completed")
  }
}