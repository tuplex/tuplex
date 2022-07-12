import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
// needs to go to extra object b.c. Spark's closure mechanism is broken.
object Helpers {

  implicit class ColumnFormatterRichDataFrame(df: DataFrame) {

    val parseDouble = udf((x: String) => {
      val precision = 1 // precision of float to compare, here only one because of Spark issues...
      Try(x.slice(0, x.indexOf(".") + precision + 1).toDouble).toOption
    })

    /*!
    b.c. each framework uses different precision printing, need to round result.
    This needs to be done by truncation, else some frameworks might round up or down.
     */
    def roundColumn(colName: String): DataFrame = df.withColumn(colName, parseDouble(col(colName)))

    def lower(colName: String): DataFrame = df.withColumn(colName, org.apache.spark.sql.functions.lower(col(colName)))

    def roundColumns(colNames: Seq[String]): DataFrame = {
      var res_df = df
      colNames.foreach(colName => res_df = res_df.roundColumn(colName))
      res_df
    }
  }
}