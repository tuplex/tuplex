/*!
(c) L.Spiegelberg 2019
Scala implementation using DataFrame API for cleaning the zillow files
 */

package tuplex.exp.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, udf}

object ZillowClean {
  def main(args: Array[String]): Unit = {

    // UDFs
    val extractBd: String => Integer = v => try {
      val idx = v.indexOf(" bd")

      val max_idx = if (idx < 0) v.length else idx

      // find comma before
      val s = v.substring(0, max_idx)

      val comma_idx = s.lastIndexOf(",")

      val split_idx = if (comma_idx < 0) 0 else comma_idx + 2

      Integer.parseInt(s.substring(split_idx))
    } catch {
      case e: Exception => null
    }

    val extractBa: String => Integer = v => try {
      val idx = v.indexOf(" ba")

      val max_idx = if (idx < 0) v.length else idx

      // find comma before
      val s = v.substring(0, max_idx)

      val comma_idx = s.lastIndexOf(",")

      val split_idx = if (comma_idx < 0) 0 else comma_idx + 2

      Integer.parseInt(s.substring(split_idx))
    } catch {
      case e: Exception => null
    }

    val extractSqft: String => Integer = v => try {
      val idx = v.indexOf(" sqft")

      val max_idx = if (idx < 0) v.length else idx

      // find comma before
      val s = v.substring(0, max_idx)

      val comma_idx = s.lastIndexOf("ba ,")

      val split_idx = if (comma_idx < 0) 0 else comma_idx + 5

      Integer.parseInt(s.substring(split_idx).replace(",", ""))
    } catch {
      case e: Exception => null
    }

    val extractType: String => String = v => try {
      val s = v.toLowerCase
      if(s.contains("condo") || s.contains("apartment")) {
        "condo"
      }
      else if(s contains "house") {
        "house"
      } else {
        "unknown"
      }
    } catch {
      case e: Exception => null
    }

    val extractZipcode: String => String = v => try {
      val z = v.toDouble.toInt
      f"$z%05d"
    } catch {
      case e: Exception => null
    }

    val extractCity: String => String = v => try {
      v.head.toUpper + v.tail.toLowerCase
    } catch {
      case e: Exception => null
    }

    val extractOffer: String => String = v => try {
      if(v.contains("Sale")) {
        "sale"
      } else if(v.contains("Rent")) {
        "rent"
      } else if(v.contains("SOLD")) {
        "sold"
      } else if(v.toLowerCase().contains("foreclose")) {
        "foreclosed"
      } else {
        v
      }
    } catch {
      case e: Exception => null
    }

    val extractPrice: (String, String, String, Integer) => Integer = (price, offer, facts, sqft) => try {
      if(offer == "sold") {
        val s = facts.substring(facts.indexOf("Price/sqft:") + "Price/sqft:".length + 1)
        val r = s.substring(s.indexOf("$")+1, s.indexOf(", ") - 1)

        val price_per_sqft = Integer.parseInt(r)

        price_per_sqft * sqft
      } else if(offer == "rent") {
        val max_idx = price.lastIndexOf("/")
        Integer.parseInt(price.substring(1, max_idx).replace(",", ""))
      } else {
        Integer.parseInt(price.substring(1).replace(",", ""))
      }
    } catch {
      case e: Exception => null
    }


    // (1) Spark Initialization
    var tstart = System.nanoTime()
    val spark = SparkSession.builder.appName("Zillow Clean Query").getOrCreate()

    val sc = spark.sparkContext

    val sparkInitDuration = (System.nanoTime() - tstart) / 1.0e+9
    tstart = System.nanoTime()

    // (2) Zillow Job
    // usage is simply with the csv file...
    val inPath = args(0)
    val outPath = args(1)
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "permissive")
      // .option("multiLine", "true") // disable, else spark fails to parallelize...
      .option("escape", "\"")
      .load(inPath)

    val extractBdUDF = udf(extractBd)
    val extractBaUDF = udf(extractBa)
    val extractSqftUDF = udf(extractSqft)
    val extractTypeUDF = udf(extractType)
    val extractZipcodeUDF = udf(extractZipcode)
    val extractCityUDF = udf(extractCity)
    val extractOfferUDF = udf(extractOffer)
    val extractPriceUDF = udf(extractPrice)

    // pipeline
    df.withColumn("bedrooms", extractBdUDF(col("facts and features")))
      .filter(col("bedrooms") < 10)
      .withColumn("type", extractTypeUDF(col("title")))
      .filter("type == 'house'")
      .withColumn("zipcode", extractZipcodeUDF(col("postal_code")))
      .withColumn("city", extractCityUDF(col("city")))
      .withColumn("bathrooms", extractBaUDF(col("facts and features")))
      .withColumn("sqft", extractSqftUDF(col("facts and features")))
      .withColumn("offer", extractOfferUDF(col("title")))
      .withColumn("price", extractPriceUDF(col("price"), col("offer"), col("facts and features"), col("sqft")))
      .filter(col("price") > 100000 && col("price") < 2e7 && col("offer").like("sale"))
      .select(col("url"), col("zipcode"), col("address"),
              col("city"), col("state"), col("bedrooms"),
              col("bathrooms"), col("sqft"), col("offer"),
              col("type"), col("price"))
      //.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "True")
      .csv(outPath)

    val jobDuration = (System.nanoTime() - tstart) / 1.0e+9
    sc.stop()

    // print out time results
    System.out.println("Spark startup took: " + sparkInitDuration.toString + "s")
    System.out.println("Zillow job took: " + jobDuration.toString + "s")
  }
}