/*!
(c) L.Spiegelberg 2019
Scala implementation using DataFrame API for cleaning the zillow files
 */

package tuplex.exp.scala

import java.io.{BufferedWriter, File, FileWriter}

import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.databind.ObjectReader
import com.fasterxml.jackson.dataformat.csv._

import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.CsvSchema
import java.util

// pure scala version of pipeline
object ZillowClean {

  def escape_cell(str: String) : String = {
    if(str.contains(",") || str.contains("\"")) {
      "\"" + str.replace("\"", "\"\"") + "\""
    } else {
      str
    }
  }

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

    val extractBa: String => Option[Double] = v => try {
      val idx = v.indexOf(" ba")

      val max_idx = if (idx < 0) v.length else idx

      // find comma before
      val s = v.substring(0, max_idx)

      val comma_idx = s.lastIndexOf(",")

      val split_idx = if (comma_idx < 0) 0 else comma_idx + 2

      Some((2.0 * s.substring(split_idx).toDouble).ceil /  2.0)
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


    // Initialization
    var tstart = System.nanoTime()


    val initDuration = (System.nanoTime() - tstart) / 1.0e+9
    tstart = System.nanoTime()

    // (2) Zillow Job
    // usage is simply with the csv file...
    if(args.length != 2) {
      System.out.println("invalid number of arguments: " + args.mkString(","))
      System.exit(1)
    }


    val inPath = args(0)
    val outPath = args(1)

    // read CSV file in using Jackson CSV parser
    val csvFile = new File(inPath)
    val mapper = new CsvMapper()
    //    val schema = CsvSchema.builder()
    //      .addColumn("title")
    //    .addColumn("address")
    //    .addColumn("city")
    //    .addColumn("state")
    //    .addColumn("postal_code")
    //    .addColumn("price")
    //    .addColumn("facts and features")
    //    .addColumn("real estate provider")
    //    .addColumn("url")
    //    .addColumn("sales_date").build().withHeader

    val schema = CsvSchema.emptySchema.withHeader // use first row as header; otherwise defaults are fine

    // this works...
    val it = mapper.readerForMapOf(classOf[String]).`with`(schema).readValues(csvFile)

    val file = new File(outPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("url,zipcode,address,city,state,bedrooms,bathrooms,sqft,offer,type,price\n")
    while(it.hasNext) {
      val row = it.next().asInstanceOf[java.util.LinkedHashMap[String, String]].asScala.seq
       // title,address,city,state,postal_code,price,facts and features,real estate provider,url,sales_date
          // access by column name, as defined in the header row...

          // pipeline directly per row
          val bds = extractBd(row.getOrElse("facts and features", ""))
          if(bds < 10) {
            val _type = extractType(row.getOrElse("title", ""))
            if(_type == "condo") {
              val zipcode = extractZipcode(row.getOrElse("postal_code", ""))
              val city = extractCity(row.getOrElse("city", ""))
              val bas = extractBa(row.getOrElse("facts and features", "")).getOrElse(0.0)
              val sqft = extractSqft(row.getOrElse("facts and features", ""))
              val offer = extractOffer(row.getOrElse("title", ""))
              val price = extractPrice(row.getOrElse("price", ""), offer, row.getOrElse("facts and features", ""), sqft)
              val url = row.getOrElse("url", "")
              val address = row.getOrElse("address", "")
              val state = row.getOrElse("state", "")
              if(price > 100000 && price < 2e7 && offer == "sale") {
                //      .select(col("url"), col("zipcode"), col("address"),
                //              col("city"), col("state"), col("bedrooms"),
                //              col("bathrooms"), col("sqft"), col("offer"),
                //              col("type"), col("price"))
                // do not escape the converted types but just the original data taken...
                bw.write(s"${escape_cell(url)},${escape_cell(zipcode)}," +
                  s"${escape_cell(address)},${escape_cell(city)},${escape_cell(state)}," +
                  s"$bds,$bas,$sqft," +
                  s"$offer,${_type},${price}\n")
              }
            }
          }
    }

    bw.flush()
    bw.close()

    val jobDuration = (System.nanoTime() - tstart) / 1.0e+9

    // print out time results
    System.out.println("Scala startup took: " + initDuration.toString + "s")
    System.out.println("Zillow job took: " + jobDuration.toString + "s")
  }
}