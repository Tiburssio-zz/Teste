import java.sql.Timestamp

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Semantix {

    val path = "D:\\Repositories\\dados\\nasa\\json"

    def main(args: Array[String]): Unit = {

        var appName = "Semantix"

        val conf: SparkConf = new SparkConf()
            .setAppName(appName)
            .setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        val spark: SparkSession = SparkSession.builder()
            .config(sc.getConf)
            .getOrCreate()

        import spark.implicits._

        val udfParseTimestamp = udf(parseTimestamp(_: String): Timestamp)

        val text: Dataset[String] = spark.read.text(path).as[String]

        val df: DataFrame = text
            .map(x => {

                val splitted_0 = x.split(" - - \\[")
                val splitted_1 = splitted_0.last.split("] \"")
                val splitted_2 = splitted_1.last.split("\" ")
                val splitted_3 = splitted_2.last.split(" ")

                (splitted_0(0), splitted_1(0), splitted_2(0), splitted_3(0), splitted_3.last)

            })
            .toDF("host", "timestamp", "request", "code", "bytes")
            .cache()

        val ds: Dataset[Nasa] = df
            .withColumn("timestamp", udfParseTimestamp(col("timestamp")).cast(DataTypes.TimestampType))
            .withColumn("code", col("code").cast(DataTypes.IntegerType))
            .withColumn("bytes", col("bytes").cast(DataTypes.LongType))
            .as[Nasa]

        val uniqueHost = ds
            .select(col("host"))
            .distinct()
            .count()

        println(s"1) Total de Hosts unicos: ${uniqueHost}")

        val errorCode = ds
            .filter(col("code").equalTo(404))
            .select(col("code"))
            .count()

        println(s"2) Total de erros: ${errorCode}")

        val hostOfErrors = ds
            .filter(col("code").equalTo(404))
            .select(col("host"))
            .groupBy(col("host"))
            .count()
            .orderBy(col("count").desc)
            .limit(5)

        println("3) Hosts com maiores erros:")
        hostOfErrors.show(false)

        val errorsPerDay = ds
            .filter(col("code").equalTo(404))
            .select(col("timestamp").cast("Date"))
            .groupBy(col("timestamp"))
            .count()
            .orderBy(col("timestamp").asc)

        println("4) Erros por dia:")
        errorsPerDay.show(100, false)

        val bytes = ds
            .select(sum(col("bytes")))
            .as[Long]
            .first()

        println(s"5) Total de bytes: ${bytes}")
    }

    def parseTimestamp(value: String): Timestamp = {
        val parser = new java.text.SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss")
        val tsSplitted = value.split(" ")
        val dtDateTime = tsSplitted(0)

        val timezone = tsSplitted(1).toLong
        val hour = (timezone.toInt / 100)
        val minute = (timezone - hour * 100).toInt
        val offset = hour * 60 * 60 + minute * 60

        new Timestamp((parser.parse(dtDateTime).getTime() + offset * 1000))
    }

    case class Nasa(host: String, timestamp: Timestamp, request: String, code: Int, bytes: Long)

}
