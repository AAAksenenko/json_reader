import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class JsonReader(
                       id: Option[BigInt] = None,
                       country: Option[String] = None,
                       points: Option[Int] = None,
                       price: Option[BigDecimal] = None,
                       title: Option[String] = None,
                       variety: Option[String] = None,
                       winery: Option[String] = None
                     )

object JsonReader {
  def main(args:Array[String]) :Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("json_reader_aksenenko")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
//    val lines = sc.textFile("C:\\Users\\Aleksei\\Downloads\\winemag.json")
    lines.foreach(x => {implicit val formats = DefaultFormats; println(parse(x).extract[JsonReader])})
  }
}