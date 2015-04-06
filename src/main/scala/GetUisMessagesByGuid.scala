import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object GetUisMessagesByGuid {
  def main(args: Array[String]): Unit = {

    var bookingCompleteGuidPath: String = args(0)
    var inputPath: String = args(1)
    var outputPath: String = args(2)
    val conf = new SparkConf().setAppName("Get Booking Ids")
    val spark = new SparkContext(conf)
    val bookingCompletedGuis = spark.textFile(bookingCompleteGuidPath).takeSample(false,50,0)

    val allGuids = spark.textFile(inputPath,16)
    val filteredUisMessages = allGuids.filter(line => bookingCompletedGuis.exists(line.contains))
      .map(value => (extractBookingIdAndTimeStamp(value))) //

//    val uisGropedByGuidSortedByTimeStamp  = filteredUisMessages.groupBy(_._1).mapValues {triples =>
//      val sortedTriples = triples.toList.sortBy(_._2) //
//      val keepOnlyProducts = sortedTriples.map(_._3)
//      val uisByNewLine = keepOnlyProducts.reduceLeft(_ + "\n" + _)
//      uisByNewLine
//
//    }

    val uisGropedByGuidSortedByTimeStamp=filteredUisMessages.reduceByKey(_+ "\n" + _)

    uisGropedByGuidSortedByTimeStamp.saveAsHadoopFile(outputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

  }

  def extractBookingIdAndTimeStamp(line: String): (String, String) = {

    var mapper: ObjectMapper = new ObjectMapper()
    val uisMessageJson = mapper.readTree(line)
    val contextObject = uisMessageJson.get("context")
    var guid=""
    if(contextObject!=null && contextObject.get("user")!=null)
    {
     guid = uisMessageJson.get("context").get("user").get("guid").asText()
    }
    //var utcTimestamp = mapper.readTree(line).get("utcTimestamp").asText()
    return (guid,line)

  }

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]
  }



}
