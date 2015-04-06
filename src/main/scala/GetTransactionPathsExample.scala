import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

class GetTransactionPathsExample {
  def main(args: Array[String]): Unit = {

    var bookingCompleteGuidPath: String = args(0)
    var inputPath: String = args(1)
    var outputPath: String = args(2)
    val conf = new SparkConf().setAppName("Get Booking Ids")
    conf.set("spark.hadoop.validateOutputSpecs", "false");
    conf.set("spark.shuffle.consolidateFiles", "true");
    conf.set("spark.default.parallelism", "96");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.kryoserializer.buffer.mb", "200");
    conf.set("spark.storage.memoryFraction", "0.2");
    val spark = new SparkContext(conf)
    val bookingCompletedGuis = spark.textFile(bookingCompleteGuidPath).collect()

    val allGuids = spark.textFile(inputPath)
    val filteredUisMessages = allGuids.filter(line => bookingCompletedGuis.exists(line.contains))
      .map(value => (extractBookingIdAndTimeStamp(value))) //

    val uisGropedByGuidSortedByTimeStamp  = filteredUisMessages.groupBy(_._1).mapValues {triples =>
    val sortedTriples = triples.toList.sortBy(_._2) // assuming rating has an Ordering, e.g., it's an Int
    val keepOnlyProducts = sortedTriples.map(_._3)
    val uisByNewLine = keepOnlyProducts.reduceLeft(_ + "\n" + _)
    uisByNewLine

    }

    uisGropedByGuidSortedByTimeStamp.map((v)=> (v._2,1)).reduceByKey(_+_).map((value)=> (value._2,value._1)).sortByKey(false)
    .saveAsTextFile(outputPath)

   // uisGropedByGuidSortedByTimeStamp.saveAsHadoopFile(outputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

  }

  def extractBookingIdAndTimeStamp(line: String): (String, String,String) = {

    var mapper: ObjectMapper = new ObjectMapper()
    val uisMessageJson = mapper.readTree(line)
    var guid = uisMessageJson.get("context").get("user").get("guid").asText()
    var utcTimestamp = mapper.readTree(line).get("utcTimestamp").asText()
    return (guid, utcTimestamp,line)

  }

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]
  }




}
