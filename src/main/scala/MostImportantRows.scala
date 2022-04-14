object MostImportantRows extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import org.apache.spark.sql.functions._
  import spark.implicits._

  val input = Seq(
    (1, "MV2"),
    (1, "MV1"),
    (2, "VPV"),
    (2, "Others"),
    (3, "Others"))
    .toDF("id", "value")

  val priorities = Seq(
    ("Others", 4),
    ("MV1", 1),
    ("VPV", 3),
    ("MV2", 2))
    .toDF("value", "priority")

  val output = input.join(priorities, "value").sort("id", "priority").dropDuplicates("id").show()
}