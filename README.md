# Scala-Advantages

Scala example for handling order fallout in an Apache Spark environment. This example processes failed orders from a dataset, identifies the reasons for fallout, and categorizes them for further analysis.

Scenario:
We have an orders dataset where each record contains an orderId, status, and falloutReason (if applicable). We use Spark to:

Filter out failed orders (status = 'Failed').

Group orders by fallout reason.

Count occurrences of each fallout category.

Scala + Spark Code for Order Fallout Analysis
scala
Copy
Edit
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object OrderFalloutAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Order Fallout Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample order dataset
    val ordersDF = Seq(
      ("O1001", "Success", None),
      ("O1002", "Failed", Some("Payment Issue")),
      ("O1003", "Failed", Some("Address Not Found")),
      ("O1004", "Success", None),
      ("O1005", "Failed", Some("Inventory Shortage")),
      ("O1006", "Failed", Some("Payment Issue")),
      ("O1007", "Failed", Some("Technical Error"))
    ).toDF("orderId", "status", "falloutReason")

    // Filter out failed orders
    val failedOrdersDF = ordersDF.filter($"status" === "Failed")

    // Group by fallout reason and count occurrences
    val falloutSummaryDF = failedOrdersDF
      .groupBy("falloutReason")
      .agg(count("*").alias("falloutCount"))
      .orderBy(desc("falloutCount"))

    // Show results
    falloutSummaryDF.show()

    // Stop Spark session
    spark.stop()
  }
}
Expected Output:
pgsql
Copy
Edit
+------------------+------------+
| falloutReason    |falloutCount|
+------------------+------------+
| Payment Issue    |  2         |
| Address Not Found|  1         |
| Inventory Shortage |  1       |
| Technical Error  |  1         |
+------------------+------------+
Key Features:
✅ Functional Programming: Uses filter, groupBy, and agg functions.
✅ Spark SQL Optimization: Uses DataFrame API for efficient processing.
✅ Scalability: Can handle large-scale order data in a distributed environment.
