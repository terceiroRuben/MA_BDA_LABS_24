package rplaces

import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RPlacesData {
    def readDataset(spark: SparkSession, path: String): RDD[PaintEvent] = {
        import spark.implicits._
        spark.read.parquet(path).as[PaintEvent].rdd
    }
}
