package rplaces

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.time.Instant

case class PaintEvent(
    timestamp: Instant, 
    id: Types.UserId, 
    pixel_color_index: Types.ColorIndex, 
    coordinate_x: Types.CoordX, 
    coordinate_y: Types.CoordY, 
    end_x: Option[Types.CoordX], 
    end_y: Option[Types.CoordY],
)

object RPlacesQuery {

    //
    // 1. Getting started
    //
    val spark: SparkSession = SparkSession.builder()
        .appName("RPlacesQuery")
        .master("local[*]")
        .getOrCreate()
    val sc: SparkContext = spark.sparkContext


    //
    // 2. Read-in r/places Data
    //

    def loadPaintsRDD(): RDD[PaintEvent] = RPlacesData.readDataset(spark, "data/paint_event_500x500.parquet")

    // Change to INFO or DEBUG if need more information / De base WARN
    sc.setLogLevel("WARN")

    def main(args: Array[String]): Unit = {
        val paintsRDD = loadPaintsRDD()
        val q = new RPlacesQuery(spark, sc)
        val canvas = Canvas(500, 500)

        val beforeWhitening = timed("Last colored event", q.lastColoredEvent(paintsRDD))
        println(beforeWhitening)

        val colors = Colors.idMapping.keys.toList
        
        val colorRanked = timed("naive ranking", q.rankColors(colors, paintsRDD))
        println(colorRanked)

        def colorIndex = q.makeColorIndex(paintsRDD)
        val colorRanked2 = timed("ranking using inverted index", q.rankColorsUsingIndex(colorIndex))
        println(colorRanked2)

        val colorRanked3 = timed("ranking using reduceByKey", q.rankColorsReduceByKey(paintsRDD))
        println(colorRanked3)

        val mostActiveUsers = timed("Most active users", q.nMostActiveUsers(paintsRDD))
        println(mostActiveUsers)

        val mostActiveUsersBusiestTile = timed("Most active users busiest tile", q.usersBusiestTile(paintsRDD, mostActiveUsers.map(_._1), canvas))
        println(mostActiveUsersBusiestTile)

        // This can be commented out if you don't want to render an image of the canvas just before the whitening
        val atT = q.colorsAtTime(beforeWhitening, paintsRDD)
        timed("render color before the whitening", canvas.render("color_render_before_whitening", canvas.rddToMatrix(atT, 31), Colors.id2rgb))

        val similarity = timed("color similarity between history and the whitening", q.colorSimilarityBetweenHistoryAndGivenTime(beforeWhitening, paintsRDD).cache())
        println(similarity.take(10).toList)
        // This can be commented out if you don't want to render the similarity
        timed("render color similarity between history and the whitening", canvas.render("similarity_render", canvas.rddToMatrix(similarity, 0.0), Colors.ratio2grayscale))

        /* Output the speed of each query */
        println(timing)

        spark.close()
    }

    val timing = new StringBuffer("Recap:\n")
    def timed[T](label: String, code: => T): T = {
        val start = System.currentTimeMillis()
        val result = code
        val stop = System.currentTimeMillis()
        val s = s"Processing $label took ${stop - start} ms.\n"
        timing.append(s)
        print(s)
        result

    }


}


class RPlacesQuery(spark: SparkSession, sc: SparkContext) {
    import Types._

    //
    // 3. Find the last paint event before the whitening
    //

    def lastColoredEvent(rdd: RDD[PaintEvent]): Instant = {
        val nonWhiteEvents = rdd.filter(_.pixel_color_index != 31)
        val lastNonWhiteEvent = nonWhiteEvents.reduce((event1, event2) => 
            if (event1.timestamp.isAfter(event2.timestamp)) event1 else event2)

        lastNonWhiteEvent.timestamp
    }

    
    //
    // 4. Compute a ranking of colors
    //

    def occurencesOfColor(color: ColorIndex, rdd: RDD[PaintEvent]): Long = {
        rdd.filter(_.pixel_color_index == color).count()
    }

    def rankColors(colors: List[ColorIndex], rdd: RDD[PaintEvent]): List[(ColorIndex, Long)] = {
        colors.map(color => (color, occurencesOfColor(color, rdd)))
            .sortWith(_._2 > _._2)
    }

    def makeColorIndex(rdd: RDD[PaintEvent]): RDD[(ColorIndex, Iterable[PaintEvent])] = {
        ???
    }

    def rankColorsUsingIndex(index: RDD[(ColorIndex, Iterable[PaintEvent])]): List[(ColorIndex, Long)] = {
        ???
    }

    def rankColorsReduceByKey(rdd: RDD[PaintEvent]): List[(ColorIndex, Long)] = {
        ???
    }


    //
    // 5. Find for the n users who have placed the most pixels, their busiest tile
    //

    def nMostActiveUsers(rdd: RDD[PaintEvent], n: Int = 10): List[(UserId, Long)] = {
        ???
    }

    def usersBusiestTile(rdd: RDD[PaintEvent], activeUsers: List[UserId], canvas: Canvas): List[(UserId, (TileId, Long))] = {
        // You can use canvas.tileAt(x, y) to know the tile of a coordinate.
        ???
    }


    //
    // 6. Find the percentage of painting event at each coordinate which have the same color as just before the whitening
    //

    def colorsAtTime(t: Instant, rdd: RDD[PaintEvent]): RDD[((CoordX, CoordY), ColorIndex)] = {
        ???
    }

    def colorSimilarityBetweenHistoryAndGivenTime(t: Instant, rdd: RDD[PaintEvent]): RDD[((CoordX, CoordY), Double)] = {
        ???
    }

}
