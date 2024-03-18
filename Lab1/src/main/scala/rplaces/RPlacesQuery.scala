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
        val canvas = Canvas(500, 500) // De base, c'était (2000,2000). J'ai changé car le fichier utilisé pour le moment est 500x500

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

    // Output : 2022-04-04T22:47:37.197Z
    
    //
    // 4. Compute a ranking of colors
    //

    def occurencesOfColor(color: ColorIndex, rdd: RDD[PaintEvent]): Long = {
        rdd.filter(_.pixel_color_index == color).count()
    }

    def rankColors(colors: List[ColorIndex], rdd: RDD[PaintEvent]): List[(ColorIndex, Long)] = {
        colors.map(color => (color, occurencesOfColor(color, rdd))).sortWith(_._2 > _._2)
    }

    // Processing naive ranking took 15242 ms.
    // List((27,1616699), (31,1461618), (2,706722), (12,578426), (4,431632), (13,253347), (3,237898), (18,225761), (14,218502), (8,180505), (6,146267), (1,140549), (29,139909), (23,132582), (25,131534),
    //(30,119143), (19,76291), (24,68159), (26,58186), (7,53937), (28,53001), (22,49074), (15,40300), (9,32348), (5,24705), (17,23087), (0,20461), (21,19174), (16,17895), (20,11626), (10,8137), (11,6533))

    def makeColorIndex(rdd: RDD[PaintEvent]): RDD[(ColorIndex, Iterable[PaintEvent])] = {
        rdd.map(event => (event.pixel_color_index, event)).groupByKey()
    }

    def rankColorsUsingIndex(index: RDD[(ColorIndex, Iterable[PaintEvent])]): List[(ColorIndex, Long)] = {
        index.mapValues(_.size.toLong).collect().toList.sortWith(_._2 > _._2)
    }

    // Processing ranking using inverted index took 6538 ms.
    // List((27,1616699), (31,1461618), (2,706722), (12,578426), (4,431632), (13,253347), (3,237898), (18,225761), (14,218502), (8,180505), (6,146267), (1,140549), (29,139909), (23,132582), (25,131534),
    //(30,119143), (19,76291), (24,68159), (26,58186), (7,53937), (28,53001), (22,49074), (15,40300), (9,32348), (5,24705), (17,23087), (0,20461), (21,19174), (16,17895), (20,11626), (10,8137), (11,6533))

    //Première tentative (classement naïf) : 15242 ms (environ 15,2 secondes)
    //Deuxième tentative (utilisant un index inversé) : 6538 ms (environ 6,5 secondes)

    //Le temps d'exécution est meilleur due à l'efficacité de l'indexation dans la deuxième méthode. La liste de classements des couleurs reste cependant la  même pour les deux méthodes.
    //En utilisant un index, on pointe directement vers où chaque couleur est utilisée, ce qui évite de chercher dans tout le dataset pour chaque couleur. 
    //Cela simplifie le travail en organisant les données une seule fois et permet à Spark de traiter les données plus rapidement.

    def rankColorsReduceByKey(rdd: RDD[PaintEvent]): List[(ColorIndex, Long)] = {
        rdd.map(event => (event.pixel_color_index, 1L)).reduceByKey(_ + _).collect().toList.sortWith(_._2 > _._2)
    }

    //Processing ranking using reduceByKey took 1005 ms.
    //List((27,1616699), (31,1461618), (2,706722), (12,578426), (4,431632), (13,253347), (3,237898), (18,225761), (14,218502), (8,180505), (6,146267), (1,140549), (29,139909), (23,132582), (25,131534), 
    //(30,119143), (19,76291), (24,68159), (26,58186), (7,53937), (28,53001), (22,49074), (15,40300), (9,32348), (5,24705), (17,23087), (0,20461), (21,19174), (16,17895), (20,11626), (10,8137), (11,6533))

    //Il y a une nette amélioration de performance avec reduceByKey qui a mis seulement 1005 ms, contre plus pour les autres méthodes. En gros, reduceByKey travaille directement sur les données
    //pour les combiner de manière intelligente et rapide, sans passer par l'étape de création d'un index comme avant. C'est plus direct et donc plus rapide.

    //
    // 5. Find for the n users who have placed the most pixels, their busiest tile
    //

    def nMostActiveUsers(rdd: RDD[PaintEvent], n: Int = 10): List[(UserId, Long)] = {
        rdd.map(event => (event.id, 1L)).reduceByKey(_ + _).takeOrdered(n)(Ordering[Long].reverse.on(x => x._2)).toList
    }

    //List((85899866930,559), (17180084138,443), (68719685381,432), (42950200594,429), (25770217927,422), (34360264713,420), (103079423385,415), (68719954231,412), (51540074706,406), (17180078961,403))

    def usersBusiestTile(rdd: RDD[PaintEvent], activeUsers: List[UserId], canvas: Canvas): List[(UserId, (TileId, Long))] = {
        // You can use canvas.tileAt(x, y) to know the tile of a coordinate.
        rdd.filter(event => activeUsers.contains(event.id))
            .map(event => ((event.id, canvas.tileAt(event.coordinate_x, event.coordinate_y)), 1L))
            .reduceByKey(_ + _)
            .map { case ((userId, tileId), count) => (userId, (tileId, count)) }
            .groupByKey()
            .mapValues(iter => iter.maxBy(_._2))
            .collect()
            .toList
    }

    //List((42950200594,(185,255)), (25770217927,(167,136)), (17180084138,(169,230)), (68719685381,(169,205)), (34360264713,(177,174)), (51540074706,(176,104)), (85899866930,(197,495)), (103079423385,(199,126)), (17180078961,(195,79)), (68719954231,(167,282)))

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
