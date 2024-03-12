package rplaces

import java.awt.image.BufferedImage
import java.awt.image.IndexColorModel
import java.io.File
import javax.imageio.ImageIO
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

case class Canvas(val width: Int, val height: Int) {
    import Types._
    
    /**
      * Calculate the tile associated with a given pixel.
      * 
      * The canvas is divided in 10x10 tiles:
      *
      *        0px  200px          2000px
      *    0px +----+----+    +----+
      *        |  0 |  1 | ⋯ |  9 |
      *  200px +----+----+    +----+
      *        | 10 | 11 | ⋯ | 19 |
      *        +----+----+    +----+
      *           ⋮   ⋮   ⋱   ⋮
      *        +----+----+    +----+
      *        | 90 | 91 | ⋯ | 99 |
      * 2000px +----+----+    +----+
      * 
      * @param x The x coordinate of the pixel
      * @param y The y coordinate of the pixel
      * @return the tile number of the pixel
      */
    def tileAt(x: Int, y: Int): Int = {
        val (tx, ty) = (10, 10)
        val col = Math.floor((x.toDouble / width) * tx).toInt
        val row = Math.floor((y.toDouble / height) * ty).toInt
        row * tx + col
    }
    
    /**
      * Render the given matrix as a TIFF image file.
      *
      * @param filename The filename without the extension
      * @param matrix The matrix to render
      * @param valueToColor The function that maps the value of a matrix cell to its color
      */
    def render[T](filename: String, matrix: Array[Array[T]], valueToColor: (T) => ColorRgb): Unit = {
      assert(matrix.length == height, "Matrix should have same height as the canvas")
      assert(matrix.forall(_.length == width), "Matrix should have same width as the canvas")

      val bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
      for {
        (line, y) <- matrix.zipWithIndex
        (pixel, x) <- line.zipWithIndex
      } bufferedImage.setRGB(x, y, valueToColor.apply(pixel))
      val file = new File(filename + ".tif");
      ImageIO.write(bufferedImage, "TIFF", file);
    }

    def emptyMatrix[T : ClassTag](defaultValue: T): Array[Array[T]] = 
        Array.fill(height)(Array.fill(width)(defaultValue))
    
    /**
      * Transform a RDD with coordinate and a value to a matrix of values.
      *
      * @param rdd A pair RDD with the coordinate as the key.
      * @param defaultValue The value of the matrix cell if it isn't specified in the rdd.
      * @return A matrix of values
      */
    def rddToMatrix[T : ClassTag](rdd: RDD[((CoordX, CoordY), T)], defaultValue: T): Array[Array[T]] = {
        val matrix = emptyMatrix(defaultValue)

        rdd.collect().foreach{case ((x, y), p) => matrix(y)(x) = p}

        matrix
    }
}
