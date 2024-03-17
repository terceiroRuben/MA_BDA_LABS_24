package rplaces

object Colors {
  import Types._

  /**
    * A mapping between the hexadecimal representation of the valid colors and their id.
    */
  val hexMapping: Map[ColorHex, ColorIndex] = Seq(
        "#6d001a", "#be0039", "#ff4500", "#ffa800",
        "#ffd635", "#fff8b8", "#00a368", "#00cc78",
        "#7eed56", "#00756f", "#009eaa", "#00ccc0",
        "#2450a4", "#3690ea", "#51e9f4", "#493ac1",
        "#6a5cff", "#94b3ff", "#811e9f", "#b44ac0",
        "#e4abff", "#de107f", "#ff3881", "#ff99aa",
        "#6d482f", "#9c6926", "#ffb470", "#000000",
        "#515252", "#898d90", "#d4d7d9", "#ffffff",
    ).map(_.toUpperCase()).zipWithIndex.toMap

  /**
    * A mapping between the id of the valid color and their hexadecimal representation.
    */
  val idMapping: Map[ColorIndex, ColorHex] = hexMapping.map{case (hex, id) => (id, hex)}

  /**
    * Maps a hexadecimal representation of a color to its integer representation.
    *
    * @param hex The color in hexadecimal, e.g. #ffeedd.
    * @return The color as an integer.
    */
  def hex2rgb(hex: ColorHex): ColorRgb = Integer.parseInt(hex.tail, 16)

  /**
    * Maps a color id to its integer representation.
    *
    * @param id The id of the color.
    * @return The color as an integer.
    */
  def id2rgb(id: ColorIndex): ColorRgb = hex2rgb(Colors.idMapping(id))

  /**
   * Maps a ratio (a value between 0.0 and 1.0) to a grayscale color.
   * 
   * @param d The ratio.
   * @return The grayscale color as an integer.
   */
  def ratio2grayscale(d: Double): ColorRgb = hex2rgb{
      val i = Math.round(d * 255).toInt 
      val h = f"${i}%02X"
      "#" + h * 3
  }
}
