package com.beam_series.beam_series_scala.model_guide
import com.spotify.scio._
import com.spotify.scio.values.SCollection

object pplPcoll {
  def main(cmdlineArgs: Array[String]): Unit = {

    // scollection of integer
    //val data_list: Seq[Int] = Seq(1,2,3,4)
    //scollection of strings
    //val data_list: Seq[String] = Seq("banana","orange","apple")
    // scollection of Maps
    /*
    val data_list: Seq[Map[String,Any]] = Seq(
      Map( 
        "name" -> "Lucas",
        "gende" -> "M",
        "age" -> 30
        ),
      Map( 
        "name" -> "Alice",
        "gende" -> "F",
        "age" -> 28
        )
      )
    */
    // scollection mix
    //val data_list : Seq[Any] = Seq("banana","apple",4,"orange")
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val pcollSize = args("pcollSize").toInt

    val data_list: Seq[Int] = generateSeq(pcollSize)
    val scoll = sc.parallelize(data_list)


    scoll.map(x => println(x))
    
    sc.run()

  }
  def generateSeq(element: Int): Seq[Int] = {
    return 1 to element toSeq
  }
}
