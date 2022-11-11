package com.beam_series.beam_series_scala.model_guide

import com.spotify.scio._

object pplPcoll {
  def main(cmdlineArgs: Array[String]): Unit = {
      // Scala Seq of Integers
      //val data_list: Seq[Int] = 1 to 10 toSeq
      
      // Scala Seq of Strings
      //val data_list: Seq[String] = Seq("banana","orange","apple")
      
      // Scala Seq of Maps
      /*
      val data_list: Seq[Map[String,Any]] = Seq(
        Map(
          "name" -> "Fernando",
          "age" -> 13,
          "gender" -> "Male"
          ),
        Map(
          "name" -> "Lucas",
          "age" -> 29,
          "gender" -> "Male"
          )
        )
      */
    
    // element type restriction
     //val data_list: Seq[Any] = Seq(1,2,3,"banana",4)

     // dynamic scollection creation

      val (sc, args) = ContextAndArgs(cmdlineArgs)
      val pcollSize: Int = args("pcollSize").toInt
      val data_list: Seq[Int] = generateSeq(pcollSize)
      val scoll = sc.parallelize(data_list)

      scoll
        .map(println)

      sc.run()
  }

  def generateSeq(element: Int): Seq[Int] = {
    return 1 to element toSeq

  }
}
