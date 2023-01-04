
package com.beam_series.beam_series_scala.model_guide
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
object ptransformParDo {

  class MyDoFn extends DoFn[Map[String,Any],Map[String,Any]] {

    @StartBundle
    def startBundle(c: DoFn[Map[String,Any],Map[String,Any]]#StartBundleContext): Unit = {
      println("Iniciando Bundle....")
    }
    @ProcessElement
    def processElement(@Element element: Map[String,Any], out: OutputReceiver[Map[String,Any]]): Unit = {
      if (element("is_active").asInstanceOf[Boolean] == true) {
        out.output(Map("account" -> element,"status" -> "approved"))
      } else {
        out.output(Map("account" -> element,"status" -> "rejected"))
      }
    }
    
    @FinishBundle
    def finishBundle(c: DoFn[Map[String,Any],Map[String,Any]]#FinishBundleContext): Unit = {
      println("Finalizando Bundle....")
    }
    
  }
  def main(cmdlineArgs: Array[String]): Unit = {

    val accounts: Seq[Map[String,Any]] = Seq(
      Map( 
        "name" -> "Lucas",
        "age" -> 32,
        "gender" -> "M",
        "email" -> "lucas@foo.com",
        "is_active" -> true
        ),
      Map( 
        "name" -> "Fernando",
        "age" -> 38,
        "gender" -> "M",
        "email" -> "fernando@foo.com",
        "is_active" -> true
        ),
      Map( 
        "name" -> "Maria",
        "age" -> 14,
        "gender" -> "F",
        "email" -> "maria@foo.com",
        "is_active" -> false
        )
      )
    val (sc, args) = ContextAndArgs(cmdlineArgs)


    val scoll = sc.parallelize(accounts)

    scoll
      //.filter(x => x("age").asInstanceOf[Int] >= 18)
      //.map(x=> x("email").asInstanceOf[String]) 
      .applyTransform(ParDo.of(new MyDoFn))
      .map(x => println(x))
    
    sc.run()

  }
}
