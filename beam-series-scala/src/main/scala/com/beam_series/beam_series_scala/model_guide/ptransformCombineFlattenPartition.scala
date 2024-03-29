
package com.beam_series.beam_series_scala.model_guide
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo, Combine}
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.values.KV
import scala.collection.JavaConverters._
import java.lang
import scala.util.Random
object ptransformCombineFlattenPartition {
  
  def myPartitionFn(value: Map[String,Any]): Int = {
    Random.nextInt(3)
  }
  class LastDateFn extends CombineFn[Map[String,Any],String,String] {
    override def createAccumulator(): String = "0000-00-00"
    override def addInput(acc: String, input: Map[String,Any]): String = {
      if (acc >= input("joining_date").asInstanceOf[String]) {
        acc
      } else {
        input("joining_date").asInstanceOf[String]
      }
    }

    override def mergeAccumulators(acc: java.lang.Iterable[String]): String = acc.asScala.max

    override def extractOutput(acc: String): String = acc
  }
  val employees: Seq[Map[String,Any]] = Seq(
      Map( 
        "id" -> 0,
        "name" -> "John C",
        "joining_date" -> "2000-01-01",
        "dept_id" -> "D101",
        "is_active" -> true
        ),
      Map( 
        "id" -> 1,
        "name" -> "Tom D",
        "joining_date" -> "2002-02-01",
        "dept_id" -> "D102",
        "is_active" -> true
        ),
      Map( 
        "id" -> 2,
        "name" -> "Max",
        "joining_date" -> "2003-04-01",
        "dept_id" -> "D104",
        "is_active" -> false
        ),
      Map( 
        "id" -> 3,
        "name" -> "Bruce",
        "joining_date" -> "2003-05-01",
        "dept_id" -> "D102",
        "is_active" -> false
        ),
      Map( 
        "id" -> 4,
        "name" -> "Barry",
        "joining_date" -> "2003-04-2",
        "dept_id" -> "D101",
        "is_active" -> false
        ),
      Map( 
        "id" -> 5,
        "name" -> "Clark",
        "joining_date" -> "2001-04-01",
        "dept_id" -> "D103",
        "is_active" -> false
        ),
      Map( 
        "id" -> 6,
        "name" -> "Bryan C.",
        "joining_date" -> "2010-07-01",
        "dept_id" -> "D104",
        "is_active" -> true
        )
      )



  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)


    val employees_scoll = sc.parallelize(employees)

    val max_id_employee: SCollection[Int] = employees_scoll
      .map(x => x("id").asInstanceOf[Int])
      .max(Ordering[Int])

    val employees_kv = employees_scoll.keyBy(x => x("dept_id"))


    val max_id_by_dept = employees_kv
      .mapValues(x=> x("id").asInstanceOf[Int])
      .maxByKey(Ordering[Int])

    val max_id_by_dept_simple = employees_kv
      .mapValues(x=> x("id").asInstanceOf[Int])
      .combineByKey(x=>x)( (acc,value) => {
        if (value >= acc) {
          value
        } else {
          acc
        }
      })((acc1,acc2) => {
        if (acc1 >= acc2) {
          acc1
        } else {
          acc2
        }
      })


    val last_joining_date_by_dept_complex = employees_kv
      .map { case (k, v) => KV.of(k,v)}
      .applyTransform(Combine.perKey(new LastDateFn))
   

    val scoll_groups = employees_scoll
      .partition(3,myPartitionFn)

    val union_scoll = sc.unionAll(scoll_groups)


    union_scoll
      .map(x => println(x))
    
    sc.run()

  }
}
