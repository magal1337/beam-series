
package com.beam_series.beam_series_scala.model_guide
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
object ptransformGroupOps {
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

    val dept : Seq[Map[String,Any]] = Seq(
      Map(
        "id" -> "D101",
        "dept_name" -> "Support"
        ),
      Map(
        "id" -> "D102",
        "dept_name" -> "HR"
        ),
      Map(
        "id" -> "D103",
        "dept_name" -> "Marketing"
        ),
      Map(
        "id" -> "D104",
        "dept_name" -> "Sells"
        )
  )

  def combFunc(x: String, y: Any): String = {
    if (x > y.asInstanceOf[String]) {
      y.toString()
    } else {
      x
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)


    val employees_scoll = sc.parallelize(employees)
    val dept_scoll = sc.parallelize(dept)

    val employees_kv = employees_scoll.keyBy(x => x("dept_id"))
    val dept_kv = dept_scoll.keyBy(x => x("id"))
    
    val joined_cogroup = employees_kv.leftOuterJoin(dept_kv)

    val flat_join = joined_cogroup.map(x=> {
      val dept_name = x._2._2.get("dept_name")
      val employee_date = x._2._1("joining_date")
      (dept_name,employee_date)
    })

    val min_joining_date_grouped_by_dept = flat_join
      .groupByKey
      .map( x=> {
        val iterable_dates: Iterable[String] = x._2.asInstanceOf[Iterable[String]]
        (x._1,iterable_dates.min(Ordering[String]))
      })
    
    val min_joining_date_grouped_by_dept_2 = flat_join
      .aggregateByKey("9999-99-99")(combFunc,combFunc)

    val min_joining_date_grouped_by_dept_3 = flat_join
      .reduceByKey( (x,y) => {
        if(x.asInstanceOf[String] > y.asInstanceOf[String]) {
          y.toString()
        } else {
          x.toString()
        }
      })

    val min_joining_date_grouped_by_dept_4 = flat_join
      .mapValues(_.asInstanceOf[String])
      .minByKey(Ordering[String])
    

    min_joining_date_grouped_by_dept_4
      .map(x => println(x))
    
    sc.run()

  }
}
