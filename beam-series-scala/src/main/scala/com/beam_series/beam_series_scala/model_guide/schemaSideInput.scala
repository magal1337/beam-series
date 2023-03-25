
package com.beam_series.beam_series_scala.model_guide
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.coders.IterableCoder
import _root_.com.twitter.algebird.Aggregator
import scala.collection.SortedSet
import com.twitter.algebird.MinAggregator
import com.spotify.scio.values.SCollectionWithSideInput
object schemaSideInput {
  case class Account(
    name: String,
    age: Int,
    gender: String,
    email: String
  )
    
  case class Scores(
    account: String,
    date: String,
    score: Int
  )

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)


    val accounts_scoll: SCollection[Account] = sc
      .textFile("./data/model_guide/accounts.csv")
      .filter(x => !x.contains("name,age,gender,email"))
      .map(row => {
          val record: Seq[String] = row.split(",")
          new Account(
            name=record(0),
            age=record(1).toInt,
            gender=record(2),
            email=record(3)
          )
      })
    
      val scores_scoll: SCollection[Scores] = sc
      .textFile("./data/model_guide/scores.csv")
      .filter(x => !x.contains("Account,date,score"))
      .map(row => {
          val record: Seq[String] = row.split(",")
          new Scores(
            account=record(0),
            date=record(1),
            score=record(2).toInt
          )
      })

    val accounts_max_age = accounts_scoll.map(_.age).max(Ordering[Int])
    
    val accounts_side_input = accounts_scoll.asListSideInput

    val accounts_score = scores_scoll
      .withSideInputs(accounts_side_input)
      .map{ case (score_entity,side) => {
        val accounts = side(accounts_side_input)
        val target_name = accounts.filter(account => account.email == score_entity.account)(0) 
        (target_name.name,score_entity.score)
      }
    }.toSCollection

    val max_score_by_user = accounts_score.maxByKey(Ordering[Int])


    max_score_by_user 
      .map(println(_))
    
    sc.run()

  }
}
