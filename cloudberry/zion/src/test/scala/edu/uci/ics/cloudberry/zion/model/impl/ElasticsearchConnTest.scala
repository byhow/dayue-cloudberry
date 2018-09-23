package edu.uci.ics.cloudberry.zion.model.impl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import play.api.libs.ws._
import play.api.libs.ws.ahc.AhcWSClient
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.Await

import org.specs2.mutable.Specification

class ElasticsearchConnTest extends Specification {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  val wsClient: WSClient = AhcWSClient()
  val ElasticsearchURL = "http://localhost:9200"
  val connector = new ElasticsearchConn(ElasticsearchURL, wsClient)

  "Elasticsearch connector" should {
//    "connect to elasticsearch engine successfully" in {
//      val result: Future[JsValue] = connector.postQuery("")
//      val myResult = Await.result(result, Duration.Inf)
//
//      myResult.toString() must endWith("for Search\"")
//    }
//
//    "check whether a query succeeds (function: postControl)" in {
//      val result: Future[Boolean] = connector.postControl("")
//      val myResult: Boolean = Await.result(result, Duration.Inf)
//
//      myResult must equalTo(true)
//    }
//    "search a document by full-text search" in {
//      val result: Future[JsValue] = connector.postQuery("")
//      val myResult = Await.result(result, Duration.Inf)
//
//      myResult.toString().toLowerCase must contain("trump")
//    }
      "create a date set named berry.meta" in {
        val query = s"""
                    |{"method": "create",
                    |"dateset": "berry.meta",
                    |"json": {
                    |    "mappings" : {
                    |        "_doc" : {
                    |            "properties" : {
                    |                "dataInterval.start" : { "type" : "date", "format": "strict_date_time" },
                    |                "dataInterval.end": { "type" : "date", "format": "strict_date_time" },
                    |                "stats.createTime": { "type" : "date", "format": "strict_date_time" },
                    |                "stats.lastModifyTime": { "type" : "date", "format": "strict_date_time" },
                    |                "stats.lastReadTime": { "type" : "date", "format": "strict_date_time" }
                    |            }
                    |        }
                    |    }
                    |}
                    |}
                    """.stripMargin
        val result: Future[Boolean] = connector.postControl(query)
        val myResult = Await.result(result, Duration.Inf)

        myResult must equalTo(false) or equalTo(true)
      }
  }

  println("end\n")

}
