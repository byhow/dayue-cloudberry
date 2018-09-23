package edu.uci.ics.cloudberry.zion.model.impl

import edu.uci.ics.cloudberry.zion.model.datastore.IDataConn

import play.api.Logger
import play.api.libs.json._
import play.api.libs.ws._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, Await}

class ElasticsearchConn(url: String, wSClient: WSClient)(implicit ec: ExecutionContext) extends IDataConn {

  import ElasticsearchConn._

  // defaultEmptyResponse is defined in companion object (like static in Java)
  // defaultEmptyResponse = Json.toJson(Seq(Seq.empty[JsValue]))
  override def defaultQueryResponse: JsValue = defaultEmptyResponse

  // TODO: value field and meta post need changing
  def postQuery(query: String): Future[JsValue] = {
    println("call postQuery")
    query match {
      case berry if query.contains("berry.meta") => postWithCheckingStatus(query, metaPost, (ws: WSResponse, query: String) => {parseQueryResponse((ws.json \ "hits" \ "hits").get, query)}, (ws: WSResponse, query: String) => {defaultQueryResponse})
      case _ => postWithCheckingStatus(query, post, (ws: WSResponse, query: String) => {parseQueryResponse(ws.json, query)}, (ws: WSResponse, query: String) => defaultQueryResponse)
    }
  }

  def postControl(query: String): Future[Boolean] = {
    println("call postControl")
    query match {
      case berry if query.contains("berry.meta") || query.contains("doc_as_upsert") => postWithCheckingStatus(query, metaPost, (ws: WSResponse, query: String) => true, (ws: WSResponse, query: String) => parseMetaControlResponse(ws, query))
      case _  => postWithCheckingStatus(query, post, (ws: WSResponse, query: String) => parseControlResponse(ws, query), (ws: WSResponse, query: String) => parseControlResponse(ws, query))
    }
  }

  protected def postWithCheckingStatus[T](query: String, post: String => Future[WSResponse], succeedHandler: (WSResponse, String) => T, failureHandler: (WSResponse, String) => T): Future[T] = {
    post(query).map { wsResponse =>
      if (wsResponse.status == ElasticsearchConn.normalResponseStatus) {
        println(wsResponse.json)
        succeedHandler(wsResponse, query)
      }
      else {
        println("Query failed: " + wsResponse.body)
        println("Query response status code: " + wsResponse.status)

        failureHandler(wsResponse, query)
      }
    }
  }

  def metaPost(query: String): Future[WSResponse] = {
    println("call metaPost function")
    var method = ""
    var urlSuffix = ""
    var stringQuery = ""
    var jsonQuery: JsObject = Json.obj()

    if (query.contains("doc_as_upsert")) {
      method = "upsert"
      urlSuffix = berryMetaUrlSuffix + upsertUrlSuffix
      stringQuery= Json.parse(query).as[List[JsValue]].mkString("", "\n", "\n")
    } else {
      jsonQuery = Json.parse(query).as[JsObject]
      method = (jsonQuery \ "method").get.toString().stripPrefix("\"").stripSuffix("\"")
      println("method to use: " + method)

      jsonQuery -= "method"
      jsonQuery -= "dataset"

      urlSuffix = method match {
        case "create" => berryMetaUrlSuffix
        case "search" => berryMetaUrlSuffix + "/_search"
        case "check create existence" => berryMetaUrlSuffix
      }
    }

    println("Query url been posted: " + url + urlSuffix)

    val request = wSClient.url(url+urlSuffix).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf)
    // if no data need posting, use Array.empty[Byte]
    val response = method match {
      case "create" => request.put(jsonQuery)
      case "search" => {
        if ((jsonQuery \ "size").asOpt[Int].isEmpty)
          jsonQuery += ("size" -> JsNumber(maxResultWindow))
        request.post(jsonQuery)
      }
      case "upsert" => request.post(stringQuery)
      case "check create existence" => request.get()
    }

    // For debugging
    response.onFailure(wsFailureHandler(query))

    // Return response
    response
  }

  // TODO: Parse the result into the desired format
  def post(query: String): Future[WSResponse] = {
    println("call post function")
    var method = ""
    var datasetUrl = ""
    var stringQuery = ""
    var jsonQuery: JsObject = Json.obj()

    if (query.startsWith("[{\"index\":{\"_id\":")) {
      val queryList = query.split('\n')
      val q = queryList(0)
      datasetUrl = queryList(1)
      method = "index"
      stringQuery= Json.parse(q).as[List[JsValue]].mkString("", "\n", "\n")
    } else {
      jsonQuery = Json.parse(query).as[JsObject]
      method = (jsonQuery \ "method").get.toString().stripPrefix("\"").stripSuffix("\"")
      datasetUrl = "/" + (jsonQuery \ "dataset").get.toString().stripPrefix("\"").stripSuffix("\"")

      jsonQuery -= "method"
      jsonQuery -= "dataset"
      println("jsonQuery: " + jsonQuery)
    }
    println("method to use: " + method)
    println("dataset: " + datasetUrl)


    val urlSuffix = method match {
      case "search" => datasetUrl + searchUrlSuffix
      case "min" => datasetUrl + searchUrlSuffix
      case "max" => datasetUrl + searchUrlSuffix
      case "group by aggregation" => {jsonQuery -= "aggr"; datasetUrl + searchUrlSuffix}
      case "global count without group" => datasetUrl + countUrlSuffix
      case "check drop existence" => datasetUrl
      case "drop" => datasetUrl
      case "create" => datasetUrl
      case "index" => datasetUrl + upsertUrlSuffix
    }

    println("Query url been posted: " + url + urlSuffix)

    // Send a GET request
    val request = wSClient.url(url + urlSuffix).withHeaders(("Content-Type", "application/json")).withRequestTimeout(Duration.Inf)

    val response = method match {
      case "create" => {jsonQuery -= "query"; request.put(jsonQuery)} // To create view
      case "check drop existence" => request.get() // Check if view already exists
      case "drop" => request.delete() // Drop view
      case "index" => request.post(stringQuery) // Index document into dataset
      case "search" => {
        if ((jsonQuery \ "size").asOpt[Int].isEmpty)
          jsonQuery += ("size" -> JsNumber(maxResultWindow))
        request.post(jsonQuery)
      }
      case _ => request.post(jsonQuery)
    }

    // Debug
    response.onFailure(wsFailureHandler(query))

    // Return response
    response
  }

  protected def wsFailureHandler(query: String): PartialFunction[Throwable, Unit] = {
    case e: Throwable => Logger.error("WS Error:" + query, e);
      throw e
  }

  private def parseQueryResponse(in: JsValue, query: String): JsValue = {

    val jsonQuery = Json.parse(query).as[JsObject]
    var method = (jsonQuery \ "method").get.toString().stripPrefix("\"").stripSuffix("\"")
    var out = Json.arr()

    method match {
      case "global count without group" => {
        out = out :+ Json.obj("count" -> (in \ "count").get)
      }
      case "min" => {
        out = out :+ Json.obj("min" -> (in \ "aggregations" \ "min" \ "value_as_string").get)
      }
      case "max" => {
        out = out :+ Json.obj("max" -> (in \ "aggregations" \ "max" \ "value_as_string").get)
      }
      case "search" => {
        val inList = in.as[List[JsValue]]

        for (element <- inList) {
          val _source = (element \ "_source").get
          out = out :+ _source
        }
      }
      case "group by aggregation" => {
        val buckets = (in \ "aggregations" \ "group_by" \ "buckets").get.as[List[JsValue]]
        val field = (jsonQuery \ "aggr").get.toString()
        for (bucket <- buckets) {
          val id = (bucket \ "key").get.as[Int]
          (bucket \ "sub_group_by" \ "buckets").asOpt[List[JsValue]] match {
            case Some(subBuckets) =>
              for (subBucket <- subBuckets) {
                val day = (subBucket \ "key_as_string").get.toString()
                val count = (subBucket \ "doc_count").get.as[Int]
                val result = Json.parse(s"""{$field:$id, "day":$day, "count":$count}""")
                out = out :+ result
              }
            case None =>
              val count = (bucket \ "doc_count").get.as[Int]
              val population = 1
              val result = Json.parse(s"""{$field:$id, "count":$count, "population":$population}""")
              out = out :+ result
          }
        }
      }
    }
    out
  }

  private def parseControlResponse(response: WSResponse, query: String): Boolean = {
    val statusCode = response.status

    if (statusCode == normalResponseStatus || statusCode == notFoundResponseStatus) {
      if (query.contains("check drop existence")) {
        var jsonQuery = Json.parse(query).as[JsObject]
        val dataset = "/" + (jsonQuery \ "dataset").get.toString().stripPrefix("\"").stripSuffix("\"")
        if (statusCode == normalResponseStatus) {
          jsonQuery -= "method"
          jsonQuery += ("method" -> JsString("drop"))
          postWithCheckingStatus(jsonQuery.toString(), post, (ws: WSResponse, query: String) => true, (ws: WSResponse, query: String) => false)
          println("droped!!!")
        }
        jsonQuery -= "method"
        jsonQuery += ("method" -> JsString("create"))
        postWithCheckingStatus(jsonQuery.toString(), post, (ws: WSResponse, query: String) => true, (ws: WSResponse, query: String) => false)
        println("created!!!")
        val selectRecordQuery = (jsonQuery \ "query").get
        println("selectRecordQuery: " + selectRecordQuery)
        postWithCheckingStatus(selectRecordQuery.toString(), post, (ws: WSResponse, query: String) => {parseQueryResponse((ws.json \ "hits" \ "hits").get, query)}, (ws: WSResponse, query: String) => defaultQueryResponse).map { records =>
          val recordList = records.as[List[JsValue]]
          var queryBuilder = Json.arr()
          for (record <- recordList) {
            val id = (record \ "id").get.toString()
            queryBuilder = queryBuilder :+ Json.obj(("index" -> Json.obj("_id" -> JsString(id))))
            queryBuilder = queryBuilder :+ record.as[JsObject]
          }
          postWithCheckingStatus(queryBuilder.toString() + "\n" + dataset, post, (ws: WSResponse, query: String) => true, (ws: WSResponse, query: String) => false)
        }
        true
      } else {
        statusCode == normalResponseStatus
      }
    } else {
      false
    }
  }

  private def parseMetaControlResponse(response: WSResponse, query: String): Boolean = {
    val statusCode = response.status

    if (statusCode == notFoundResponseStatus) { // Dataset to create not exists
      println("index not found")
      var jsonQuery = Json.parse(query).as[JsObject]
      jsonQuery -= "method"
      jsonQuery += ("method" -> JsString("create"))
      println("second query: " + jsonQuery)
      postWithCheckingStatus(jsonQuery.toString(), metaPost, (ws: WSResponse, query: String) => true, (ws: WSResponse, query: String) => false)
      println("created!!!")
      true
    } else {
      false
    }
  }
}

object ElasticsearchConn {
  val defaultEmptyResponse: JsValue = Json.toJson(Seq(Seq.empty[JsValue]))
  val normalResponseStatus: Int = 200
  val notFoundResponseStatus: Int = 404
  val maxResultWindow: Int = 2147483647
  val berryMetaUrlSuffix: String = "/berry.meta"
  val searchUrlSuffix: String = "/_search"
  val countUrlSuffix: String = "/_count"
  val upsertUrlSuffix: String = "/_doc/_bulk"
}