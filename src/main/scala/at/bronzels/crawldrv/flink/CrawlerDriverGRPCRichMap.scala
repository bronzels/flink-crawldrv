package at.bronzels.crawldrv.flink

import at.bronzels.crawldrv.CrawlCategoryEnum
import at.bronzels.crawldrv.CrawlCategoryEnum.CrawlCategoryEnum
import at.bronzels.crawler.{CrawlNewsRequest, CrawlerGrpc, Logflag}
import at.bronzels.libcdcdwstr.flink.util.MyJackson
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.util.Collector

class CrawlerDriverGRPCRichMap(val host: String, val port: Int) extends RichFlatMapFunction[JsonNode, (Int, String)] {
  private lazy val crawlNewsStubHolder = new ThreadLocal[CrawlerGrpc.CrawlerBlockingStub]

  private def getStubCrawNews: CrawlerGrpc.CrawlerBlockingStub = {
    val channel = io.grpc.ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext()
      .build()
    CrawlerGrpc.newBlockingStub(channel)
  }

  @Override
  def flatMap(jsonNode: JsonNode, out: Collector[(Int, String)]) = {
    val category = CrawlCategoryEnum.apply(MyJackson.get(jsonNode, "CrawlCategoryEnum").asInstanceOf[java.lang.Integer])
    import scala.collection.JavaConversions._
    if(category.equals(CrawlCategoryEnum.News)) {
      val arrNode = jsonNode.get("Queries2Extract")
      val strList = arrNode.elements().map(node => node.asText()).toList
      val crawlNewsRequestBuilder = CrawlNewsRequest.newBuilder()
        .setMyasync(MyJackson.get(jsonNode, "Myasync").asInstanceOf[java.lang.Boolean])
        .setUserAgent(MyJackson.get(jsonNode, "UserAgent").asInstanceOf[java.lang.String])
        .setParallelism(MyJackson.get(jsonNode, "Parallelism").asInstanceOf[java.lang.Integer])
        .setRandomDelay(MyJackson.get(jsonNode, "RandomDelay").asInstanceOf[java.lang.Integer])
        .setUrl2Crawl(MyJackson.get(jsonNode, "Url2Crawl").asInstanceOf[java.lang.String])
        .setNewsEntry(MyJackson.get(jsonNode, "NewsEntry").asInstanceOf[java.lang.String])
        .addAllQueries2Extract(strList)
        .setScriptPublishedAt(MyJackson.get(jsonNode, "ScriptPublishedAt").asInstanceOf[java.lang.String])
        .setLogflag(Logflag.forNumber(MyJackson.get(jsonNode, "Logflag").asInstanceOf[java.lang.Integer]))
      val rq = crawlNewsRequestBuilder.build()

      var stub = crawlNewsStubHolder.get()
      if(stub == null) {
        stub = getStubCrawNews
        crawlNewsStubHolder.set(stub)
        val resp = stub.crawlNews(rq.asInstanceOf[CrawlNewsRequest])
        out.collect((resp.getCrawled, resp.getLogged))
      }
    }
  }

}