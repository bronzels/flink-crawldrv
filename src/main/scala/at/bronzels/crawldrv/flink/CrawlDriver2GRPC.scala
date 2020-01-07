package at.bronzels.crawldrv.flink

import java.util

import at.bronzels.libcdcdwstr.flink.FrameworkScalaInf
import at.bronzels.libcdcdwstr.flink.bean.ScalaStreamContext
import at.bronzels.libcdcdwstr.flink.source.KafkaJsonNodeScalaStream

import org.apache.flink.streaming.api.scala._

import at.bronzels.crawldrv.CliInput

object CrawlDriver2GRPC {
  val appName = "crawldrv"

  def main(args: Array[String]): Unit = {
    //val newargs = args
    val newargs = Array[String](
      "-f", "4",

      "-cgttp", "crawldrv_grpc_test",

      "-cgtzkq", "beta-hbase02:2181,beta-hbase03:2181,beta-hbase04:2181/kafka",
      "-cgtbstr", "beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092",

      "-cgthost", "localhost",
      "-cgtport", "30092"
    )

    val myCli = new CliInput
    val options = myCli.buildOptions()

    if (myCli.parseIsHelp(options, newargs)) return

    val prefixedStrNameCliInput = appName + at.bronzels.libcdcdw.Constants.commonSep + myCli.getStrNameCliInput
    FrameworkScalaInf.launchedMS = System.currentTimeMillis()
    FrameworkScalaInf.appName = prefixedStrNameCliInput

    var flinkInputParallelism4Local: java.lang.Integer = null
    if (myCli.isFlinkInputLocalMode)
      flinkInputParallelism4Local = myCli.getFlinkInputParallelism4Local
    FrameworkScalaInf.setupEnv(flinkInputParallelism4Local, myCli.isOperatorChainDisabled)

    val streamContext = new ScalaStreamContext(FrameworkScalaInf.env, FrameworkScalaInf.tableEnv)

    val cdcPrefix = myCli.getCdcPrefix
    val outputPrefix = myCli.getOutputPrefix

    val strZkQuorum = myCli.kafkaZKQuorum
    val strBrokers = myCli.kafkaBootstrap

    val kafkaJsonNodeStreamObj = new KafkaJsonNodeScalaStream(outputPrefix, prefixedStrNameCliInput, streamContext, strZkQuorum, strBrokers, util.Collections.singletonList(myCli.topicName))

    val kafkaJsonNodeStream = kafkaJsonNodeStreamObj.getStream
    kafkaJsonNodeStream.print()

    val grpcReqStream = kafkaJsonNodeStream
      .map(record => record.getData)
    grpcReqStream.print()

    val grpcRespStream = grpcReqStream
      .flatMap(new CrawlerDriverGRPCRichMap(myCli.serviceHost, myCli.servicePort))
    grpcRespStream.print()

    FrameworkScalaInf.env.execute(prefixedStrNameCliInput)

  }

}
