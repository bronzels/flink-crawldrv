package at.bronzels.crawldrv

import at.bronzels.libcdcdwstr.SimpleCommonCliInput
import org.apache.commons.cli.{Option, Options}

class CliInput extends SimpleCommonCliInput {
  var topicName:String = "crawldrv_grpc_test"

  var kafkaZKQuorum:String = _
  var kafkaBootstrap:String = _

  var serviceHost:String = _
  var servicePort:Int = _

  override def buildOptions(): Options = {
    val options = super.buildOptions()

    //topic name
    val optionTopicName = new Option("cgttp", "topicName", true, "sensor data topic name")
    optionTopicName.setRequired(true);options.addOption(optionTopicName)

    //kafka zookeeper quorum(with kafka path)
    val optionKafkaZKQuorum = new Option("cgtzkq", "kafkaZKQuorum", true, "kafka zookeeper quorum with zk host:port concatted with , and end with kafka path")
    optionKafkaZKQuorum.setRequired(true);options.addOption(optionKafkaZKQuorum)

    //kafka bootstrap hosts/ports
    val optionKafkaBootstrapServers = new Option("cgtbstr", "kafkaBootstrap", true, "kafka bootstrap servers with kafka daemon host:port concatted with ,")
    optionKafkaBootstrapServers.setRequired(true);options.addOption(optionKafkaBootstrapServers)

    //kafka zookeeper quorum(with kafka path)
    val optionServiceHost = new Option("cgthost", "serviceHost", true, "grpc service host")
    optionServiceHost.setRequired(true);options.addOption(optionServiceHost)

    //kafka bootstrap hosts/ports
    val optionServicePort = new Option("cgtport", "servicePort", true, "grpc service port")
    optionServicePort.setRequired(true);options.addOption(optionServicePort)

    options
  }

  override def parseIsHelp(options: Options, args: Array[String]): Boolean = {
    if(super.parseIsHelp (options, args))
      return true

    val comm = getCommandLine(options, args)

    if (comm.hasOption("cgttp")) {
      topicName = comm.getOptionValue("cgttp")
    }

    if (comm.hasOption("cgtzkq")) {
      kafkaZKQuorum = comm.getOptionValue("cgtzkq")
    }
    if (comm.hasOption("cgtbstr")) {
      kafkaBootstrap = comm.getOptionValue("cgtbstr")
    }

    if (comm.hasOption("cgthost")) {
      serviceHost = comm.getOptionValue("cgthost")
    }
    if (comm.hasOption("cgtport")) {
      servicePort = java.lang.Integer.parseInt(comm.getOptionValue("cgtport"))
    }

    return false

  }

}
