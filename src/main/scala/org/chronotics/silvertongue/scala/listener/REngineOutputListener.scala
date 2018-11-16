package org.chronotics.silvertongue.scala.listener

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.chronotics.pandora.java.log.LoggerFactory
import org.chronotics.pithos.ext.redis.scala.adaptor.RedisPoolConnection
import org.chronotics.silverbullet.scala.akka.io.LoggingIO
import org.chronotics.silverbullet.scala.akka.state.Working
import org.chronotics.silverbullet.scala.akka.util.EnvConfig
import org.rosuda.REngine.{REngine, REngineStdOutput}

class REngineOutputListener(strCallback: String, strUserId: String, strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, isUseBufferred: Boolean) extends REngineStdOutput {
  val log = LoggerFactory.getLogger(getClass());
  val config = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val topic = config.getString("kafka.topic")
  val redisHost = config.getString("redis.host")
  val redisPort = config.getInt("redis.port")
  val redisDB = config.getInt("redis.db")
  val redisPass = config.getString("redis.pass")
  val redisCli = RedisPoolConnection.getInstance(redisHost, redisPort, redisDB, redisPass)
  var arrMessage: List[String] = List.empty
  var arrKafkaMessage: List[String] = List.empty
  var intCurIndex: Int = 0
  val loggingIO = LoggingIO.getInstance()
  var propsKafka = new java.util.Properties()
  var strCurRequest = ""
  val partition = 0
  val key = null

  propsKafka.put("bootstrap.servers", config.getString("kafka.broker"))
  propsKafka.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  propsKafka.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  var kafkaProducer = new KafkaProducer[Array[Byte], String](propsKafka)

  var objStrBuilder: StringBuilder = StringBuilder.newBuilder

  override def RWriteConsole(re: REngine, text: String, intType: Int) = {
    var strConsole: String = ""

    if (text != "") {
      strConsole = text
      strConsole = loggingIO.generateLogMessage(strCallback, strUserId, "C", strRequestId.toInt, Working, strWorkflowId, strLibId, strTaskId, strConsole)

      sendKafkaMessage(strConsole)
    }
  }

  def sendKafkaMessage(message: String) {
    try {
      log.info(message)

      var lCurTimestamp = EnvConfig.getCurrentTimestamp

      if (isUseBufferred) {
        arrKafkaMessage.foreach { curMsg =>
          lCurTimestamp = EnvConfig.getCurrentTimestamp

          if (curMsg.contains("Listening")) {
            log.debug(curMsg)
          }

          var record = new ProducerRecord[Array[Byte], String](topic, partition, lCurTimestamp, key, strCurRequest + curMsg)

          kafkaProducer.send(record, new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, ex: Exception): Unit = {
            }
          })
        }

        arrKafkaMessage = List.empty
      } else {
        var record = new ProducerRecord[Array[Byte], String](topic, partition, lCurTimestamp, key, strCurRequest + message)

        kafkaProducer.send(record, new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, ex: Exception): Unit = {
          }
        })
      }

      kafkaProducer.flush
    } catch {
      case ex: Throwable => {
        log.error("ERR", ex)
      }
    }
  }

  def readInput(curKey: String): Any = {
    redisCli.getKey(curKey)
  }
}