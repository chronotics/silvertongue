package org.chronotics.silvertongue.scala.engine

import java.util.Calendar

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang.SerializationUtils
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.pandora.java.log.LoggerFactory
import org.chronotics.pandora.scala.file.FileIO
import org.chronotics.pithos.ext.redis.scala.adaptor.RedisPoolConnection
import org.chronotics.silverbullet.scala.akka.io.LoggingIO
import org.chronotics.silverbullet.scala.akka.protocol.Message.SendTaskToExecuteNoActorRef
import org.chronotics.silverbullet.scala.akka.state.{ActorState, Working}
import org.chronotics.silverbullet.scala.akka.util.EnvConfig
import org.rosuda.REngine.REngine

import scala.collection.immutable.HashMap

abstract class AbstractLangEngine(strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, strLanguage: String, objREngine: REngine) {
  val log = LoggerFactory.getLogger(getClass());
  val configGeneral = ConfigFactory.parseFile(EnvConfig.getConfigFile("z2_conn")).resolve()
  val redisHost = configGeneral.getString("redis.host")
  val redisPort = configGeneral.getInt("redis.port")
  val redisDB = configGeneral.getInt("redis.db")
  val redisPass = configGeneral.getString("redis.pass")
  val redisCli = RedisPoolConnection.getInstance(redisHost, redisPort, redisDB, redisPass)
  val redisKeyTaskRequestTemplate = configGeneral.getString("redis.key-task-request")
  val mapperJson = new ObjectMapper with ScalaObjectMapper
  val kafkaTopic = configGeneral.getString("kafka.topic")
  val loggingIO = LoggingIO.getInstance()
  val kafkaPartition: Int = 0
  val kafkaKey = null
  val strPrefix = EnvConfig.getPrefix()
  val strRscriptDir = configGeneral.getString("rserve.script-dir").replace("[PREFIX]", strPrefix)
  val strConfigResourceDir = configGeneral.getString("configdir.resource_dir").replace("[PREFIX]", strPrefix)
  val maxObjectSize = configGeneral.getInt("configdir.max_object_size")
  val strRscriptNameTemplate = configGeneral.getString("rserve.script-name")
  val strPythonEnv = configGeneral.getString("python.env")
  val redisExpireTime = configGeneral.getInt("redis.key_timeout")
  val fileConnection: FileIO = FileIO.getInstance()
  val strExtension = strLanguage match { case "R" => ".R" case "Python" => ".py" case _ => ".txt" }

  var mapAttr: HashMap[String, String] = HashMap.empty
  var mapInputKey: HashMap[String, String] = HashMap.empty
  var mapInputField: Seq[String] = Seq.empty
  var mapPrevOutputField: Map[String, String] = Map.empty
  var mapPropField: Map[String, String] = Map.empty
  var mapOutputField: Seq[String] = Seq.empty
  var mapPropFieldType: Map[String, String] = Map.empty
  var mapPrevInput: Map[String, Seq[String]] = Map.empty
  var arrPrevResultKey: HashMap[String, HashMap[String, String]] = HashMap.empty
  var kafkaProducer: KafkaProducer[Array[Byte], String] = null
  var objTaskToExecute: SendTaskToExecuteNoActorRef = null

  mapperJson.registerModule(DefaultScalaModule)
  mapperJson.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def openKafkaProducer() {
    var propsKafka = new java.util.Properties()
    propsKafka.put("bootstrap.servers", configGeneral.getString("kafka.broker"))
    propsKafka.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    propsKafka.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    kafkaProducer = new KafkaProducer[Array[Byte], String](propsKafka)
  }

  def closeKafkaProducer() {
    try {
      kafkaProducer.flush()
      kafkaProducer.close()
    } catch {
      case ex: Throwable => log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
    }
  }

  def readDataObjectFromRedis() = {
    val strRedisKeyTaskRequest = redisKeyTaskRequestTemplate + strRequestId + "_" + strTaskId

    val arrTaskResultByte: Array[Byte] = redisCli.getKeyBytes(strRedisKeyTaskRequest)

    redisCli.delKey(strRedisKeyTaskRequest)

    log.info("readDataObjectFromRedis: " + arrTaskResultByte.toString)

    if (arrTaskResultByte != null) {
      objTaskToExecute = SerializationUtils.deserialize(arrTaskResultByte).asInstanceOf[SendTaskToExecuteNoActorRef]
    }
  }

  def openLanguageEngine(strLanguage: String, objREngine: REngine) = {
  }

  def executeLanguageEngine(bShouldExit: Boolean = true) = {
  }

  def writeResultToRedis(strStatus: String, isEndLoop: Boolean = false, isPrevConditionSatisfied: Boolean = true) = {
    val strRedisKeyTaskRequestCompleted = redisKeyTaskRequestTemplate + strRequestId + "_" + strTaskId + "_completed"
    val strValue = strStatus + "|" + (isEndLoop match {
      case true => "1"
      case false => "0"
    }) + "|" + (isPrevConditionSatisfied match {
      case true => "1"
      case false => "0"
    })

    redisCli.setKey(strRedisKeyTaskRequestCompleted, strValue, 3600)
  }

  def updateMessageToKafka(strMessage: String, objStatus: ActorState, messageType: String = "L") = {
    var strLogMessage = messageType match {
      case "C" => loggingIO.generateScriptMesageTaskExecuteNoActor(objTaskToExecute, objStatus, strMessage)
      case _ => loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, objStatus, strMessage)
    }
    //String topic, Integer partition, Long timestamp, K key, V value
    val kafkaRecord = new ProducerRecord[Array[Byte], String](kafkaTopic, kafkaPartition, Calendar.getInstance.getTimeInMillis, kafkaKey, strLogMessage)

    kafkaProducer.send(kafkaRecord, new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, ex: Exception): Unit = {
      }
    })
  }

  def writeOutput(strOutKey: String, fieldName: String, result: Any) = {
    log.info("writeOut: " + strOutKey + " - " + fieldName)
    //log.info("result: " + result.toString)

    try {
      redisCli.hSet(strOutKey, fieldName, result)
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString)
        if (strOutKey.contains("_attr")) {
          redisCli.hSet(strOutKey, fieldName, result)
        }
      }
    }
  }

  def readOutput(strOutKey: String, fieldName: String): Any = {
    try {
      redisCli.hGet(strOutKey, fieldName) match {
        case Some(x) => x
        case None => ""
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString)
        ""
      }
    }
  }
}
