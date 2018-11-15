package org.chronotics.silvertongue.scala.engine

import java.nio.file.Paths
import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.asynchttpclient.Dsl
import org.asynchttpclient.ws.{WebSocket, WebSocketListener, WebSocketUpgradeHandler}
import org.chronotics.pandora.java.exception.ExceptionUtil
import org.chronotics.pithos.ext.es.adaptor.ElasticService
import org.chronotics.pithos.ext.es.model.ESPrepListActionRequestModel
import org.chronotics.pithos.ext.es.util.ESPrepActionConverterUtil
import org.chronotics.silverbullet.scala.akka.protocol.Message.SendTaskToExecuteNoActorRef
import org.chronotics.silverbullet.scala.akka.state.{Failed, Finished, Waiting, Working}
import org.chronotics.silvertongue.java.util.RedisDataConversion
import org.rosuda.REngine.REngine

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.control.Breaks._

class ElasticSearchEngine(strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, var objRengine: REngine)
  extends AbstractLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, "ES", null) {
  val redisKeyTemplate = configGeneral.getString("redis.key-task-result")
  var objESConnection = ElasticService.getInstance(configGeneral.getString("es.cluster_name"), configGeneral.getString("es.host"), configGeneral.getInt("es.port"))
  val strIndexPattern = configGeneral.getString("es.index_pattern")
  val strTypeName = configGeneral.getString("es.type_name")
  var objRedisKeyLib: RedisDataConversion = new RedisDataConversion(redisHost, redisPort, redisPass, redisDB, redisKeyTemplate)
  var objJSONMapper: ObjectMapper = new ObjectMapper()
  val wsPrepURL = configGeneral.getString("webapi.prep_ws")
  val ws = Dsl.asyncHttpClient().prepareGet(configGeneral.getString("webapi.prep_ws"))
  var webSocket: WebSocket = null
  var bIsOpening = false
  var bIsForeverClose = false

  def openConnection(bShouldExit: Boolean): WebSocket = {
    log.debug("Try to open connection again!");

    bIsOpening = false

    ws.execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketListener() {
      override def onTextFrame(message: String, finalFragment: Boolean, rsv: Int) = {
        bIsOpening = false
        if (message.contains("|Failed")) {
          log.error("ERR", message)
        } else {
          log.info("ElasticSearchEngine - WebsocketMsg:" + message)

          var bIsExist = processWSMessage(message, bShouldExit)

          if (bIsExist) {
            bIsForeverClose = true
            webSocket.sendCloseFrame()
          }
        }
      }

      override def onOpen(websocket: WebSocket) {
        bIsOpening = false
        websocket.sendTextFrame("ElasticSearchEngine Client says hello");
      }

      override def onClose(curWS: WebSocket, arg1: Int, arg2: String) {
        log.error("ElasticSearchEngine - WebsocketMsg: Closed")

        if (!bIsOpening && !bIsForeverClose) {
          bIsOpening = true
          webSocket = openConnection(bShouldExit)
        }
      }

      override def onError(t: Throwable) {
        log.error("ElasticSearchEngine - WebsocketMsg: Error - " + t.getStackTraceString)

        if (!bIsOpening && !bIsForeverClose) {
          bIsOpening = true
          webSocket = openConnection(bShouldExit)
        }
      }
    }).build()).get();
  }

  def processWSMessage(message: String, bShouldExit: Boolean): Boolean = {
    var bIsExist = false

    if (message != null && !message.isEmpty()) {
      var arrMessage = message.split("\\|")

      if (arrMessage != null && arrMessage.length > 1) {
        if (arrMessage(0).equals(strRequestId)) {
          bIsExist = true

          var strCSV = arrMessage(1).trim

          if (strCSV != null && !strCSV.isEmpty) {
            val strRedisKey = configGeneral.getString("redis.key-task-result") + objTaskToExecute.objRequest.iRequestId + "_" + objTaskToExecute.objRequest.iParentTaskId + "_" + objTaskToExecute.iTaskId
            val strRedisKeyPretty = strRedisKey + "_pretty"
            log.info("strNewFileName: " + strCSV)

            for (strField: String <- mapOutputField) {
              writeOutput(strRedisKey, strField, strCSV)
              writeOutput(strRedisKeyPretty, strField, strCSV)
            }

            writeResultToRedis(Finished.toString, true, true)
          } else {
            updateMessageToKafka("Can't create CSV File", Failed)
            writeResultToRedis(Failed.toString, true, true)
          }

          updateMessageToKafka("=======End " + objTaskToExecute.libMeta.strName + "=======", Working, "C")

          if (bShouldExit) {
            objESConnection.closeInstance();
            System.exit(0)
          }
        }
      }
    }

    bIsExist
  }

  override def openLanguageEngine(strLanguage: String, objCurREngine: REngine) = {
  }

  override def executeLanguageEngine(bShouldExit: Boolean = true) = {
    bIsForeverClose = false

    updateMessageToKafka("=======Start " + objTaskToExecute.libMeta.strName + "=======", Working, "C")
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Received task: " + objTaskToExecute))

    try {
      //Get Input, Output, Properties
      mapInputField = Seq.empty
      mapPrevOutputField = Map.empty
      mapPropField = Map.empty
      mapOutputField = Seq.empty
      mapPropFieldType = Map.empty

      //Check Prop CMD: is_simulate or is_execute
      getListInputOutputParam(objTaskToExecute)

      var arrPrevResult: Map[String, Any] = Map.empty

      //Load Previous Result
      arrPrevResult = readInput(mapPrevInput, mapPrevOutputField)

      var strCmd: String = ""
      var strJSONPrepStep: String = ""
      var bIsSimulate: Boolean = false
      var bHasHeader: Boolean = true
      var strSeparator: String = ","
      var strDateColumn: String = ""

      log.info("mapPropFieldType: " + mapPropFieldType)
      log.info("mapPropField: " + mapPropField)

      if (mapPropField != null) {
        if (mapPropField.contains("cmd")) {
          strCmd = mapPropField("cmd")

          if (strCmd.toLowerCase().equals("is_simulate")) {
            bIsSimulate = true
          } else if (strCmd.toLowerCase().equals("is_execute")) {
            if (mapPropField.contains("prep_steps")) {
              strJSONPrepStep = mapPropField("prep_steps")
            }
          }
        }

        if (mapPropField.contains("has_header")) {
          bHasHeader = mapPropField("has_header").toLowerCase().trim() match {
            case "true" => true
            case "false" => false
          }
        }

        if (mapPropField.contains("data_separator")) {
          strSeparator = mapPropField("data_separator")

          if (strSeparator == null || strSeparator.isEmpty()) {
            strSeparator = ","
          }
        }

        if (mapPropField.contains("date_column")) {
          strDateColumn = mapPropField("date_column")
        }
      }

      log.info("strCmd: " + strCmd)
      log.info("bIsSimulate: " + bIsSimulate)
      log.info("strJSONPrepStep: " + strJSONPrepStep)

      val strRedisKey = configGeneral.getString("redis.key-task-result") + objTaskToExecute.objRequest.iRequestId + "_" + objTaskToExecute.objRequest.iParentTaskId + "_" + objTaskToExecute.iTaskId
      val strRedisKeyPretty = strRedisKey + "_pretty"
      val strRedisKeyLang = strRedisKey + "_lang"
      writeOutput(strRedisKeyLang, "lang", "ES");

      //If IS_SIMULATE
      if (bIsSimulate) {
        //1. Read data from file of previous node
        var objReturn = readDataFileFromPrevNodeFile(arrPrevResult, bHasHeader, strSeparator)

        var lstData: util.List[java.util.HashMap[String, Object]] = objReturn._1
        var strOldCSVFile = objReturn._2

        //2. Delete index if existed and Create index/type
        var strNewIndex = createElasticIndex(objTaskToExecute, lstData, strDateColumn)

        //3. Send to Kafka Message with ES Index Name/Type/Waiting Message
        //4. Send Waiting Status back to AlibTaskActor
        if (strNewIndex != null && !strNewIndex.isEmpty()) {
          var strResponse = "{\"index\": \"" + strNewIndex + "\",\"type\":\"" + strTypeName + "\"}";
          updateMessageToKafka(strResponse, Waiting, "C")

          webSocket = openConnection(bShouldExit)

          while (!bIsForeverClose) {
            Thread.sleep(100)
          }
        }
      } else if (!bIsSimulate && strJSONPrepStep != null && !strJSONPrepStep.isEmpty()) {
        //IF IS_RUN
        //1. Read data from file of previous node
        var objReturn = readDataFileFromPrevNodeFile(arrPrevResult, bHasHeader, strSeparator)

        var lstData: util.List[java.util.HashMap[String, Object]] = objReturn._1
        var strOldCSVFile = objReturn._2

        //2. Delete index if existed and Create index/type
        var strNewIndex = createElasticIndex(objTaskToExecute, lstData, strDateColumn)
        //3. Read PROP prep_step

        Thread.sleep(500)

        if (strJSONPrepStep == null || strJSONPrepStep.isEmpty()) {
          for (strField: String <- mapOutputField) {
            writeOutput(strRedisKey, strField, strOldCSVFile)
            writeOutput(strRedisKeyPretty, strField, strOldCSVFile)
          }

          writeResultToRedis(Finished.toString, true, true)
        } else {
          var objJSONMetaStep: ESPrepListActionRequestModel = objJSONMapper.readValue[ESPrepListActionRequestModel](strJSONPrepStep, new TypeReference[ESPrepListActionRequestModel]() {})

          log.info("objJSONMetaStep: " + objJSONMetaStep)

          if (objJSONMetaStep == null || objJSONMetaStep.getActions == null || objJSONMetaStep.getActions.size <= 0) {
            for (strField: String <- mapOutputField) {
              writeOutput(strRedisKey, strField, strOldCSVFile)
              writeOutput(strRedisKeyPretty, strField, strOldCSVFile)
            }

            writeResultToRedis(Finished.toString, true, true)
          } else {
            var lstPrepMetaStep = ESPrepActionConverterUtil.convert(objJSONMetaStep)

            log.info("lstPrepMetaStep: " + lstPrepMetaStep)

            if (lstPrepMetaStep != null && lstPrepMetaStep.size() > 0) {
              var bIsAllProcessed = true

              breakable {
                for (iStep <- 0 until lstPrepMetaStep.size()) {
                  log.info("lstPrepMetaStep - iStep: " + lstPrepMetaStep.get(iStep))

                  //3.1. Process each step
                  val arrCurStep = util.Arrays.asList(lstPrepMetaStep.get(iStep))
                  val bIsProcessed = objESConnection.prepESData(arrCurStep)

                  //3.2. If success, send Success WS message and continue to next step
                  if (bIsProcessed) {
                    updateMessageToKafka("[" + (lstPrepMetaStep.get(iStep).getId) + "][Succeed]", Working, "C")
                  } else {
                    //3.3. If fail, send Failed WS Message and stop
                    bIsAllProcessed = false
                    updateMessageToKafka("[" + (lstPrepMetaStep.get(iStep).getId) + "][Failed]", Failed, "C")

                    break
                  }
                }
              }

              log.info("bIsAllProcessed: " + bIsAllProcessed)

              //4. Save last result to CSV File and put to Redis
              if (bIsAllProcessed) {
                Thread.sleep(500)

                val strFileName = objTaskToExecute.objRequest.iRequestId.toString + "_" + objTaskToExecute.objRequest.iWorkFlowId.toString + "_" + objTaskToExecute.iTaskId.toString + ".csv"
                val strCSVFile = Paths.get(strConfigResourceDir, "csv", strFileName).toString

                val strNewFileName: String = objESConnection.exportESDataToCSV(strNewIndex, strTypeName, strCSVFile, 10000)

                log.info("strNewFileName: " + strNewFileName)

                if (strNewFileName != null && !strNewFileName.isEmpty()) {
                  for (strField: String <- mapOutputField) {
                    writeOutput(strRedisKey, strField, strNewFileName)
                    writeOutput(strRedisKeyPretty, strField, strNewFileName)
                  }

                  writeResultToRedis(Finished.toString, true, true)
                } else {
                  updateMessageToKafka("Can't create CSV File", Failed)
                  writeResultToRedis(Failed.toString, true, true)
                }
              } else {
                writeResultToRedis(Failed.toString, true, true)
              }
            } else {
              for (strField: String <- mapOutputField) {
                writeOutput(strRedisKey, strField, strOldCSVFile)
                writeOutput(strRedisKeyPretty, strField, strOldCSVFile)
              }

              writeResultToRedis(Finished.toString, true, true)
            }
          }
        }
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getMessage + " - " + ex.getCause + " - " + ex.getStackTraceString))
        writeResultToRedis(Failed.toString, true, true)
      }
    }

    updateMessageToKafka("=======End " + objTaskToExecute.libMeta.strName + "=======", Working, "C")

    if (bShouldExit) {
      objESConnection.closeInstance();
      System.exit(0)
    }
  }

  @deprecated
  def executeLanguageEngine_Old(bShouldExit: Boolean = true) = {
    updateMessageToKafka("=======Start " + objTaskToExecute.libMeta.strName + "=======", Working, "C")
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Received task: " + objTaskToExecute))

    try {
      //Get Input, Output, Properties
      mapInputField = Seq.empty
      mapPrevOutputField = Map.empty
      mapPropField = Map.empty
      mapOutputField = Seq.empty
      mapPropFieldType = Map.empty

      //Check Prop CMD: is_simulate or is_execute
      getListInputOutputParam(objTaskToExecute)

      var arrPrevResult: Map[String, Any] = Map.empty

      //Load Previous Result
      arrPrevResult = readInput(mapPrevInput, mapPrevOutputField)

      var strCmd: String = ""
      var strJSONPrepStep: String = ""
      var bIsSimulate: Boolean = false
      var bHasHeader: Boolean = true
      var strSeparator: String = ","
      var strDateColumn: String = ""

      log.info("mapPropFieldType: " + mapPropFieldType)
      log.info("mapPropField: " + mapPropField)

      if (mapPropField != null) {
        if (mapPropField.contains("cmd")) {
          strCmd = mapPropField("cmd")

          if (strCmd.toLowerCase().equals("is_simulate")) {
            bIsSimulate = true
          } else if (strCmd.toLowerCase().equals("is_execute")) {
            if (mapPropField.contains("prep_steps")) {
              strJSONPrepStep = mapPropField("prep_steps")
            }
          }
        }

        if (mapPropField.contains("has_header")) {
          bHasHeader = mapPropField("has_header").toLowerCase().trim() match {
            case "true" => true
            case "false" => false
          }
        }

        if (mapPropField.contains("data_separator")) {
          strSeparator = mapPropField("data_separator")

          if (strSeparator == null || strSeparator.isEmpty()) {
            strSeparator = ","
          }
        }

        if (mapPropField.contains("date_column")) {
          strDateColumn = mapPropField("date_column")
        }
      }

      log.info("strCmd: " + strCmd)
      log.info("bIsSimulate: " + bIsSimulate)
      log.info("strJSONPrepStep: " + strJSONPrepStep)

      val strRedisKey = configGeneral.getString("redis.key-task-result") + objTaskToExecute.objRequest.iRequestId + "_" + objTaskToExecute.objRequest.iParentTaskId + "_" + objTaskToExecute.iTaskId
      val strRedisKeyPretty = strRedisKey + "_pretty"
      val strRedisKeyLang = strRedisKey + "_lang"
      writeOutput(strRedisKeyLang, "lang", "ES");

      //If IS_SIMULATE
      if (bIsSimulate) {
        //1. Read data from file of previous node
        var objReturn = readDataFileFromPrevNodeFile(arrPrevResult, bHasHeader, strSeparator)

        var lstData: util.List[java.util.HashMap[String, Object]] = objReturn._1
        var strOldCSVFile = objReturn._2

        //2. Delete index if existed and Create index/type
        var strNewIndex = createElasticIndex(objTaskToExecute, lstData, strDateColumn)

        //3. Send to Kafka Message with ES Index Name/Type/Waiting Message
        //4. Send Waiting Status back to AlibTaskActor
        if (strNewIndex != null && !strNewIndex.isEmpty()) {
          var strResponse = "{\"index\": \"" + strNewIndex + "\",\"type\":\"" + strTypeName + "\"}";
          updateMessageToKafka(strResponse, Waiting, "C")

          writeResultToRedis(Waiting.toString, true, true)
        } else {
          updateMessageToKafka("Can't create ElasticSearch Index", Failed, "C")

          writeResultToRedis(Failed.toString, true, true)
        }
      } else if (!bIsSimulate && strJSONPrepStep != null && !strJSONPrepStep.isEmpty()) {
        //IF IS_RUN
        //1. Read data from file of previous node
        var objReturn = readDataFileFromPrevNodeFile(arrPrevResult, bHasHeader, strSeparator)

        var lstData: util.List[java.util.HashMap[String, Object]] = objReturn._1
        var strOldCSVFile = objReturn._2

        //2. Delete index if existed and Create index/type
        var strNewIndex = createElasticIndex(objTaskToExecute, lstData, strDateColumn)
        //3. Read PROP prep_step

        Thread.sleep(500)

        if (strJSONPrepStep == null || strJSONPrepStep.isEmpty()) {
          for (strField: String <- mapOutputField) {
            writeOutput(strRedisKey, strField, strOldCSVFile)
            writeOutput(strRedisKeyPretty, strField, strOldCSVFile)
          }

          writeResultToRedis(Finished.toString, true, true)
        } else {
          var objJSONMetaStep: ESPrepListActionRequestModel = objJSONMapper.readValue[ESPrepListActionRequestModel](strJSONPrepStep, new TypeReference[ESPrepListActionRequestModel]() {})

          log.info("objJSONMetaStep: " + objJSONMetaStep)

          if (objJSONMetaStep == null || objJSONMetaStep.getActions == null || objJSONMetaStep.getActions.size <= 0) {
            for (strField: String <- mapOutputField) {
              writeOutput(strRedisKey, strField, strOldCSVFile)
              writeOutput(strRedisKeyPretty, strField, strOldCSVFile)
            }

            writeResultToRedis(Finished.toString, true, true)
          } else {
            var lstPrepMetaStep = ESPrepActionConverterUtil.convert(objJSONMetaStep)

            log.info("lstPrepMetaStep: " + lstPrepMetaStep)

            if (lstPrepMetaStep != null && lstPrepMetaStep.size() > 0) {
              var bIsAllProcessed = true

              breakable {
                for (iStep <- 0 until lstPrepMetaStep.size()) {
                  log.info("lstPrepMetaStep - iStep: " + lstPrepMetaStep.get(iStep))

                  //3.1. Process each step
                  val arrCurStep = util.Arrays.asList(lstPrepMetaStep.get(iStep))
                  val bIsProcessed = objESConnection.prepESData(arrCurStep)

                  //3.2. If success, send Success WS message and continue to next step
                  if (bIsProcessed) {
                    updateMessageToKafka("[" + (lstPrepMetaStep.get(iStep).getId) + "][Succeed]", Working, "C")
                  } else {
                    //3.3. If fail, send Failed WS Message and stop
                    bIsAllProcessed = false
                    updateMessageToKafka("[" + (lstPrepMetaStep.get(iStep).getId) + "][Failed]", Failed, "C")

                    break
                  }
                }
              }

              log.info("bIsAllProcessed: " + bIsAllProcessed)

              //4. Save last result to CSV File and put to Redis
              if (bIsAllProcessed) {
                Thread.sleep(500)

                val strFileName = objTaskToExecute.objRequest.iRequestId.toString + "_" + objTaskToExecute.objRequest.iWorkFlowId.toString + "_" + objTaskToExecute.iTaskId.toString + ".csv"
                val strCSVFile = Paths.get(strConfigResourceDir, "csv", strFileName).toString

                val strNewFileName: String = objESConnection.exportESDataToCSV(strNewIndex, strTypeName, strCSVFile, 10000)

                log.info("strNewFileName: " + strNewFileName)

                if (strNewFileName != null && !strNewFileName.isEmpty()) {
                  for (strField: String <- mapOutputField) {
                    writeOutput(strRedisKey, strField, strNewFileName)
                    writeOutput(strRedisKeyPretty, strField, strNewFileName)
                  }

                  writeResultToRedis(Finished.toString, true, true)
                } else {
                  updateMessageToKafka("Can't create CSV File", Failed)
                  writeResultToRedis(Failed.toString, true, true)
                }
              } else {
                writeResultToRedis(Failed.toString, true, true)
              }
            } else {
              for (strField: String <- mapOutputField) {
                writeOutput(strRedisKey, strField, strOldCSVFile)
                writeOutput(strRedisKeyPretty, strField, strOldCSVFile)
              }

              writeResultToRedis(Finished.toString, true, true)
            }
          }
        }
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getMessage + " - " + ex.getCause + " - " + ex.getStackTraceString, "C"))
        writeResultToRedis(Failed.toString, true, true)
      }
    }

    updateMessageToKafka("=======End " + objTaskToExecute.libMeta.strName + "=======", Working, "C")

    if (bShouldExit) {
      objESConnection.closeInstance();
      System.exit(0)
    }
  }

  def readDataFileFromPrevNodeFile(arrPrevResult: Map[String, Any], bHasHeader: Boolean = true, strSeparator: String = ","): (util.List[java.util.HashMap[String, Object]], String) = {
    var lstMapConvertedContent: util.List[java.util.HashMap[String, Object]] = new util.ArrayList[java.util.HashMap[String, Object]]()
    var strFilePath: String = ""

    try {
      if (arrPrevResult != null && arrPrevResult.size > 0) {
        strFilePath = arrPrevResult.values.toList(0).toString

        if (strFilePath != null && !strFilePath.isEmpty) {
          if (strFilePath.toLowerCase.endsWith(".csv")) {
            var objReadFile = Source.fromFile(strFilePath)
            var intCount = 0
            var arrHeader: Array[String] = Array.empty

            for (curLine <- objReadFile.getLines()) {
              val arrSplit = curLine.split(strSeparator).map(_.trim)
              var curMap: java.util.HashMap[String, Object] = new java.util.HashMap[String, Object]()

              if (intCount == 0) {
                if (bHasHeader) {
                  arrHeader = arrSplit.map(_.replace(".", "_"))
                } else {
                  for (i <- 0 until arrSplit.length) {
                    arrHeader = arrHeader :+ ("column_" + (i + 1).toString)
                  }
                }
              }

              if (!bHasHeader || (intCount != 0 && bHasHeader)) {
                for (i <- 0 until arrSplit.length) {
                  curMap.put(arrHeader.apply(i).toLowerCase(), arrSplit.apply(i))
                }

                lstMapConvertedContent.add(curMap)
              }

              intCount += 1
            }

            log.info("strFilePath: " + strFilePath)
          } else {
            log.info("ES: Input is not CSV File, parse JSON instead")

            try {
              lstMapConvertedContent = objJSONMapper.readValue(strFilePath, new TypeReference[List[java.util.HashMap[String, Object]]]() {})
            } catch {
              case ex: Throwable => log.error("ES: Input is not properly JSON: " + strFilePath)
            }
          }
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ExceptionUtil.getStrackTrace(ex))
      }
    }

    log.info("lstMapConvertedContent-size: " + lstMapConvertedContent.size)

    (lstMapConvertedContent, strFilePath)
  }

  def createElasticIndex(task: SendTaskToExecuteNoActorRef, lstMapData: util.List[java.util.HashMap[String, Object]], strDateField: String): String = {
    var strTaskIndex = strIndexPattern + task.objRequest.iRequestId.toString + "_" + task.objRequest.iWorkFlowId.toString + "_" + task.iTaskId.toString

    try {
      var bIsCreated = objESConnection.createIndex(strTaskIndex, strTypeName, lstMapData, strDateField, null, true)

      if (!bIsCreated) {
        strTaskIndex = ""
      } else {
        objESConnection.insertBulkData(strTaskIndex, strTypeName, lstMapData, "", null)
      }
    } catch {
      case ex: Throwable => {
        strTaskIndex = ""
        log.error(ExceptionUtil.getStrackTrace(ex), "ERR")
      }
    }

    strTaskIndex
  }

  def getListInputOutputParam(task: SendTaskToExecuteNoActorRef) = {
    if (task.libMeta != null) {
      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "libMeta:" + task.libMeta))

      if (task.arrPrevResultKey != null && task.arrPrevResultKey.size > 0) {
        for (curKey <- task.arrPrevResultKey) {
          for (curParam <- curKey._2) {
            log.info("CurParam: " + curParam._1 + " - " + curParam._2)
            mapInputField = mapInputField :+ curParam._1
            mapPrevOutputField = mapPrevOutputField + (curParam._1 -> curParam._2)
            mapPropFieldType = mapPropFieldType + (curParam._1 -> "Entire Result")
          }
        }
      }

      for (curInput <- task.libMeta.arrParam.arrLibParam) {
        log.info("CurParam: " + curInput.paramType + " - " + curInput.strName)

        if (curInput.paramType == "input") {
          mapInputField = mapInputField :+ curInput.strName

          var curInputId = curInput.iId

          breakable {
            for (curConnection <- task.objTaskInfo.arrConnection) {
              if (curConnection.toInputId == curInputId) {
                mapPrevOutputField = mapPrevOutputField + (curInput.strName -> curConnection.fromOutputName)
                break
              }
            }
          }
        }

        if (curInput.paramType == "output") {
          mapOutputField = mapOutputField :+ curInput.strName
        }

        if (curInput.paramType == "prop" && (task.arrParam == null || task.arrParam.length <= 0)) {
          curInput.dDefaultValue match {
            case Some(x) => {
              var strValue: String = x.toString()

              /*if (curInput.strDataType == "String") {
                  strValue = "\"" + strValue + "\""
                }*/

              (mapPropField = mapPropField + (curInput.strName -> strValue))
            }
            case None =>
          }

        }

        mapPropFieldType = mapPropFieldType + (curInput.strName -> curInput.strDataType)
      }
    }

    arrPrevResultKey = task.arrPrevResultKey
    mapPrevInput = Map.empty

    for (prevKey <- arrPrevResultKey) {
      mapPrevInput = mapPrevInput + (prevKey._1 -> prevKey._2.keySet.toSeq)
    }

    mapInputField = mapInputField.distinct
  }

  def readInput(arrInputKey: Map[String, Seq[String]], arrPrevOutput: Map[String, String]): Map[String, Any] = {
    var seqResult: scala.collection.immutable.Map[String, Any] = scala.collection.immutable.Map.empty

    mapInputKey = scala.collection.immutable.HashMap.empty

    log.info("arrInputKey: " + arrInputKey)

    if (arrInputKey == null)
      null

    try {
      for (curKey: (String, Seq[String]) <- arrInputKey) {
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, curKey._1))

        for (curProp: String <- curKey._2) {
          var curPrevOutput = arrPrevOutput.get(curProp) match {
            case Some(txt) => txt
            case None => ""
          }

          if (curPrevOutput == "") {
            curPrevOutput = curProp
          }

          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Out: " + curProp + " - In: " + curPrevOutput))

          var curResult = objRedisKeyLib.getStrContentOfKey(0, 0, curPrevOutput, curKey._1, curKey._1 + "_json")

          if (curResult == null || curResult.isEmpty()) {
            curResult = redisCli.hGet(curKey._1, curPrevOutput) match {
              case Some(x) => x.toString
              case None => ""
            }
          }

          if (curResult != null && curResult.toString != "") {
            seqResult = seqResult + (curProp -> curResult)
          }
        }
      }
    } catch {
      case ex: Throwable => log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))
    }

    seqResult
  }

  override def writeOutput(strOutKey: String, fieldName: String, result: Any) = {
    redisCli.hSet(strOutKey, fieldName, result)
  }
}
