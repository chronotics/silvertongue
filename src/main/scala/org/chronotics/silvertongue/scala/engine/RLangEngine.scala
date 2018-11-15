package org.chronotics.silvertongue.scala.engine

import java.util.concurrent.atomic.AtomicInteger

import jep.Jep
import org.chronotics.silverbullet.scala.akka.protocol.Message.SendTaskToExecuteNoActorRef
import org.chronotics.silverbullet.scala.akka.state.{Failed, Finished, NotSatisfied, Working}
import org.chronotics.silvertongue.scala.util.RedisDataConversion
import org.rosuda.REngine.{REXP, REXPLogical, REngine}

import scala.collection.immutable.HashMap
import scala.util.control.Breaks._

class RLangEngine(strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, var objRengine: REngine)
  extends AbstractLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, "R", objRengine) {

  var objAtomicInteger = new AtomicInteger()
  var intPortIndex = 0
  var bIsShared: Boolean = false
  var objPythonEngine: Jep = null

  override def openLanguageEngine(strLanguage: String, objCurREngine: REngine) = {
    if (objRengine == null) {
      try {
        objRengine = objCurREngine
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
        }
      }
    }
  }

  override def executeLanguageEngine(bShouldExit: Boolean = true) = {
    if (!objTaskToExecute.isMergeMode) {
      if (objRengine != null) {
        var strErr = ""
        var strEngine = "RJava"
        var arrPrevResult: Map[String, Any] = Map.empty
        var arrPrevResultKey = objTaskToExecute.arrPrevResultKey
        var endLoopCondition = objTaskToExecute.endLoopCondition

        try {
          if (openConnection(objTaskToExecute)) {
            mapAttr = HashMap.empty
            mapInputKey = HashMap.empty
            mapInputField = Seq.empty
            mapPrevOutputField = Map.empty
            mapPropField = Map.empty
            mapOutputField = Seq.empty
            mapPropFieldType = Map.empty
            mapPrevInput = Map.empty

            //Get List In-Out Connection
            getListInputOutputParam(objTaskToExecute)

            //Read Input
            arrPrevResult = getListInputOutputValue(objTaskToExecute)

            log.info("arrPrevResult: " + arrPrevResult)

            //Execute
            executeScript(objTaskToExecute, arrPrevResult)

            //Remove all variables
            objRengine.parseAndEval("rm(list=ls(all=TRUE))")
            //objRengine.parseAndEval("redisClose()")
          } else {
            writeResultToRedis(Failed.toString, true, true)
          }
        } catch {
          case ex: Throwable => {
            log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, ex.getCause + " - " + ex.getMessage + " - " + ex.getStackTraceString))

            writeResultToRedis(Failed.toString, true, true)
          }
        }
      }
    }

    if (bShouldExit) {
      try {
        objRengine.parseAndEval("redisClose()")
      } catch {
        case ex: Throwable => {}
      }
      System.exit(0)
    }
  }

  def openConnection(task: SendTaskToExecuteNoActorRef): Boolean = {
    if (objRengine != null) {

      try {
        objRengine.parseAndEval("library(\"data.table\")")
        objRengine.parseAndEval("library(\"rredis\")")
        objRengine.parseAndEval("library(\"jsonlite\")")

        try {
          objRengine.parseAndEval("redisSelect(" + redisDB + ")")
        } catch {
          case ex: Throwable => {
            objRengine.parseAndEval("redisConnect(host = \"" + redisHost + "\", port = " + redisPort + ", password = \"" + redisPass + "\")")
            objRengine.parseAndEval("redisSelect(" + redisDB + ")")
          }
        }

        true
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
          updateMessageToKafka(ex.getMessage, Failed)
          false
        }
      }
    } else {
      updateMessageToKafka("Can't start RJava Engine", Failed)
      false
    }
  }

  def getListInputOutputParam(task: SendTaskToExecuteNoActorRef) = {
    log.info("task: " + task)
    log.info("task.libMeta: " + task.libMeta)
    log.info("task.arrPrevResultKey: " + task.arrPrevResultKey)

    if (task.libMeta != null && task.arrPrevResultKey != null) {
      for (curKey <- task.arrPrevResultKey) {
        for (curParam <- curKey._2) {
          mapInputField = mapInputField :+ curParam._1
          mapPrevOutputField = mapPrevOutputField + (curParam._1 -> curParam._2)
          mapPropFieldType = mapPropFieldType + (curParam._1 -> "Entire Result")
        }
      }

      for (curInput <- task.libMeta.arrParam.arrLibParam) {
        log.info("curInput: " + curInput.strName)
        log.info("curInput: " + curInput.paramType)

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

        mapPropFieldType = mapPropFieldType + (curInput.strName -> curInput.strDataType)
      }

      for (curInput <- task.libMeta.arrParam.arrLibParam) {
        log.info("curInput.strName: " + curInput.strName)
        log.info("curInput.paramType: " + curInput.paramType)
        log.info("curInput.strDataType: " + curInput.strDataType)

        if (curInput.paramType == "prop" && (task.arrParam == null || task.arrParam.length <= 0)) {
          curInput.dDefaultValue match {
            case Some(x) => {
              var strValue: String = x.toString()

              log.info("curInput-value: " + strValue)

              if (curInput.strDataType == "String" || curInput.strDataType == "Resource") {
                strValue = "'" + strValue.replace("'", "") + "'"
              }

              mapPrevOutputField.foreach { curItem =>

                if (strValue.contains(curItem._2 + "[,")) {
                  strValue = strValue.replace(curItem._2 + "[,", curItem._1 + "[,");
                } else if (strValue.contains(curItem._2 + "$")) {
                  strValue = strValue.replace(curItem._2 + "$", curItem._1 + "$");
                } else if (strValue.equals(curItem._2)) {
                  strValue = curItem._1
                }
              }

              (mapPropField = mapPropField + (curInput.strName -> strValue))
            }
            case None =>
          }

        }
      }
    }

    mapInputField = mapInputField.distinct

    log.info("mapInputField: " + mapInputField)
    log.info("mapPrevInput: " + mapPrevInput)
  }

  def readRedisFromREngine(strKey: String, strField: String, strProp: String, isOnlyCheckKey: Boolean): Boolean = {
    var bIsExist: Boolean = false

    try {
      //Check if null

      objRengine.parseAndEval("z2_check_field_redis <- redisHExists(\"" + strKey + "\", \"" + strField + "\")")
      var objExist = objRengine.parseAndEval("z2_check_field_redis")

      if (objExist.isLogical()) {
        var objLogical = objExist.asInstanceOf[REXPLogical]

        if (objLogical.isTRUE().length > 0 && objLogical.isTRUE()(0)) {

          if (!isOnlyCheckKey) {
            objRengine.parseAndEval(strProp + " <- redisHGet(\"" + strKey + "\", \"" + strField + "\")")
          }

          bIsExist = true
        }
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
      }
    }

    bIsExist
  }

  def writeRedisFromREngine(strKey: String, strField: String, strProp: String,
                            isVariable: Boolean = false, isWriteByte: Boolean = true, isParsedFullData: Boolean = false, isWriteJSON: Boolean = true) = {
    val strJSONKey = strKey + "_json"
    val strPrettyKey = strKey + "_pretty"

    try {
      if (isWriteByte) {
        //Check size of object
        objRengine.parseAndEval("obj_size <- as.double(object.size(" + strProp + "))")
        var dbObjectSize = objRengine.parseAndEval("obj_size").asDouble()

        if (dbObjectSize > maxObjectSize) {
          writeFileFromREngine(strKey, strField, strProp, isVariable, isWriteJSON)
        } else {
          try {
            objRengine.parseAndEval("redisHSet(\"" + strKey + "\", \"" + strField + "\", " + strProp + ")")
            objRengine.parseAndEval("redisExpire(\"" + strKey + "\", " + redisExpireTime.toString + ")")

            if (isWriteJSON) {
              try {
                objRengine.parseAndEval(strProp + "_json <- toJSON(" + strProp + ")")
                var strPropJSON: String = objRengine.parseAndEval(strProp + "_json").asStrings()(0)

                if (strPropJSON != null && !strPropJSON.isEmpty()) {
                  writeOutput(strJSONKey, strField, strPropJSON)
                }

                objRengine.parseAndEval("redisExpire(\"" + strJSONKey + "\", " + redisExpireTime.toString + ")")
              } catch {
                case ex: Throwable => {
                  log.error("ERR: " + ex.getStackTraceString)
                }
              }
            }

            var strPrettyProp = "pretty_ouput_data <- capture.output(str(" + strProp + "))"
            objRengine.parseAndEval(strPrettyProp)

            var objREXP = objRengine.parseAndEval("pretty_ouput_data")

            try {
              var strPrettyOutput: String = objREXP.asStrings().mkString("\r\n")
              writeOutput(strPrettyKey, strField, strPrettyOutput)
            } catch {
              case ex: Throwable => {
              }
            }

            //objRengine.parseAndEval("redisHSet(\"" + strPrettyKey + "\", \"" + strField + "\", " + strPrettyProp + ")")
            //objRengine.parseAndEval("redisExpire(\"" + strPrettyKey + "\", " + redisExpireTime.toString + ")")
          } catch {
            case ex: Throwable => {
              log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Object is too big to save in Redis. Using File System instead"))
              writeFileFromREngine(strKey, strField, strProp, isVariable, isWriteJSON)
            }
          }
        }
      } else {
        try {
          var objData = objRengine.parseAndEval(strProp)

          //1log.info("Write Direct - " + strProp + ": " + objData.toDebugString())
          var strParsedData: String = ""

          strParsedData = "objData";
          //          if (!isParsedFullData) {
          //            strParsedData = convertREXPToString(objData, 0, "[Input]", false, 1, 5).toString()
          //          } else {
          //            strParsedData = convertREXPToString(objData, 0, "[Input]", false, -1, -1).toString()
          //          }

          writeOutput(strKey, strField, strParsedData)
        } catch {
          case ex: Throwable => {
            log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
          }
        }
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
      }
    }
  }

  def writeFileFromREngine(strKey: String, strField: String, strProp: String, isVariable: Boolean, isWriteJSON: Boolean) = {
    if (isVariable) {
      objRengine.parseAndEval("redisHSet(\"" + strKey + "\", \"" + strField + "\", \"VAR\"")
      objRengine.parseAndEval("redisExpire(\"" + strKey + "\", " + redisExpireTime.toString + ")")
    } else {
      //Serialize object in R
      //Get byte arrays and write to file system
      val strFilePath: String = strRscriptDir + "/output/" + strKey + "_" + strProp + ".txt"
      objRengine.parseAndEval(strProp + "_ser <- serialize(" + strProp + ", NULL)")
      objRengine.parseAndEval("save(" + strProp + "_ser, file = \"" + strFilePath + "\")")

      objRengine.parseAndEval("redisHSet(\"" + strKey + "\", \"" + strField + "\", \"FILE: " + strFilePath + "\")")

      objRengine.parseAndEval("redisExpire(\"" + strKey + "\", " + redisExpireTime.toString + ")")

      if (isWriteJSON) {
        objRengine.parseAndEval("redisHSet(\"" + strKey + "_json\", \"" + strField + "\", \"FILE: " + strFilePath + "\")")
        objRengine.parseAndEval("redisExpire(\"" + strKey + "_json\", " + redisExpireTime.toString + ")")
      }
    }
  }

  def getListInputOutputValue(task: SendTaskToExecuteNoActorRef): Map[String, Any] = {
    //readInput(mapPrevInput, mapPrevOutputField, mapPropFieldType)
    //readInput(arrInputKey: Map[String, Seq[String]], arrPrevOutput: Map[String, String], arrFieldType: Map[String, String])
    var seqResult: Map[String, Any] = Map.empty

    mapInputKey = HashMap.empty

    if (mapPrevInput == null)
      null

    try {
      for (curKey: (String, Seq[String]) <- mapPrevInput) {
        log.info("curKey: " + curKey)

        for (curProp: String <- curKey._2) {
          log.info("curProp: " + curProp)
          var curPrevOutput = mapPrevOutputField.get(curProp) match {
            case Some(txt) => txt
            case None => ""
          }

          if (curPrevOutput == "") {
            curPrevOutput = curProp
          }

          var bIsExist = readRedisFromREngine(curKey._1, curPrevOutput, curProp, true)

          if (bIsExist) {
            var curPrevLang: String = ""

            try {
              curPrevLang = readOutput(curKey._1 + "_lang", "lang").toString()
            } catch {
              case ex: Throwable => {
                curPrevLang = ""
              }
            }

            if (curPrevLang != null && !curPrevLang.isEmpty() && !curPrevLang.equals("R")) {
              //redisDataConversion.convertRedisObject(curPrevLang, "R", curKey._1, curPrevOutput, curProp, "")
              if (curPrevLang.equals("Python")) {
                executePythonWithR(curKey._1, curPrevOutput, strPythonEnv, curProp)
              } else {
                //Native Language
                var redisDataConversion = new RedisDataConversion(null, objRengine, null, "", redisHost, redisPort, redisDB, redisPass)
                redisDataConversion.convertFromNativeToR(curKey._1, curPrevOutput, curProp, "")
              }

              seqResult = seqResult + (curProp -> 0)
            } else {
              var bIsExist = readRedisFromREngine(curKey._1, curPrevOutput, curProp, false)

              if (bIsExist) {
                var curResult: REXP = objRengine.parseAndEval(curProp)

                if (!curResult.isNull()) {
                  var bIsFile: Boolean = false

                  if (curResult.isString()) {
                    var strResult = curResult.asString()

                    if (strResult.contains("FILE:")) {
                      val strFilePath = strRscriptDir + "/output/" + curKey._1 + "_" + curPrevOutput + ".txt"

                      objRengine.parseAndEval("load(file = \"" + strFilePath + "\")")
                      objRengine.parseAndEval(curProp + " <- unserialize(" + curPrevOutput + "_ser)")

                      bIsFile = true
                    }

                    if (!strResult.isEmpty()) {
                      //Check already existed in env
                      objRengine.parseAndEval("z2_check_var <- exists(\"" + curProp + "\")")
                      var curCheck = objRengine.parseAndEval("z2_check_var")

                      if (curCheck != null && curCheck.isLogical()) {
                        var objLogical = curCheck.asInstanceOf[REXPLogical]

                        if (objLogical.isFALSE().length > 0 && objLogical.isFALSE()(0)) {
                          objRengine.parseAndEval(curProp + " <- \"" + strResult.replace("\"", "").replace("'", "") + "\")")
                        } else {
                          try {
                            curCheck = objRengine.parseAndEval(curProp)
                          } catch {
                            case ex: Throwable => {

                            }
                          }
                        }
                      }
                    }
                  }

                  seqResult = seqResult + (curProp -> (bIsFile match {
                    case true => 1
                    case false => 0
                  }))
                }
              }
            }
          }
        }
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
        updateMessageToKafka(ex.getMessage, Failed)
      }
    }

    seqResult
  }

  def executePythonWithR(redisKey: String, redisField: String, pythonEnv: String, paramInput: String): Any = {
    try {
      var cmdBuilder: StringBuilder = StringBuilder.newBuilder

      var cmd: String = "library(reticulate)"
      cmdBuilder.append(cmd).append(";")

      cmd = "use_python('" + (pythonEnv.isEmpty() match { case true => "/usr/bin/python3.6"; case false => pythonEnv }) + "')"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyYAML <- import('yaml')"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyRedis <- import('pyredis')"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyConnect <- pyRedis$Client(host=\"" + redisHost + "\", port=as.integer(" + redisPort + "), database=as.integer(" + redisDB + "), password=\"" + redisPass + "\")"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyResult <- pyConnect$hget(\"" + redisKey + "\", \"" + redisField + "\")"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyResult <- pyYAML$load(pyResult)"
      cmdBuilder.append(cmd).append(";")

      cmd = paramInput + " <- tryCatch({ py_to_r(pyResult) }, error = function(e) { pyResult })"
      cmdBuilder.append(cmd)
      objRengine.parseAndEval(cmdBuilder.toString())
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
      }
    }
  }

  def executeEndLoopCondition(task: SendTaskToExecuteNoActorRef): Boolean = {
    var isEndLoop: Boolean = true
    var strCmd = "isEnd <- " + task.endLoopCondition.endLoopCondition
    objRengine.parseAndEval(strCmd)

    strCmd = "isEnd"
    val objResult = objRengine.parseAndEval(strCmd)

    if (objResult != null && objResult.isLogical()) {
      var objLogical = objResult.asInstanceOf[REXPLogical]

      if (objLogical.length() == 1) {
        val curString = objLogical.asIntegers()(0)
        if (curString == 1) {
          isEndLoop = true
        } else if (curString == 0) {
          isEndLoop = false
        }
      }
    }

    isEndLoop
  }

  def executePrevCondition(task: SendTaskToExecuteNoActorRef): Boolean = {
    var isAllStatisfied: Boolean = true

    if (task.prevCondition != null && task.prevCondition.arrCondition != null
      && task.prevCondition.arrCondition.size > 0) {
      breakable {
        task.prevCondition.arrCondition.foreach { curCondition =>
          if (!curCondition.trim().isEmpty()) {
            var isStatisfied = executeBooleanCmd(curCondition.trim())

            if (!isStatisfied) {
              isAllStatisfied = false
              break
            }
          }
        }
      }
    }

    isAllStatisfied
  }

  def executeBooleanCmd(cmd: String): Boolean = {
    var bResult: Boolean = true
    var strCmd = "checkCmd <- " + cmd
    objRengine.parseAndEval(strCmd)

    strCmd = "checkCmd"
    val objResult = objRengine.parseAndEval(strCmd)

    if (objResult != null && objResult.isLogical()) {
      var objLogical = objResult.asInstanceOf[REXPLogical]

      if (objLogical.length() == 1) {
        val curString = objLogical.asIntegers()(0)
        if (curString == 1) {
          bResult = true
        } else if (curString == 0) {
          bResult = false
        }
      }
    }

    bResult
  }

  def executeScript(task: SendTaskToExecuteNoActorRef, arrPrevResult: Map[String, Any]) = {
    var bResult: Boolean = false
    var arrPrevResultKey = task.arrPrevResultKey
    var isEndLoop: Boolean = true
    var isPrevConditionSatisfied: Boolean = true

    if (arrPrevResultKey == null || (arrPrevResult.size == mapInputField.length)) {
      val strRedisKey = configGeneral.getString("redis.key-task-result") + task.objRequest.iRequestId + "_" + task.objRequest.iParentTaskId + "_" + task.iTaskId
      val strRedisInputKey = strRedisKey + "_input"
      val strRedisVarKey = strRedisKey + "_all_vars"
      val strRedisLangKey = strRedisKey + "_lang"

      try {
        if (objRengine != null) {
          objRengine.parseAndEval("print(\"=======Start " + task.libMeta.strName + "=======\")")
          //sendSimpleMessageToBroker("rWriteConsole: =======Start " + task.libMeta.strName + "=======")

          //Save Input Value for current node
          var strArgs = ""

          for (curItem: (String, Any) <- arrPrevResult) {
            //            if (curItem._2.toString().toInt == 1) {
            //              writeRedisFromREngine(strRedisInputKey, curItem._1, curItem._1, true, false, false)
            //            }
            writeRedisFromREngine(strRedisInputKey, curItem._1, curItem._1, true, false, false, false)
          }

          //Load Property
          for (curItem: (String, String) <- mapPropField) {
            var dataFieldType = mapPropFieldType.get(curItem._1) match {
              case Some(x) => x.toString()
              case None => ""
            }

            var curCmd = ""
            var curVal = curItem._2.toString
            curVal = curItem._2.toString.replace("'", "\"")

            if (dataFieldType.startsWith("Number") || dataFieldType.startsWith("Resource")) {
              curCmd = curItem._1 + " <- " + curVal
            } else if (dataFieldType.startsWith("Boolean")) {
              curCmd = curItem._1 + " <- " + curVal.replace("\"", "")
            } else {
              if (curVal.trim().isEmpty()) {
                curVal = "\"\""
              }

              if (curVal.contains("data.frame(") || curVal.contains("cbind(") || curVal.contains("[,") || curVal.contains("$")) {
                curCmd = curItem._1 + " <- " + curVal
              } else {
                curVal = "\"" + curVal.replace("\"", "") + "\""
                curCmd = curItem._1 + " <- " + curVal
              }
            }

            objRengine.parseAndEval(curCmd)

            writeRedisFromREngine(strRedisInputKey, curItem._1, curItem._1, true, false, false, false)

            strArgs += (curItem._1 + " = " + curItem._1 + ",");
          }

          if (strArgs != "") {
            strArgs = strArgs.substring(0, strArgs.length - 1)
          }

          //Re-assign iteration
          if (task.endLoopCondition != null && !task.endLoopCondition.counter.isEmpty()) {
            try {
              objRengine.parseAndEval(task.endLoopCondition.counter)
            } catch {
              case ex: Throwable => {
                log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
              }
            }
          }

          var strCmd: String = ""
          //Execute R Code
          var strRSource: String = strRscriptDir + "/" + strRscriptNameTemplate + "_" + task.strType + ".R"

          //Load Function
          strCmd = "source(\"" + strRSource + "\")"

          try {
            var arrScriptContent = fileConnection.readFile(strRSource, true)

            if (arrScriptContent.length == 1) {
              strCmd = arrScriptContent(0)
            }
          } catch {
            case ex: Throwable => {
              log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
            }
          }

          //Save Plot Result to Redis First
          var strPlotBuilder = StringBuilder.newBuilder

          for (intCountPlot <- 0 to 100) {
            var strCurPlot = "plot_" + task.strType + "_" + intCountPlot + ".jpg"

            if (strCmd.contains(strCurPlot)) {
              strCmd = strCmd.replace(strCurPlot, task.objRequest.iRequestId + "_" + strCurPlot);
              strPlotBuilder = strPlotBuilder.append(task.objRequest.iRequestId + "_" + strCurPlot).append("|")
            }
          }

          writeOutput(strRedisKey, "fixed_plot", strPlotBuilder.toString);
          writeOutput(strRedisLangKey, "lang", "R");

          //Save Dynamic Plot to Redis
          var strDynamicPlot = "html_" + task.strType + "_0.html"

          if (strCmd.contains(strDynamicPlot)) {
            strCmd = strCmd.replace(strDynamicPlot, task.objRequest.iRequestId + "_" + strDynamicPlot);
            writeOutput(strRedisKey, "dynamic_plot", task.objRequest.iRequestId + "_" + strDynamicPlot);
          }

          var isCheckPrevConditions = executePrevCondition(task)

          if (isCheckPrevConditions) {
            //Parse main script
            objRengine.parseAndEval(strCmd)
            //Write Output
            for (curOutput <- task.libMeta.arrParam.arrLibParam) {
              if (curOutput.paramType == "output") {
                strCmd = curOutput.strName
                writeRedisFromREngine(strRedisKey, curOutput.strName, curOutput.strName)
              }
            }

            if (task.endLoopCondition != null && !task.endLoopCondition.endLoopCondition.trim().isEmpty()) {
              isEndLoop = executeEndLoopCondition(task)
            } else {
              if (task.endLoopCondition == null) {
                isEndLoop = true
              }
            }

            objRengine.parseAndEval("print(\"=======End " + task.libMeta.strName + "=======\")")
          } else {
            isPrevConditionSatisfied = false
            updateMessageToKafka("", NotSatisfied)
          }
        } else {
          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "REngine is null"))
        }
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString, "C"))
          updateMessageToKafka(ex.getStackTraceString, Failed)
        }
      }
    } else {
      val strErr = "Haven't enough prev results to run this library"

      updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, strErr), Failed)
    }

    writeResultToRedis(Finished.toString, isEndLoop, isPrevConditionSatisfied)
  }
}
