package org.chronotics.silvertongue.scala.engine

import java.io.PrintStream

import jep.{Jep, SharedInterpreter}
import org.chronotics.silverbullet.scala.akka.protocol.Message.SendTaskToExecuteNoActorRef
import org.chronotics.silverbullet.scala.akka.state.{Failed, Finished, NotSatisfied, Working}
import org.chronotics.silverbullet.scala.kafka.LangEngineOutputListener
import org.chronotics.silvertongue.scala.util.RedisDataConversion
import org.rosuda.REngine.REngine

import scala.collection.immutable.HashMap
import scala.compat.Platform.EOL
import scala.util.control.Breaks._

class PythonLangEngine(strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, var objRengine: REngine)
  extends AbstractLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, "Python", null) {
  override def openLanguageEngine(strLanguage: String, objCurREngine: REngine) = {
  }

  override def executeLanguageEngine(bShouldExit: Boolean = true) = {
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Received task: " + objTaskToExecute))

    if (!objTaskToExecute.isMergeMode) {
      try {
        var strErr = "Can't connect to JPyServe"

        execPythonScript(objTaskToExecute, bShouldExit)
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getMessage + " - " + ex.getCause + " - " + ex.getStackTraceString))
          writeResultToRedis(Failed.toString, true, true)
        }
      }
    }

    if (bShouldExit) {
      closeKafkaProducer()
      System.exit(0)
    }
  }

  def getListInputOutputParam(task: SendTaskToExecuteNoActorRef) = {
    if (task.libMeta != null) {
      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "libMeta:" + task.libMeta))

      if (task.arrPrevResultKey != null && task.arrPrevResultKey.size > 0) {
        arrPrevResultKey = task.arrPrevResultKey

        for (curKey <- task.arrPrevResultKey) {
          for (curParam <- curKey._2) {
            mapInputField = mapInputField :+ curParam._1
            mapPrevOutputField = mapPrevOutputField + (curParam._1 -> curParam._2)
            mapPropFieldType = mapPropFieldType + (curParam._1 -> "Entire Result")
          }
        }
      }

      for (curInput <- task.libMeta.arrParam.arrLibParam) {
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

    mapPrevInput = Map.empty

    for (prevKey <- arrPrevResultKey) {
      mapPrevInput = mapPrevInput + (prevKey._1 -> prevKey._2.keySet.toSeq)
    }

    mapInputField = mapInputField.distinct
  }

  def executeMainScript(objJepPythonEngine: Jep, task: SendTaskToExecuteNoActorRef, arrPrevResult: Map[String, Any]) = {
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "Prev Result: " + arrPrevResult.size))
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "NumOfPrevOutput: " + mapPrevOutputField.size))
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "NumOutput: " + mapOutputField.length))
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "PrevKey: " + arrPrevResultKey))
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "NumInput: " + mapInputField.length))

    if (arrPrevResultKey == null || (arrPrevResult.size == mapInputField.length)) {
      val strRedisKey = configGeneral.getString("redis.key-task-result") + task.objRequest.iRequestId + "_" + task.objRequest.iParentTaskId + "_" + task.iTaskId
      val strRedisInputKey = strRedisKey + "_input"
      val strRedisLangKey = strRedisKey + "_lang"

      //Prepare Python Script
      var strRSource: String = strRscriptDir + "/" + strRscriptNameTemplate + "_" + task.strType + ".py"
      var strRSourceContent: String = ""
      var strPlotImg: String = ""

      strRSourceContent = fileConnection.readFile(strRSource, true)(0)

      if (strRSourceContent.contains("matplotlib") && strRSourceContent.contains("savefig")) {
        strPlotImg = task.objRequest.iRequestId + "_plot_" + task.libMeta.iId + "_0.jpg"

        writeOutput(strRedisKey, "fixed_plot", strPlotImg);
      }

      writeOutput(strRedisLangKey, "lang", "Python");

      try {
        updateMessageToKafka("=======Start " + task.libMeta.strName + "=======" + EOL, Working, "C")

        for (curItem: (String, String) <- mapPropField) {
          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "prop " + curItem._1 + ": " + curItem._2))

          val curFieldType = mapPropFieldType.get(curItem._1) match {
            case Some(txt) => txt
            case None => ""
          }

          var curVal = curItem._2.toString

          if (curFieldType.contains("String") || curFieldType.contains("Resource")) {
            log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "type: " + curFieldType))
            objJepPythonEngine.set(curItem._1, curItem._2.toString)
          } else if (curFieldType.contains("Number (")) {
            log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "type: " + curFieldType))
            objJepPythonEngine.set(curItem._1, curItem._2.toDouble)
          } else {
            log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "type: " + curFieldType))
            objJepPythonEngine.eval(curItem._1 + " = " + curItem._2)
          }

          objJepPythonEngine.eval("print(" + curItem._1 + ")")

          //writeOutput(strRedisInputKey, curItem._1, curVal)
        }

        //Add Input variables to script
        for (curItem: (String, Any) <- arrPrevResult) {
          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "in " + curItem._1 + ": " + curItem._2.toString))

          val curFieldType = mapPropFieldType.get(curItem._1) match {
            case Some(txt) => txt
            case None => ""
          }

          var curVal = curItem._2.toString
          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "curItem: " + curItem._1))

          if (!curItem._2.toString().equals("Python") && !curItem._2.toString().equals("R")) {
            if (curFieldType.contains("String")) {
              log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "type: " + curFieldType))
              objJepPythonEngine.set(curItem._1, curItem._2.toString)
            } else if (curFieldType.contains("Number (")) {
              log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "type: " + curFieldType))
              objJepPythonEngine.set(curItem._1, curItem._2.toString.toDouble)
            } else {
              log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "type: " + curFieldType))
              objJepPythonEngine.eval(curItem._1 + " = " + curItem._2.toString)
            }

            objJepPythonEngine.eval("print(" + curItem._1 + ")")
          }

          //writeOutput(strRedisInputKey, curItem._1, curVal)
        }

        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "Current Source: " + strRSource))
        objJepPythonEngine.runScript(strRSource)

        if (strPlotImg != "") {
          updateMessageToKafka("rWriteConsole: z2_akka_show_plot: " + strPlotImg, Working)
          updateMessageToKafka("rWriteConsole: \n", Working)
        }

        updateMessageToKafka("=======End " + task.libMeta.strName + "=======" + EOL, Working, "C")

        updateMessageToKafka("", Finished)

        for (curOutput <- task.libMeta.arrParam.arrLibParam) {
          if (curOutput.paramType == "output") {
            //var curConvert = objJepPythonEngine.getValue(curOutput.strName)

            //writeOutput(strRedisKey, curOutput.strName, curConvert)
            writeRedisInPython(objJepPythonEngine, strRedisKey, curOutput.strName)
          }
        }
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(task, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))
          updateMessageToKafka(ex.getStackTraceString, Failed)
        }
      } finally {
        try {
          objJepPythonEngine.close()
        } catch {
          case ex: Throwable => {}
        }
      }
    } else {
      val strErr = "Haven't enough prev results to run this library"

      updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, strErr), Failed)
    }
  }

  def execPythonScript(task: SendTaskToExecuteNoActorRef, bShouldExit: Boolean = true): Any = {
    var isPrevConditionSatisfied: Boolean = true
    var isEndLoop: Boolean = true

    try {
      //setRedirectOutputStreams: false -> inside python, true -> move to java System.out, System.err
      //Each Jep Engine has its own thread, so can't be shared between threads
      var objPipeOut: LangEngineOutputListener = new LangEngineOutputListener(task.objRequest.strCallback, task.objRequest.iUserId.toString, strRequestId,
        strWorkflowId, strTaskId, strLibId, kafkaProducer)
      System.setOut(new PrintStream(objPipeOut))

      var objJepPythonEngine: Jep = null

      objJepPythonEngine = new SharedInterpreter()

      try {
        objJepPythonEngine.eval("from jep import redirect_streams")
        objJepPythonEngine.eval("redirect_streams.setup()")
        objJepPythonEngine.eval("import inspect")
        objJepPythonEngine.eval("import yaml")
        objJepPythonEngine.eval("import pyredis")
        objJepPythonEngine.eval("import json")
        objJepPythonEngine.eval("import rpy2.robjects as robjects")
        objJepPythonEngine.eval("from rpy2.robjects import pandas2ri")

        objJepPythonEngine.eval("pandas2ri.activate()")

        var strCmd = "redisConn = pyredis.Client(host = '" + redisHost + "', port = " + redisPort + ", database = " + redisDB + ", password = '" + redisPass + "')"
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "strCmd: " + strCmd))

        objJepPythonEngine.eval(strCmd)
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(task, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))
        }
      }

      try {
        //Get Input, Output, Properties
        mapInputField = Seq.empty
        mapPrevOutputField = Map.empty
        mapPropField = Map.empty
        mapOutputField = Seq.empty
        mapPropFieldType = Map.empty

        getListInputOutputParam(task)

        var arrPrevResult: Map[String, Any] = Map.empty

        //Load Previous Result
        arrPrevResult = readInput(mapPrevInput, mapPrevOutputField, mapPropFieldType, objJepPythonEngine)

        var isCheckPrevConditions = executePythonPrevCondition(objJepPythonEngine, task)

        if (isCheckPrevConditions) {
          //Execute Main Script
          executeMainScript(objJepPythonEngine, task, arrPrevResult)

          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "Loop Condition: " + task.endLoopCondition))

          if (task.endLoopCondition != null && !task.endLoopCondition.endLoopCondition.trim().isEmpty()) {
            log.info(loggingIO.generateLogMesageTaskExecuteNoActor(task, Working, "Check Loop"))
            isEndLoop = executePythonEndLoopCondition(objJepPythonEngine, task)
          } else {
            if (task.endLoopCondition == null) {
              isEndLoop = true
            }
          }
        } else {
          isPrevConditionSatisfied = false
          updateMessageToKafka("", NotSatisfied)
        }
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(task, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))

          updateMessageToKafka(ex.getStackTraceString, Failed)
        }
      } finally {
        try {
          objJepPythonEngine.eval("redisConn.close()")
        } catch {
          case ex: Throwable => {
          }
        }

        objJepPythonEngine.close()
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(task, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))

        updateMessageToKafka(ex.getStackTraceString, Failed)
      }
    }

    writeResultToRedis(Finished.toString, isEndLoop, isPrevConditionSatisfied)
  }

  def executeRWithPython(pythonEngine: Jep, redisKey: String, redisField: String, paramInput: String, paramInputParam: String, propDataType: String): Object = {
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "executeRWithPython: " + redisKey + " - " + redisField + " - " + paramInput + " - " + paramInputParam))

    try {
      val pyStr: StringBuilder = StringBuilder.newBuilder
      pyStr.append("rString = \"function() { library(rredis);redisConnect('" + redisHost + "', " + redisPort + ", '" + redisPass + "');redisSelect(" + redisDB + ");")
      pyStr.append(paramInput + " <- redisHGet('" + redisKey + "', '" + redisField + "');")

      paramInputParam.isEmpty() match {
        case false => pyStr.append(paramInput + "$" + paramInputParam + " }\"")
        case true => pyStr.append(paramInput + " }\"")
      }

      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "pyStr " + pyStr.toString))

      pythonEngine.eval(pyStr.toString)

      pythonEngine.eval("rfunc = robjects.r(rString)")
      pythonEngine.eval("rdata = rfunc()")

      paramInputParam.isEmpty() match {
        case false => {
          pythonEngine.eval(paramInputParam + " = pandas2ri.ri2py(rdata)")

          if (propDataType.equals("String") || propDataType.startsWith("Number")) {
            pythonEngine.eval(paramInputParam + " = " + paramInputParam + "[0]")
          }

          //pythonEngine.getValue(paramInputParam)
          "true"
        }
        case true => {
          pythonEngine.eval(paramInput + " = pandas2ri.ri2py(rdata)")

          if (propDataType.equals("String") || propDataType.startsWith("Number")) {
            pythonEngine.eval(paramInput + " = " + paramInput + "[0]")
          }

          //pythonEngine.getValue(paramInput)
          "true"
        }
      }
    } catch {
      case ex: Throwable => {
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "ERR " + ex.getMessage + " - cause: " + ex.getCause + " - stackstrace: " + ex.getStackTraceString))
        log.error("ERR", ex)

        if (paramInputParam != null && !paramInputParam.isEmpty()) {
          try {
            val pyStr: StringBuilder = StringBuilder.newBuilder
            pyStr.append("rString = \"function() { library(rredis);redisConnect('" + redisHost + "', " + redisPort + ", '" + redisPass + "');redisSelect(" + redisDB + ");")
            pyStr.append(paramInputParam + " <- redisHGet('" + redisKey + "', '" + redisField + "');" + paramInputParam + " }\"")

            log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "pyStr2 " + pyStr.toString))

            pythonEngine.eval(pyStr.toString)

            pythonEngine.eval("rfunc = robjects.r(rString)")
            pythonEngine.eval("rdata = rfunc()")
            pythonEngine.eval(paramInputParam + " = pandas2ri.ri2py(rdata)")

            if (propDataType.equals("String") || propDataType.startsWith("Number")) {
              pythonEngine.eval(paramInputParam + " = " + paramInputParam + "[0]")
            }

            //pythonEngine.getValue(paramInputParam)
            "true"
          } catch {
            case ex2: Throwable => {
              log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex2.getStackTraceString))
              null
            }
          }
        } else {
          null
        }
      }
    }
  }

  def writeRedisInPython(objCurPythonEngine: Jep, strKey: String, strField: String) {
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "strKey: " + strKey + " - strField: " + strField))
    val strJSONKey = strKey + "_json"
    val strJSONPrettyKey = strKey + "_pretty"

    if (objCurPythonEngine != null) {
      try {
        var strCmd = "redisConn.hset(\"" + strKey + "\", \"" + strField + "\", yaml.dump(" + strField + "))"
        objCurPythonEngine.eval(strCmd)

        strCmd = "redisConn.expire(\"" + strKey + "\", " + redisExpireTime.toString + ")"
        objCurPythonEngine.eval(strCmd)
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))
        }
      }

      try {
        var strCmd = "redisConn.hset(\"" + strJSONKey + "\", \"" + strField + "\", json.dumps(" + strField + "))"
        objCurPythonEngine.eval(strCmd)

        strCmd = "redisConn.expire(\"" + strJSONKey + "\", " + redisExpireTime.toString + ")"
        objCurPythonEngine.eval(strCmd)

        strCmd = "redisConn.hset(\"" + strJSONPrettyKey + "\", \"" + strField + "\", json.dumps(" + strField + "))"
        objCurPythonEngine.eval(strCmd)

        strCmd = "redisConn.expire(\"" + strJSONPrettyKey + "\", " + redisExpireTime.toString + ")"
        objCurPythonEngine.eval(strCmd)
      } catch {
        case ex: Throwable => {
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))
        }
      }
    }
  }

  def executePythonEndLoopCondition(objJepPythonEngine: Jep, task: SendTaskToExecuteNoActorRef): Boolean = {
    if (task.endLoopCondition != null && task.endLoopCondition.endLoopCondition != null
      && !task.endLoopCondition.endLoopCondition.isEmpty()) {
      executeBooleanCmd(objJepPythonEngine, task.endLoopCondition.endLoopCondition)
    } else {
      true
    }
  }

  def executePythonPrevCondition(objJepPythonEngine: Jep, task: SendTaskToExecuteNoActorRef): Boolean = {
    var isAllSatisfied: Boolean = true

    if (task.prevCondition != null && task.prevCondition.arrCondition != null
      && task.prevCondition.arrCondition.size > 0) {
      breakable {
        task.prevCondition.arrCondition.foreach { curCondition =>
          if (!curCondition.trim().isEmpty()) {
            var isSatisfied = executeBooleanCmd(objJepPythonEngine, curCondition.trim())

            if (!isSatisfied) {
              isAllSatisfied = false
              break
            }
          }
        }
      }
    }

    isAllSatisfied
  }

  def executeBooleanCmd(objJepPythonEngine: Jep, cmd: String): Boolean = {
    var isResult: Boolean = true
    var strCmd = "check_condition = " + cmd
    objJepPythonEngine.eval(strCmd)

    strCmd = "check_condition"
    val objResult = objJepPythonEngine.getValue(strCmd)

    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "CheckCmd: " + objResult))

    if (objResult != null && objResult.isInstanceOf[Boolean]) {
      isResult = objResult.asInstanceOf[Boolean]
    }

    isResult
  }

  def readPythonObject(strKey: String, strField: String, strVar: String, objJepPythonEngine: Jep): Any = {
    try {
      var strCmd: String = strVar + " = redisConn.hget('" + strKey + "', '" + strField + "')"
      objJepPythonEngine.eval(strCmd)

      strCmd = strVar + " = yaml.load(" + strVar + ")"
      objJepPythonEngine.eval(strCmd)

      objJepPythonEngine.getValue(strVar)
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "ERR: " + ex.getMessage + " - caused by: " + ex.getCause + " - stacktrace: " + ex.getStackTraceString))
        null
      }
    }
  }

  def readInput(arrInputKey: Map[String, Seq[String]], arrPrevOutput: Map[String, String], arrPropFieldType: Map[String, String], objJepPythonEngine: Jep): Map[String, Any] = {
    var seqResult: Map[String, Any] = Map.empty

    mapInputKey = HashMap.empty

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

          var curPropDataType: String = arrPropFieldType.get(curProp) match {
            case Some(str) => str
            case None => ""
          }

          log.info("curPropDataType: " + curPropDataType)

          var curPrevLang: String = ""

          try {
            curPrevLang = redisCli.hGet(curKey._1 + "_lang", "lang") match {
              case Some(x) => x.toString()
              case None => ""
            }
          } catch {
            case ex: Throwable => {
              curPrevLang = ""
            }
          }

          var curResult: Any = null

          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "curPrevLang: " + curPrevLang))

          if (curPrevLang != null && !curPrevLang.isEmpty() && !curPrevLang.equals("Python")) {
            if (curPrevLang.equals("R")) {
              curResult = executeRWithPython(objJepPythonEngine, curKey._1, curPrevOutput, curPrevOutput, curProp, curPropDataType) //redisDataConversion.convertRedisObject(curPrevLang, "Python", curKey._1, curPrevOutput, curPrevOutput, curProp)
            } else {
              val redisDataConversion = new RedisDataConversion(null, null, objJepPythonEngine, strPythonEnv, redisHost, redisPort, redisDB, redisPass)
              curResult = redisDataConversion.convertFromNativeToPython(curKey._1, curPrevOutput, curPrevOutput, curProp)

              if (curResult != null && !curResult.toString().isEmpty()) {
                curResult = "Python"
              }
            }
          } else {
            curResult = readPythonObject(curKey._1, curPrevOutput, curProp, objJepPythonEngine)
          }

          if (curResult != null && curResult.toString != "") {
            if (curPrevLang.equals("Python") || curPrevLang.equals("R")) {
              seqResult = seqResult + (curProp -> curPrevLang)
            } else {
              seqResult = seqResult + (curProp -> curResult)
            }

          }
        }
      }
    } catch {
      case ex: Throwable => log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString))
    }

    seqResult
  }

  override def writeOutput(strOutKey: String, fieldName: String, result: Any) = {
    redisCli.hSet(strOutKey, fieldName, result)
  }
}