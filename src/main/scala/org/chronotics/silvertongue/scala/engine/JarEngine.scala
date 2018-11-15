package org.chronotics.silvertongue.scala.engine

import java.io.{BufferedReader, InputStreamReader}

import com.jayway.jsonpath.JsonPath
import org.chronotics.silverbullet.scala.akka.protocol.Message.SendTaskToExecuteNoActorRef
import org.chronotics.silverbullet.scala.akka.state.{Failed, Finished, Working}
import org.chronotics.silvertongue.java.util.RedisDataConversion
import org.rosuda.REngine.REngine

import scala.compat.Platform.EOL
import scala.collection.immutable.HashMap
import scala.util.control.Breaks._

class JarLangEngine(strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, var objRengine: REngine)
  extends AbstractLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, "Jar", null) {
  val redisKeyTemplate = configGeneral.getString("redis.key-task-result")
  val redisWarKeyTemplate = configGeneral.getString("redis.war-key")
  val shBashPath: String = configGeneral.getString("sh.bash_path")
  val shBashAnnotation: String = configGeneral.getString("sh.bash_annotation")
  val warLog: String = configGeneral.getString("war.log_file")
  val shWarShTemplate: String = configGeneral.getString("sh.bash_file")
  val shWarTempShTemplate: String = configGeneral.getString("sh.bash_tmp_file")
  val shShPath: String = configGeneral.getString("sh.sh_path")
  var numWarRepeat: Int = configGeneral.getInt("war.num_repeat")
  var objRedisKeyLib: RedisDataConversion = new RedisDataConversion(redisHost, redisPort, redisPass, redisDB, redisKeyTemplate)

  override def openLanguageEngine(strLanguage: String, objCurREngine: REngine) = {
  }

  override def executeLanguageEngine(bShouldExit: Boolean = true) = {
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Received task: " + objTaskToExecute))

    try {
      execJarFile(objTaskToExecute)
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getCause + " - " + ex.getMessage + " - " + ex.getStackTraceString))
      }
    }

    writeResultToRedis(Finished.toString, true, true)
  }

  def execJarFile(task: SendTaskToExecuteNoActorRef): Boolean = {
    var bResult: Boolean = false

    try {
      try {
        //Get Input, Output, Properties
        var mapInputField: Seq[String] = Seq.empty
        var mapPrevOutputField: Map[String, String] = Map.empty
        var mapPropField: Map[String, String] = Map.empty
        var mapOutputField: Seq[String] = Seq.empty
        var mapPropFieldType: Map[String, String] = Map.empty

        if (task.libMeta != null) {
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
                  (mapPropField = mapPropField + (curInput.strName -> strValue))
                }
                case None =>
              }

            }

            mapPropFieldType = mapPropFieldType + (curInput.strName -> curInput.strDataType)
          }
        }

        var arrPrevResultKey = task.arrPrevResultKey
        var mapPrevInput: Map[String, Seq[String]] = Map.empty

        for (prevKey <- arrPrevResultKey) {
          mapPrevInput = mapPrevInput + (prevKey._1 -> prevKey._2.keySet.toSeq)
        }

        var arrPrevResult: Map[String, Any] = Map.empty

        //Load Previous Result
        arrPrevResult = readInput(mapPrevInput, mapPrevOutputField)

        //Write Output
        val strRedisKey = configGeneral.getString("redis.key-task-result") + task.objRequest.iRequestId + "_" + task.objRequest.iParentTaskId + "_" + task.iTaskId
        val strRedisInputKey = strRedisKey + "_input"
        val strRedisKeyPretty = strRedisKey + "_pretty"

        if (arrPrevResultKey == null || (arrPrevResult.size == mapInputField.length)) {
          //Prepare Jar File
          //Prepare command to run in descriptive text file
          var strRSource: String = strRscriptDir + "/" + strRscriptNameTemplate + "_" + task.strType + ".txt"
          var strRSourceContent: String = ""
          var strJarFile: String = ""

          strRSourceContent = fileConnection.readFile(strRSource, true)(0)

          try {
            var restResultField: List[String] = List.empty
            var mapResultField: HashMap[Int, String] = HashMap.empty
            var successCommand: String = ""

            //Replace command with arguments and execute
            updateMessageToKafka("=======Start " + task.libMeta.strName + "=======" + EOL, Working)

            //Execute War File will just receive input fields, and property fields in simple types (String, and Number)
            //First line: defined how to execute war file
            //There are some required fields that have to be replaced by orders
            //{jarPath}
            //{arg_name_1}, {arg_name_2}, {arg_name_3}...
            //Output of Jar File will be also only in simple types (String, Number)

            //Replace Jar Path
            breakable {
              for (curItem: (String, String) <- mapPropField) {
                if (curItem._1.toLowerCase().equals("jar_path")) {
                  strJarFile = curItem._2
                  strRSourceContent = strRSourceContent.replace("{" + curItem._1 + "}", curItem._2)

                  break
                }
              }
            }

            strRSourceContent = strRSourceContent.replace("\n", " ")

            for (curItem: (String, String) <- mapPropField) {
              if (!curItem._1.toLowerCase().equals("jar_path")) {
                var strJarArg: String = "-D" + curItem._1 + "=" + curItem._2
                strRSourceContent = (strJarArg + " " + strRSourceContent)
              }

              writeOutput(strRedisInputKey, curItem._1, curItem._2)
            }

            //Add Input variables to script
            for (curItem: (String, Any) <- arrPrevResult) {
              var strJarArg: String = "-D" + curItem._1 + "=" + curItem._2
              strRSourceContent = (strJarArg + " " + strRSourceContent)

              writeOutput(strRedisInputKey, curItem._1, curItem._2)
            } //Finished to prepare command, run it with child-process and input/output

            strRSourceContent = strRSourceContent.replace("\n", " ").trim

            var mapJarResult = runJarChildProcess("java " + strRSourceContent, mapOutputField)

            updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "=======End " + task.libMeta.strName + "=======" + EOL), Working)
            updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, ""), Finished)

            for (curOutput <- mapJarResult) {
              writeOutput(strRedisKey, curOutput._1, curOutput._2)
              writeOutput(strRedisKeyPretty, curOutput._1, curOutput._2)
            }
          } catch {
            case ex: Throwable => {
              bResult = false
              log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))

              updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString), Failed)
            }
          }
        }
      } catch {
        case ex: Throwable => {
          bResult = false
          log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))

          updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString), Failed)
        }
      }
    } catch {
      case ex: Throwable => {
        bResult = false
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))

        updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString), Failed)
      }
    }

    bResult
  }

  def runJarChildProcess(strCmd: String, arrOutputField: Seq[String]): HashMap[String, Any] = {
    var mapResult: HashMap[String, Any] = HashMap.empty
    var strLastLine: String = "";

    log.info("runJarChildProcess - strCmd: " + strCmd)

    try {
      var objRuntime: Runtime = Runtime.getRuntime();
      var objProcess = objRuntime.exec(strCmd);

      //var objRuntime: ProcessBuilder = new ProcessBuilder(lstCmd);
      //var objProcess = objRuntime.start();

      try {
        var isr = new InputStreamReader(objProcess.getErrorStream)
        var brErr = new BufferedReader(isr)

        isr = new InputStreamReader(objProcess.getInputStream)
        var brOut = new BufferedReader(isr)
        var exitVal = objProcess.waitFor

        strLastLine = getExecLog(brErr, brOut)
      } catch {
        case ioe: Throwable => log.error("ERR: " + ioe.getStackTraceString)
      }
    } catch {
      case ex: Throwable => log.error("ERR: " + ex.getStackTraceString)
    }

    if (strLastLine != null && !strLastLine.isEmpty()) {
      log.info("runJarChild - Output: " + strLastLine)

      for (curField <- arrOutputField) {
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "JarJSONField: " + curField))

        var strCurField = curField.startsWith("$.") match {
          case true => curField
          case false => "$." + curField
        }

        val strFieldResult: String = JsonPath.parse(strLastLine).read[Any](strCurField).toString
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "JarJSONField - " + curField + ": " + strFieldResult))

        mapResult = mapResult + (curField -> strFieldResult)
      }
    }

    mapResult
  }

  def getExecLog(buffError: BufferedReader, buffOp: BufferedReader): String = {
    var strLastLine = ""
    var mapResult: HashMap[String, Any] = HashMap.empty

    try {
      if (buffError != null) {
        var line = buffError.readLine()

        while (line != null && line != "") {
          log.info("> " + line)
          strLastLine = line

          updateMessageToKafka(line, Working)

          line = buffError.readLine()
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
      }
    }

    try {
      if (buffOp != null) {
        var line = buffOp.readLine()

        while (line != null && line != "") {
          log.info("> " + line)
          strLastLine = line

          updateMessageToKafka(line, Working)

          line = buffOp.readLine()
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
      }
    }

    try {
      buffError.close();
      buffOp.close();
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
      }
    }

    strLastLine
  }

  def readInput(arrInputKey: Map[String, Seq[String]], arrPrevOutput: Map[String, String]): Map[String, Any] = {
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

          var curResult = objRedisKeyLib.getContentObjectOfKey(0, 0, curPrevOutput, curKey._1, curKey._1 + "_json").toString

          if (curResult == null || curResult.isEmpty()) {
            curResult = redisCli.hGet(curKey._1, curPrevOutput) match {
              case Some(x) => x.toString
              case None => ""
            }
          }

          log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "curResult: " + curResult))

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
