package org.chronotics.silvertongue.scala.engine

import java.io.File
import java.net.{InetAddress, URLEncoder}
import java.util.Calendar
import java.util.regex.Pattern

import com.jayway.jsonpath.JsonPath
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpPost}
import org.apache.http.impl.client.DefaultHttpClient
import org.chronotics.pandora.scala.network.NetworkIO
import org.chronotics.silverbullet.scala.akka.protocol.Message.SendTaskToExecuteNoActorRef
import org.chronotics.silverbullet.scala.akka.state.{Failed, Finished, Working}
import org.chronotics.silvertongue.java.util.RedisDataConversion
import org.rosuda.REngine.REngine

import scala.compat.Platform.EOL
import scala.collection.immutable.HashMap
import scala.sys.process.Process
import scala.util.control.Breaks._

class WarLangEngine(strRequestId: String, strWorkflowId: String, strTaskId: String, strLibId: String, var objRengine: REngine)
  extends AbstractLangEngine(strRequestId, strWorkflowId, strTaskId, strLibId, "War", null) {
  val redisKeyTemplate = configGeneral.getString("redis.key-task-result")
  val redisWarKeyTemplate = configGeneral.getString("redis.war-key")
  val shBashPath: String = configGeneral.getString("sh.bash_path")
  val shBashAnnotation: String = configGeneral.getString("sh.bash_annotation")
  val warLog: String = configGeneral.getString("war.log_file")
  val shWarShTemplate: String = configGeneral.getString("sh.bash_file")
  val shWarTempShTemplate: String = configGeneral.getString("sh.bash_tmp_file")
  val shShPath: String = configGeneral.getString("sh.sh_path")
  var numWarRepeat: Int = configGeneral.getInt("war.num_repeat")
  var curIpAddress: String = InetAddress.getLocalHost.getHostAddress
  var objRedisKeyLib: RedisDataConversion = new RedisDataConversion(redisHost, redisPort, redisPass, redisDB, redisKeyTemplate)

  override def openLanguageEngine(strLanguage: String, objCurREngine: REngine) = {
  }

  override def executeLanguageEngine(bShouldExit: Boolean = true) = {
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, "Received task: " + objTaskToExecute))

    try {
      execJarFile(objTaskToExecute)
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getCause + " - " + ex.getMessage + " - " + ex.getStackTraceString))
      }
    }

    writeResultToRedis(Finished.toString, true, true)
  }

  def checkWarFileNameInRedis(task: SendTaskToExecuteNoActorRef, newWarFileName: String, newPort: Int): (Boolean, Int) = {
    var bIsSameFile = false
    var intPort = newPort
    var strRedisWarKey = redisWarKeyTemplate + task.strType
    var strRedisWarPortKey = redisWarKeyTemplate + "port_" + task.strType
    var strRedisWarListKey = redisWarKeyTemplate + task.strType + "list"

    if (newWarFileName != null && !newWarFileName.isEmpty()) {
      try {
        redisCli.sSet(strRedisWarListKey, newWarFileName, -1)
      } catch {
        case ex: Throwable => {}
      }
    }

    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Check War - WarFileName: " + newWarFileName))
    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Check War - Port: " + newPort.toString))

    if (!newWarFileName.isEmpty()) {
      val strOldWarFileName = redisCli.getKey(strRedisWarKey) match {
        case Some(str) => str.toString
        case None => ""
      }

      if (strOldWarFileName != null && strOldWarFileName.toString().equals(newWarFileName)) {
        bIsSameFile = true
        intPort = redisCli.getKey(strRedisWarPortKey) match {
          case Some(str) => str.toString.toInt
          case None => newPort
        }
      } else {
        redisCli.setKey(strRedisWarKey, newWarFileName)

        if (newPort != -1) {
          redisCli.setKey(strRedisWarPortKey, newPort.toString)
        }
      }
    } else {
      redisCli.setKey(strRedisWarPortKey, newPort.toString)
    }

    (bIsSameFile, intPort)
  }

  //intAction == 1 -> Add, 2 -> Remove
  def updateNewRequestToRedisWar(strWarFileName: String, task: SendTaskToExecuteNoActorRef, intAction: Int) = {
    var strRedisWarKey = redisWarKeyTemplate + task.strType + "req_" + strWarFileName

    try {
      if (intAction == 1) {
        redisCli.zAdd(strRedisWarKey, task.objRequest.iRequestId.toString, Calendar.getInstance.getTimeInMillis)
      } else {
        redisCli.zRemove(strRedisWarKey, task.objRequest.iRequestId.toString)
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
      }
    }
  }

  def stopOldWar(strCurWarFileName: String, task: SendTaskToExecuteNoActorRef) = {
    try {
      var strRedisWarListKey = redisWarKeyTemplate + task.strType + "list"
      redisCli.sGet(strRedisWarListKey) match {
        case arr: Set[_] => {
          arr.foreach(optString => {
            optString match {
              case Some(strOldWarFileName) => {
                if (strOldWarFileName.isInstanceOf[String] && strOldWarFileName != null && !strOldWarFileName.equals(strCurWarFileName)) {
                  var strRedisWarKey = redisWarKeyTemplate + task.strType + "req_" + strOldWarFileName

                  var lRemainReq: Long = redisCli.zCard(strRedisWarKey)

                  if (lRemainReq <= 0) {
                    //Delete key
                    redisCli.delKey(strRedisWarKey)

                    //Stop war
                    var strKillMsg = "kill -9 `ps aux |grep \"" + strOldWarFileName + "\" |awk '{print $2}'`"

                    try {
                      val objProcess = Process(strKillMsg.trim())

                      val exitCode = objProcess.!

                      //Wait until the process will be finished
                      log.info("stopOldWar - exitCode: " + exitCode)
                    } catch {
                      case ex: Throwable => {
                      }
                    }
                  }
                }
              }
              case None =>
            }
          })
        }
        case null =>
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR: " + ex.getStackTraceString)
      }
    }
  }

  def getPortInURL(restURL: String): Int = {
    var intPort = -1

    try {
      var strRegex = "(:)(\\d+)(\\/)"
      val objPattern: Pattern = Pattern.compile(strRegex)
      val objMatcher = objPattern.matcher(restURL)

      breakable {
        while (objMatcher.find()) {
          intPort = objMatcher.group(2).toInt
          break
        }
      }
    } catch {
      case ex: Throwable =>
    }

    intPort
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

        if (arrPrevResultKey == null || (arrPrevResult.size == mapInputField.length)) {
          //Prepare Jar File
          //Prepare command to run in descriptive text file
          var strRSource: String = strRscriptDir + "/" + strRscriptNameTemplate + "_" + task.strType + ".txt"
          var strRSourceContent: String = ""
          var strJarFile: String = ""

          strRSourceContent = fileConnection.readFile(strRSource, true)(0)

          try {
            var restURL: String = ""
            var restMethod: String = "GET"
            var restQueryString: String = ""
            var restResultField: List[String] = List.empty
            var mapResultField: HashMap[Int, String] = HashMap.empty
            var successCommand: String = ""

            //Replace command with arguments and execute
            updateMessageToKafka("=======Start " + task.libMeta.strName + "=======" + EOL, Working)

            //Replace restURL with queryString
            for (curItem: (String, String) <- mapPropField) {
              if (curItem._1.toLowerCase().equals("rest_url")) {
                restURL = curItem._2
              }

              if (curItem._1.toLowerCase().equals("rest_querystring")) {
                restQueryString = curItem._2
              }

              if (curItem._1.toLowerCase().equals("success_war")) {
                successCommand = curItem._2
              }
            }

            //Execute War File will just receive input fields, and property fields in simple types (String, and Number)
            //First line: defined how to execute war file
            //There are some required fields that have to be replaced by orders
            //{classPath}
            //{mainClass}
            //{arg_name_1}, {arg_name_2}, {arg_name_3}...
            //Output of Jar File will be also only in simple types (String, Number)
            for (curItem: (String, String) <- mapPropField) {
              if (curItem._1.toLowerCase().equals("class_path")) {
                strJarFile = curItem._2
              }

              strRSourceContent = strRSourceContent.replace("{" + curItem._1 + "}", curItem._2)

              if (!restQueryString.isEmpty()) {
                restQueryString = restQueryString.replace("{" + curItem._1 + "}", curItem._2)
              }

              if (curItem._1.toLowerCase().contains("result_field_")) {
                restResultField = restResultField :+ curItem._2

                val curResultFieldIdx = curItem._1.toLowerCase().replace("result_field_", "").toInt
                mapResultField = mapResultField + (curResultFieldIdx -> curItem._2)
              }

              if (curItem._1.toLowerCase().equals("rest_method")) {
                restMethod = curItem._2
              }

              writeOutput(strRedisInputKey, curItem._1, curItem._2)
            }

            //Add Input variables to script
            for (curItem: (String, Any) <- arrPrevResult) {

              strRSourceContent = strRSourceContent.replace("{" + curItem._1 + "}", curItem._2.toString)

              writeOutput(strRedisInputKey, curItem._1, curItem._2)
            } //Finished to prepare command, run it with child-process and input/output

            val intCurPort = getPortInURL(restURL)

            updateNewRequestToRedisWar(strJarFile, task, 1)
            val result = checkWarFileNameInRedis(task, strJarFile, intCurPort)
            val bIsSameWar = result._1
            val newPort = result._2

            if (!bIsSameWar) {
              var intNewPort = NetworkIO.assignNewPort()

              if (intNewPort > -1) {

                strRSourceContent = strRSourceContent + " --server.port=" + intNewPort
                restURL = restURL.replaceAll("(:)(\\d+)(\\/)", ":" + intNewPort + "/")

                checkWarFileNameInRedis(task, "", intNewPort)

                createRunShProcess(strRSourceContent, strJarFile, task)
              }
            } else {
              if (newPort > -1) {
                strRSourceContent = strRSourceContent + " --server.port=" + newPort
                restURL = restURL.replaceAll("(:)(\\d+)(\\/)", ":" + newPort + "/")
              }
            }

            var restCheck = callGETRESTApi(restURL, restQueryString, restMethod, restResultField)

            if (numWarRepeat <= 0) {
              numWarRepeat = 50
            }

            if (restCheck._1 == 404) {
              //runJarChildProcess(config.getString("configdir.terminal"), strRSourceContent)
              if (bIsSameWar) {
                createRunShProcess(strRSourceContent, strJarFile, task)
              }

              var numTime: Int = 1

              breakable {
                do {
                  updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Num Time: " + numTime + EOL), Working)
                  updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Rest API Check: " + restCheck._1 + EOL), Working)

                  restCheck = callGETRESTApi(restURL, restQueryString, restMethod, restResultField)

                  Thread.sleep(3000)

                  numTime = numTime + 1

                  if (numTime > numWarRepeat) {
                    break
                  }
                } while (restCheck._1 == 404)
              }
            }

            var mapResult: Map[String, Any] = HashMap.empty

            if (restCheck._1 - 200 < 100 && restCheck._1 >= 200) {
              mapResult = restCheck._2
              bResult = true
            } else {
              bResult = false
            }

            updateNewRequestToRedisWar(strJarFile, task, 2)
            stopOldWar(strJarFile, task)

            updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "=======End " + task.libMeta.strName + "=======" + EOL), Working)
            updateMessageToKafka(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, ""), Finished)

            for (curOutput <- task.libMeta.arrParam.arrLibParam) {
              log.info("CHECK-Output: " + curOutput.iId)

              if (curOutput.paramType == "output") {
                mapResultField.get(curOutput.iId) match {
                  case None =>
                  case Some(jsonFieldName) => {
                    log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "jsonFieldName: " + jsonFieldName + " - iOutIdx: " + curOutput.iId + " - outName: " + curOutput.strName))

                    if (mapResult.contains(jsonFieldName)) {
                      var curConvert: String = mapResult.get(jsonFieldName) match {
                        case Some(x) => x.toString
                        case None => ""
                      }

                      writeOutput(strRedisKey, curOutput.strName, curConvert)
                    }
                  }
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

  def createRunShProcess(cmd: String, fileName: String, task: SendTaskToExecuteNoActorRef) = {
    try {
      var strTempFile = shWarTempShTemplate.replace("{0}", task.iTaskId.toString + "-" + task.objRequest.iRequestId.toString)
      var strProcessCmd = cmd.replace("\r", "").replace("\n", "")
      var objStrBuilder = new StringBuilder();
      objStrBuilder.append(shBashAnnotation).append("\r\n")

      var strCmd = "setsid java " + strProcessCmd + " >| \"" + warLog + "\" &"
      objStrBuilder.append(strCmd)

      var strFileName = shWarShTemplate.replace("{0}", task.iTaskId.toString + "-" + task.objRequest.iRequestId.toString)

      val objFile = new File(strFileName)
      fileConnection.writeFile(objStrBuilder.toString, strFileName)

      if (new File(strFileName).exists()) {
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Exist: " + strFileName))
        val objThread = new Thread(new Runnable {
          override def run(): Unit = {
            var strCmd = shShPath + " " + strFileName
            var objRuntime = Runtime.getRuntime
            objRuntime.exec(strCmd.split(" "))
          }
        })

        objThread.start()
      } else {
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Not Exist: " + strFileName))
      }
    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))
      }
    }
  }

  def runJarChildProcess(terminalCmd: String, cmd: String) = {
    var mapResult: HashMap[String, Any] = HashMap.empty
    var isStarted: Boolean = false
    var objRuntime: Runtime = Runtime.getRuntime()
    var arrCmd: Array[String] = Array.empty;

    try {
      var strCmd = "setsid java " + cmd + " >| \"/tmp/war_exec\" &"
      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Cmd: " + strCmd))

      updateMessageToKafka("Cmd: " + strCmd + EOL, Working)

      var objThread = new Thread(new Runnable() {
        var curCmd = strCmd

        def run() {
          try {
            var objRuntime = Runtime.getRuntime
            objRuntime.exec(curCmd)
          } catch {
            case ex: Throwable => {
              log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))
            }
          }
        }
      });

      objThread.start();
    } catch {
      case ex: Throwable => log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))
    }
  }

  def callGETRESTApi(restURL: String, queryString: String, method: String, resultField: List[String]): (Int, HashMap[String, Any]) = {
    var responeStatus: Int = 404
    var result: HashMap[String, Any] = HashMap.empty

    try {
      val httpClient = new DefaultHttpClient()
      var strURL = restURL;

      if (strURL.charAt(strURL.length - 1) == '/') {
        strURL = strURL.substring(0, strURL.length - 1)
      }

      var strQueryStr = queryString;

      if (!strQueryStr.isEmpty() && strQueryStr.charAt(0) == '/') {
        strQueryStr = strQueryStr.substring(1)
      }

      var strCallableURL = strURL + "/" + URLEncoder.encode(strQueryStr, "UTF-8").replaceAll("\\+", "%20")

      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "CallURL: " + strURL))
      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "QueryStr: " + strQueryStr))

      var httpResponse: CloseableHttpResponse = null

      if (method.equals("POST")) {
        httpResponse = httpClient.execute(new HttpPost(strCallableURL))
      } else if (method.equals("GET")) {
        httpResponse = httpClient.execute(new HttpGet(strCallableURL))
      }

      val entity = httpResponse.getEntity()
      responeStatus = httpResponse.getStatusLine.getStatusCode
      var content = ""

      if (entity != null) {
        val inputStream = entity.getContent()
        content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close
      }

      httpClient.getConnectionManager().shutdown()

      log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "Content: " + content))

      for (curField <- resultField) {
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "WARJSONField: " + curField))
        val strFieldResult: String = JsonPath.parse(content).read[String](curField)
        log.info(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Working, "WARJSONField - " + curField + ": " + strFieldResult))

        result = result + (curField -> strFieldResult)
      }

    } catch {
      case ex: Throwable => {
        log.error(loggingIO.generateLogMesageTaskExecuteNoActor(objTaskToExecute, Failed, ex.getStackTraceString))
      }
    }

    (responeStatus, result)
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

          var curResult = objRedisKeyLib.getContentOfKey(0, 0, curPrevOutput, curKey._1, redisKeyTemplate)

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
