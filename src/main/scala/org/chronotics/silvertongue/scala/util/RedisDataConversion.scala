package org.chronotics.silvertongue.scala.util

import jep.Jep
import org.chronotics.pandora.java.log.LoggerFactory
import org.chronotics.pithos.ext.redis.scala.adaptor.RedisPoolConnection
import org.rosuda.REngine.{REXPLogical, REngine}

class RedisDataConversion(redisCli: RedisPoolConnection, rEngine: REngine, pythonEngine: Jep, pythonEnv: String,
                          redisIP: String, redisPort: Int, redisDB: Int, redisPass: String,
                          checkKeyOnly: Boolean = false) {
  val log = LoggerFactory.getLogger(getClass)

  def checkLangEngine(): Boolean = {
    true
  }

  def convertFromRToPython(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    try {
      val pyStr: StringBuilder = StringBuilder.newBuilder
      pyStr.append("rString = \"function() { library(rredis);redisConnect('" + redisIP + "', " + redisPort + ", '" + redisPass + "');redisSelect(" + redisDB + ");")
      pyStr.append(paramInput + " <- redisHGet('" + redisKey + "', '" + redisField + "');")

      paramInputParam.isEmpty() match {
        case false => pyStr.append(paramInput + "$" + paramInputParam + " }\"")
        case true => pyStr.append(paramInput + " }\"")
      }

      log.info("pyStr " + pyStr.toString)

      pythonEngine.eval(pyStr.toString)

      pythonEngine.eval("rfunc = robjects.r(rString)")
      pythonEngine.eval("rdata = rfunc()")

      paramInputParam.isEmpty() match {
        case false => {
          pythonEngine.eval(paramInputParam + " = pandas2ri.ri2py(rdata)")
          pythonEngine.getValue(paramInputParam)
        }
        case true => {
          pythonEngine.eval(paramInput + " = pandas2ri.ri2py(rdata)")
          pythonEngine.getValue(paramInput)
        }
      }
    } catch {
      case ex: Throwable => {
        log.info("ERR " + ex.getMessage + " - cause: " + ex.getCause + " - stackstrace: " + ex.getStackTraceString)
        log.error("ERR", ex)

        if (paramInputParam != null && !paramInputParam.isEmpty()) {
          try {
            val pyStr: StringBuilder = StringBuilder.newBuilder
            pyStr.append("rString = \"function() { library(rredis);redisConnect('" + redisIP + "', " + redisPort + ", '" + redisPass + "');redisSelect(" + redisDB + ");")
            pyStr.append(paramInputParam + " <- redisHGet('" + redisKey + "', '" + redisField + "');" + paramInputParam + " }\"")

            log.info("pyStr2 " + pyStr.toString)

            pythonEngine.eval(pyStr.toString)

            pythonEngine.eval("rfunc = robjects.r(rString)")
            pythonEngine.eval("rdata = rfunc()")
            pythonEngine.eval(paramInputParam + " = pandas2ri.ri2py(rdata)")

            pythonEngine.getValue(paramInputParam)
          } catch {
            case ex2: Throwable => {
              log.info("ERR2 " + ex2.getMessage + " - cause: " + ex2.getCause + " - stackstrace: " + ex2.getStackTraceString)
              log.error("ERR", ex2)
              null
            }
          }
        } else {
          null
        }
      }
    }
  }

  def convertFromRToR(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    var bIsExist: Boolean = false

    try {
      //Check if null
      log.info("readRedis: " + redisKey + " - " + redisField + " - " + paramInput)

      rEngine.parseAndEval("z2_check_field_redis <- redisHExists(\"" + redisKey + "\", \"" + redisField + "\")")
      var objExist = rEngine.parseAndEval("z2_check_field_redis")

      log.info("objExist: " + objExist.toDebugString())

      if (objExist.isLogical()) {
        log.info("objExist: isLogical")
        var objLogical = objExist.asInstanceOf[REXPLogical]

        log.info("objExist: " + objLogical)

        if (objLogical.isTRUE().length > 0 && objLogical.isTRUE()(0)) {
          log.info("has Key: TRUE")

          if (!checkKeyOnly) {
            rEngine.parseAndEval(paramInput + " <- redisHGet(\"" + redisKey + "\", \"" + redisField + "\")")
          }

          bIsExist = true
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR", "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString)
      }
    }

    bIsExist
  }

  def convertFromRToNative(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    readJSONFromRedis(redisKey, redisField, paramInput, paramInputParam)
  }

  def convertFromPythonToR(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
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

      cmd = "pyConnect <- pyRedis$Client(host=\"" + redisIP + "\", port=as.integer(" + redisPort + "), database=as.integer(" + redisDB + "), password=\"" + redisPass + "\")"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyResult <- pyConnect$hget(\"" + redisKey + "\", \"" + redisField + "\")"
      cmdBuilder.append(cmd).append(";")

      cmd = "pyResult <- pyYAML$load(pyResult)"
      cmdBuilder.append(cmd).append(";")

      cmd = paramInput + " <- tryCatch({ py_to_r(pyResult) }, error = function(e) { pyResult })"
      cmdBuilder.append(cmd)

      log.info("builder: " + cmdBuilder.toString())
      rEngine.parseAndEval(cmdBuilder.toString())
    } catch {
      case ex: Throwable => {
        log.info("ERR: " + ex.getMessage + " - caused by: " + ex.getCause + " - stacktrace: " + ex.getStackTraceString)
      }
    }
  }

  def convertFromPythonToPython(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    try {
      var strCmd: String = paramInput + " = redisConn.hget('" + redisKey + "', '" + redisField + "')"
      pythonEngine.eval(strCmd)

      strCmd = paramInput + " = yaml.load(" + paramInput + ")"
      pythonEngine.eval(strCmd)

      pythonEngine.getValue(paramInput)
    } catch {
      case ex: Throwable => {
        log.info("ERR: " + ex.getMessage + " - caused by: " + ex.getCause + " - stacktrace: " + ex.getStackTraceString)
        null
      }
    }
  }

  def convertFromPythonToNative(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    readJSONFromRedis(redisKey, redisField, paramInput, paramInputParam)
  }

  def convertFromNativeToR(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    try {
      log.info("convertFromNativeToR: " + redisKey + " - " + redisField + " - " + paramInput + " - " + paramInputParam)

      rEngine.parseAndEval(paramInput + " <- redisHGet(\"" + redisKey + "\", \"" + redisField + "\")")

      try {
        rEngine.parseAndEval(paramInput + " <- fromJSON(" + paramInput + ")")
      } catch {
        case ex: Throwable => {
          rEngine.parseAndEval(paramInput + " <- " + paramInput)
        }
      }

      rEngine.parseAndEval("print(" + paramInput + ")")
    } catch {
      case ex: Throwable => {
        log.error("ERR", "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString)
      }
    }
  }

  def convertFromNativeToPython(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    try {
      log.info("redisKey: " + redisKey + ", redisField: " + redisField + ", paramInput: " + paramInput)

      var strCmd: String = paramInput + "_str = redisConn.hget('" + redisKey + "', '" + redisField + "')"
      pythonEngine.eval(strCmd)

      try {
        strCmd = paramInput + " = json.loads(" + paramInput + "_str)"
        pythonEngine.eval(strCmd)

        pythonEngine.getValue(paramInput)
      } catch {
        case ex: Throwable => {
          strCmd = paramInput + " = " + paramInput + "_str"
          pythonEngine.eval(strCmd)

          pythonEngine.getValue(paramInput)
        }
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR", "ERR: " + ex.getMessage + " - Cause: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString)
      }
    }
  }

  def convertFromNativeToNative(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    readJSONFromRedis(redisKey, redisField, paramInput, paramInputParam)
  }

  def readJSONFromRedis(redisKey: String, redisField: String, paramInput: String, paramInputParam: String): String = {
    var strRedisValue: String = ""

    try {
      var redisValue = redisCli.hGet(redisKey, redisField)

      if (redisValue != null) {
        strRedisValue = redisValue.toString
      }
    } catch {
      case ex: Throwable => {
        log.error("ERR", ex.getMessage + " - Caused by: " + ex.getCause + " - StackTrace: " + ex.getStackTraceString)
      }
    }

    strRedisValue
  }

  def convertJSONToObject(fromLang: String, toLang: String, redisKey: String, redisField: String, paramInput: String, paramInputParam: String): Any = {
    toLang match {
      case "R" => fromLang match {
        case "R" => convertFromRToR(redisKey, redisField, paramInput, paramInputParam)
        case "Python" => convertFromPythonToR(redisKey, redisField, paramInput, paramInputParam)
        case _ => convertFromNativeToR(redisKey, redisField, paramInput, paramInputParam)
      }
      case "Python" => fromLang match {
        case "R" => convertFromRToPython(redisKey, redisField, paramInput, paramInputParam)
        case "Python" => convertFromPythonToPython(redisKey, redisField, paramInput, paramInputParam)
        case _ => convertFromNativeToPython(redisKey, redisField, paramInput, paramInputParam)
      }
      case "Java" => fromLang match {
        case "R" => convertFromRToNative(redisKey, redisField, paramInput, paramInputParam)
        case "Python" => convertFromPythonToNative(redisKey, redisField, paramInput, paramInputParam)
        case _ => convertFromNativeToNative(redisKey, redisField, paramInput, paramInputParam)
      }
      case "C++" => fromLang match {
        case "R" => convertFromRToNative(redisKey, redisField, paramInput, paramInputParam)
        case "Python" => convertFromPythonToNative(redisKey, redisField, paramInput, paramInputParam)
        case _ => convertFromNativeToNative(redisKey, redisField, paramInput, paramInputParam)
      }
      case "Jar" => fromLang match {
        case "R" => convertFromRToNative(redisKey, redisField, paramInput, paramInputParam)
        case "Python" => convertFromPythonToNative(redisKey, redisField, paramInput, paramInputParam)
        case _ => convertFromNativeToNative(redisKey, redisField, paramInput, paramInputParam)
      }
      case "War" => fromLang match {
        case "R" => convertFromRToNative(redisKey, redisField, paramInput, paramInputParam)
        case "Python" => convertFromPythonToNative(redisKey, redisField, paramInput, paramInputParam)
        case _ => convertFromNativeToNative(redisKey, redisField, paramInput, paramInputParam)
      }
    }
  }
}
