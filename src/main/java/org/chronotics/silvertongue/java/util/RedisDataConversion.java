package org.chronotics.silvertongue.java.util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.chronotics.pandora.java.exception.ExceptionUtil;
import org.chronotics.pandora.java.log.Logger;
import org.chronotics.pandora.java.log.LoggerFactory;
import org.chronotics.pithos.ext.redis.java.adaptor.RedisConnection;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class RedisDataConversion {
    private Logger objLogger = LoggerFactory.getLogger(getClass());
    RedisConnection objRedisConnection;
    String strKeyResultTemplate;
    Gson gson = new Gson();

    public RedisDataConversion(String strRedisHost, Integer intRedisPort, String strRedisPass, Integer intRedisDB,
                       String strKeyResultTemplate) {
        objRedisConnection = RedisConnection.getInstance(strRedisHost, intRedisPort, strRedisPass, intRedisDB, 10000);
        this.strKeyResultTemplate = strKeyResultTemplate;

        if (this.strKeyResultTemplate.isEmpty()) {
            this.strKeyResultTemplate = "task_result_";
        }
    }

    public String getStrContentOfKey(Integer intRequestID, Integer intTaskID, String strPrevOutput, String strKey, String strJSONKey) {
        return getContentOfKey(intRequestID, intTaskID, strPrevOutput, strKey, strJSONKey);
    }

    public Object getContentObjectOfKey(Integer intRequestID, Integer intTaskID, String strPrevOutput, String strKey, String strJSONKey) {
        String strContent = getContentOfKey(intRequestID, intTaskID, strPrevOutput, strKey, strJSONKey);

        //objLogger.info("strContent: " + strContent);

        if (strContent != null && !strContent.isEmpty()) {
            // try with numeric
            Double dbResult = convertJSONToNumeric(strContent);

            if (dbResult == null) {
                // try with string
                String strResult = convertJSONToString(strContent);

                if (strResult == null || strResult.isEmpty()) {
                    Double[] arrNumResult = convertJSONToArrayNumeric(strContent);

                    if (arrNumResult == null || arrNumResult.length <= 0) {
                        String[] arrStrResult = convertJSONToArrayString(strContent);

                        if (arrStrResult == null || arrStrResult.length <= 0) {
                            Double[][] matrixNumResult = convertJSONToMatrixNumeric(strContent);

                            if (matrixNumResult == null || matrixNumResult.length <= 0) {
                                String[][] matrixStrResult = convertJSONToMatrixString(strContent);

                                if (matrixStrResult == null || matrixStrResult.length <= 0) {
                                    Map<String, Object> mapResult = convertJSONToMap(strContent);

                                    if (mapResult == null || mapResult.size() <= 0) {
                                        Map<String, Object>[] arrMapResult = convertJSONToListMap(strContent);

                                        return arrMapResult;
                                    } else {
                                        return mapResult;
                                    }
                                } else {
                                    return matrixStrResult;
                                }
                            } else {
                                return matrixNumResult;
                            }
                        } else {
                            return arrStrResult;
                        }
                    } else {
                        return arrNumResult;
                    }
                } else {
                    return strResult;
                }
            } else {
                return dbResult;
            }
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Boolean convertObjectToJSONString(Integer intRequestID, Integer intTaskID, String strCurOutput, Object objObj) {
        Boolean bIsConverted = false;
        String strJSONResult = "";

        if (objObj instanceof String) {
            strJSONResult = convertStringToJSON(objObj.toString());
        } else if (objObj instanceof Double) {
            strJSONResult = convertNumericToJSON((Double) objObj);
        } else if (objObj instanceof List<?>) {
            strJSONResult = convertArrayNumericToJSON((List<Double>) objObj);

            if (strJSONResult == null || strJSONResult.isEmpty()) {
                strJSONResult = convertArrayStringToJSON((String[]) objObj);
            }
        } else if (objObj instanceof String[][]) {
            strJSONResult = convertMatrixStringToJSON((String[][]) objObj);
        } else if (objObj instanceof Double[][]) {
            strJSONResult = convertMatrixNumericToJSON((Double[][]) objObj);
        } else if (objObj instanceof Map<?, ?>) {
            strJSONResult = convertMapToJSON((Map<String, Object>) objObj);
        } else if (objObj instanceof Map<?, ?>[]) {
            strJSONResult = convertListMapToJSON((Map<String, Object>[]) objObj);
        }

        if (strJSONResult != null && !strJSONResult.isEmpty()) {
            String strKey = strKeyResultTemplate + intRequestID + "_" + intTaskID;

            objRedisConnection.hSetField(strKey, strCurOutput, strJSONResult, -1);

            bIsConverted = true;
        }

        return bIsConverted;
    }

    public String getContentOfKey(Integer intRequestID, Integer intTaskID, String strPrevOutput, String strKey, String strJSONKey) {
        String strJSONResult = "";

        objLogger.info("getContentOfKey - strKey: " + strKey);
        objLogger.info("getContentOfKey - strJSONKey: " + strJSONKey);

        try {
            if (objRedisConnection != null) {
                String strResultKey = strJSONKey.isEmpty() ? (strKeyResultTemplate + intRequestID + "_" + intTaskID + "_json") : strJSONKey;
                String strContent = objRedisConnection.hGetByField(strResultKey, strPrevOutput);

                if (strContent != null && !strContent.isEmpty()) {
                    strJSONResult = strContent;
                } else {
                    strResultKey = strKey.isEmpty() ? (strKeyResultTemplate + intRequestID + "_" + intTaskID) : strKey;
                    strContent = objRedisConnection.hGetByField(strResultKey, strPrevOutput);

                    if (strContent != null && !strContent.isEmpty()) {
                        strJSONResult = strContent;
                    }
                }
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSONResult;
    }

    @SuppressWarnings("unchecked")
    private String convertJSONToString(String strJSON) {
        String strResult = "";

        try {
            List<String> lstString = gson.fromJson(strJSON, List.class);

            if (lstString != null && lstString.size() == 1) {
                strResult = lstString.get(0);
            }
        } catch (Exception objEx) {
            strResult = null;
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strResult;
    }

    private String convertStringToJSON(String strObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(strObject, String.class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    @SuppressWarnings("unchecked")
    private String[] convertJSONToArrayString(String strJSON) {
        try {
            List<String> lstString = gson.fromJson(strJSON, List.class);

            if (lstString != null && lstString.size() == 1) {
                return (String[]) lstString.toArray();
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return null;
    }

    private String convertArrayStringToJSON(String[] arrObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(arrObject, String[].class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    private String[][] convertJSONToMatrixString(String strJSON) {
        try {
            String[][] lstString = gson.fromJson(strJSON, String[][].class);

            return lstString;
        } catch (Exception objEx) {
            objLogger.error("ERR-convertJSONToMatrixString", objEx);
        }

        return null;
    }

    private String convertMatrixStringToJSON(String[][] arrObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(arrObject, String[][].class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    @SuppressWarnings({ "serial" })
    private Map<String, Object> convertJSONToMap(String strJSON) {
        try {
            Type objType = new TypeToken<Map<String, Object>>() {
            }.getType();
            Map<String, Object> objObj = gson.fromJson(strJSON, objType);

            return objObj;
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return null;
    }

    private String convertMapToJSON(Map<String, Object> mapObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(mapObject, Map.class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    @SuppressWarnings({ "serial" })
    private Map<String, Object>[] convertJSONToListMap(String strJSON) {
        try {
            Type objType = new TypeToken<Map<String, Object>[]>() {
            }.getType();
            Map<String, Object>[] objObj = gson.fromJson(strJSON, objType);

            return objObj;
        } catch (Exception objEx) {
            objLogger.error("ERR-convertJSONToListMap", objEx);
        }

        return null;
    }

    private String convertListMapToJSON(Map<String, Object>[] mapObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(mapObject, Map[].class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    @SuppressWarnings("unchecked")
    private Double convertJSONToNumeric(String strJSON) {
        Double dbResult = 0.0;

        try {
            List<Double> lstString = gson.fromJson(strJSON, List.class);

            if (lstString != null && lstString.size() == 1) {
                dbResult = lstString.get(0);
            }
        } catch (Exception objEx) {
            dbResult = null;
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        objLogger.info("dbResult: " + dbResult);

        return dbResult;
    }

    private String convertNumericToJSON(Double dbObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(dbObject, Double.class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    @SuppressWarnings("unchecked")
    private Double[] convertJSONToArrayNumeric(String strJSON) {
        try {
            List<Double> lstDouble = gson.fromJson(strJSON, List.class);

            if (lstDouble != null && lstDouble.size() == 1) {
                return (Double[]) lstDouble.toArray();
            }
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return null;
    }

    private String convertArrayNumericToJSON(List<Double> arrObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(arrObject, List.class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }

    private Double[][] convertJSONToMatrixNumeric(String strJSON) {
        try {
            Double[][] lstNumeric = gson.fromJson(strJSON, Double[][].class);

            return lstNumeric;
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return null;
    }

    private String convertMatrixNumericToJSON(Double[][] arrObject) {
        String strJSON = "";

        try {
            strJSON = gson.toJson(arrObject, Double[][].class);
        } catch (Exception objEx) {
            objLogger.error("ERR: " + ExceptionUtil.getStrackTrace(objEx));
        }

        return strJSON;
    }
}
