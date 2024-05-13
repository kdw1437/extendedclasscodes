package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.HashMap;
import java.util.Map;

public class ProcessVols {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
            dao.sqlexe("s_selectVolatilities_v1", false);
            ListParam result = dao.getNowListParam();

            //1. data validation
        	String forValidBaseDt = dao.getStringValue("baseDt");
            String forValidDataIds = dao.getStringValue("dataIds");
            
            //1.1. baseDt가 null인 경우
            if (forValidBaseDt == null || forValidBaseDt.trim().isEmpty()) {
                throw new IllegalArgumentException("baseDt가 null이나 empty이다.");
            }
        	
        	//1.2. dataIds가 null인 경우
            if (forValidDataIds == null || forValidDataIds.trim().isEmpty()) {
                throw new IllegalArgumentException("dataIds가 null이나 empty이다.");
            }
            
            //validation
            if (result.rowSize() == 0) {
            	throw new IllegalArgumentException("No data");
            }
            //JSONObject 객체, JSONArray 객체, HashMap객체를 만들어준다. (HashMap객체는 JSONObject와 유사하다고 보면 된다.)
            JSONObject finalJson = new JSONObject();
            JSONArray volCurvesArray = new JSONArray();
            Map<String, JSONObject> dataIdMap = new HashMap<>();

            for (int i = 0; i < result.rowSize(); i++) {
                String baseDt = result.getValue(i, "baseDt", "");
                String dataSetId = result.getValue(i, "dataSetId", "");
                String dataId = result.getValue(i, "dataId", "");
                //String volFactor = result.getValue(i, "volCurves.volFactor", "");
                String volFactorStr = result.getValue(i, "volCurves.volFactor", "");
                double volFactorNum = Double.parseDouble(volFactorStr);  // string을 double로 바꾼다.
                 
                //JSONObject 객체 termVolObject 선언
                JSONObject termVolObject = new JSONObject();
                //termVolObject.put("tenor", result.getValue(i, "volCurves.termVols.tenor", ""));
                String tenorStr = result.getValue(i, "volCurves.termVols.tenor", "");
                double tenorNum = Double.parseDouble(tenorStr);  // string을 double로 바꾼다.
                termVolObject.put("tenor", tenorNum);
                //termVolObject에 tenor key, value tenorNume 넣어준다.     
                //termVolObject.put("vol", result.getValue(i, "volCurves.termVols.vol", ""));
                String volStr = result.getValue(i, "volCurves.termVols.vol", "");
                double volNum = Double.parseDouble(volStr);  // string을 double로 바꾼다.
                termVolObject.put("vol", volNum);
                //termVolObject에 vol key, volNum value를 넣어준다.
                JSONObject volCurveObject = new JSONObject();
                volCurveObject.put("volFactor", volFactorNum);
                JSONArray termVolsArray = new JSONArray();
                termVolsArray.put(termVolObject);
                volCurveObject.put("termVols", termVolsArray);
                //dataIdMap이 dataId key를 가지고 있는지 확인한다. dataId가 없으면, 새로운 JSONObject 객체를 생성한다.
                if (!dataIdMap.containsKey(dataId)) {
                    JSONObject newDataIdObject = new JSONObject();
                    newDataIdObject.put("baseDt", baseDt);
                    newDataIdObject.put("dataSetId", dataSetId);
                    newDataIdObject.put("dataId", dataId);

                    JSONArray newVolCurvesArray = new JSONArray();
                    newVolCurvesArray.put(volCurveObject);
                    newDataIdObject.put("volCurves", newVolCurvesArray);

                    volCurvesArray.put(newDataIdObject);
                    dataIdMap.put(dataId, newDataIdObject);
                } else {
                    JSONObject existingDataIdObject = dataIdMap.get(dataId);
                    JSONArray existingVolCurvesArray = existingDataIdObject.getJSONArray("volCurves");

                    // 같은 volFactor를 가지는 voluCurve가 있는지 확인
                    boolean volCurveExists = false;
                    for (int j = 0; j < existingVolCurvesArray.length(); j++) {
                        JSONObject existingVolCurve = existingVolCurvesArray.getJSONObject(j);
                        
                        //if (existingVolCurve.getDouble("volFactor").equals(volFactorNum)) {
                        double epsilon = 1E-9;
                        double existingVolFactor = existingVolCurve.getDouble("volFactor");
                        if (Math.abs(existingVolFactor - volFactorNum) < epsilon) {
                            existingVolCurve.getJSONArray("termVols").put(termVolObject);
                            volCurveExists = true;
                            break;
                        }
                    }

                    // If volCurve with the same volFactor doesn't exist, add a new one
                    if (!volCurveExists) {
                        existingVolCurvesArray.put(volCurveObject);
                    }
                }
            }

            finalJson.put("volatilities", volCurvesArray);
            dao.setValue("response", finalJson.toMap());
            log.debug(finalJson.toString());

        } catch (SQLServiceException e) {
            log.error("Error processing volatility curves", e);
            dao.setError("Error processing volatility curves: " + e.getMessage());
        }  catch (IllegalArgumentException e) {
    		dao.setError(e.getMessage());
    	}
    }
}
