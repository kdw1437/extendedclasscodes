package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;

public class ProcessForwardFXCurves {
	
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
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
            
            //1.3. 결과물에 해당하는 내용이 없을 경우 (ListParam객체의 Row Size가 0인 경우)
            // SQL query 실행
            dao.sqlexe("s_selectForwardFXcurves_v1", false); // sqlquery ID
            ListParam result = dao.getNowListParam(); // query의 결과를 받는다.
            
            if (result.rowSize() == 0) {
                throw new IllegalArgumentException("dataIds나 baseDt를 제대로 입력하지 않았습니다. 혹은 관련 데이터가 없습니다.");        
            }
            
            // JSONObject를 초기화 한다. JSON 객체를 API설계서에 따라서 만들어 주어야 한다.
            JSONObject finalJson = new JSONObject();
            JSONArray yieldsArray = new JSONArray();
            
            for (int i = 0; i < result.rowSize(); i++) {
            	String baseDt = result.getValue(i, "baseDt", "");
            	String dataSetId = result.getValue(i, "dataSetId", "");
            	String dataId = result.getValue(i, "dataId", "");
            	
            	JSONObject yieldObject = new JSONObject();
                String tenorStr = result.getValue(i, "yields.tenor", "");
                double tenorNum = Double.parseDouble(tenorStr);  // Converts string to double
                yieldObject.put("tenor", tenorNum);  // Automatically uses numeric JSON representation
                //yieldObject.put("tenor", result.getValue(i, "yields.tenor", ""));
                
                String rateStr = result.getValue(i, "yields.rate", "");
                double rateNum = Double.parseDouble(rateStr);  // Converts string to double
                yieldObject.put("rate", rateNum);
                //yieldObject.put("rate", result.getValue(i, "yields.rate", ""));

                // 새로운 dataId 인지 있는 것인지 확인한다.
                JSONObject existingDataIdObject = findInArray(yieldsArray, dataId);
                if (existingDataIdObject != null) {
                    // Existing dataId, yields array에 추가
                    existingDataIdObject.getJSONArray("yields").put(yieldObject);
                } else {
                    // 새로운 dataId, 새로운 entry 만들기
                    JSONObject newDataIdObject = new JSONObject();
                    newDataIdObject.put("baseDt", baseDt);
                    newDataIdObject.put("dataSetId", dataSetId);
                    newDataIdObject.put("dataId", dataId);
                    

                    JSONArray newYieldsArray = new JSONArray();
                    newYieldsArray.put(yieldObject);
                    newDataIdObject.put("yields", newYieldsArray);

                    yieldsArray.put(newDataIdObject);
                }
            }
            
            finalJson.put("yieldCurves", yieldsArray);
            Map<String, Object> hashMap = finalJson.toMap();
            dao.setValue("response", hashMap);
            

        	} catch (Exception e) {
                log.error("Error processing yield curves", e);
                //DaoService의 setError메소드를 사용해야지 Error가 client로 전송된다.
                dao.setError("Error processing yield curves: " + e.getMessage());
        	}
    }
    
    private JSONObject findInArray(JSONArray array, String dataId) {
        for (int i = 0; i < array.length(); i++) {
            JSONObject obj = array.getJSONObject(i);
            if (obj.getString("dataId").equals(dataId)) {
                return obj;
            }
        }
        return null;
    }
}