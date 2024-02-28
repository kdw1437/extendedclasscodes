package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;
public class ProcessYieldCurves {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
            // SQL query 실행
            dao.sqlexe("s_selectYieldCurves3", false); // sqlquery ID
            ListParam result = dao.getNowListParam(); // query의 결과를 받는다.
            
            if (result.rowSize() == 0) {
                JSONObject errorJson = new JSONObject();
                errorJson.put("error", "URL에 필요한 parameter를 제대로 입력하지 않았습니다.");
                dao.setValue("response", errorJson.toMap()); //errorJson을 HashMap객체로 바꿔준 뒤에, response로 설정한다.
                log.info(errorJson.toString()); // error message 로그를 console에 남긴다.
                return; // execute 메소드를 빠져 나간다.
            }
            // JSONObject를 초기화 한다.
            JSONObject finalJson = new JSONObject();
            JSONArray yieldsArray = new JSONArray();

            // result의 각 열을 iterate한다.
            for (int i = 0; i < result.rowSize(); i++) {
                String baseDt = result.getValue(i, "baseDt", "");
                String dataSetId = result.getValue(i, "dataSetId", "");
                String dataId = result.getValue(i, "dataId", "");
                String currency = result.getValue(i, "currency", "");

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
                    newDataIdObject.put("currency", currency);

                    JSONArray newYieldsArray = new JSONArray();
                    newYieldsArray.put(yieldObject);
                    newDataIdObject.put("yields", newYieldsArray);

                    yieldsArray.put(newDataIdObject);
                }
            }

            finalJson.put("yieldCurves", yieldsArray);
            Map<String, Object> hashMap = finalJson.toMap();
            String jsonResponse = finalJson.toString().replace("\\", "");
            dao.setValue("response", hashMap);
            log.info("HashMap contents: " + hashMap.toString());
            //dao.setValue("response", finalJson.toString());
            log.info(finalJson.toString());

        } catch (SQLServiceException e) {
            log.error("Error processing yield curves", e);
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
