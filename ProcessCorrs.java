package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;

public class ProcessCorrs {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
        	String currentBaseDt = dao.getStringValue("baseDt");
            String currentDataIds = dao.getStringValue("dataIds");
            
            //1. baseDt가 null인 경우
            if (currentBaseDt == null || currentBaseDt.trim().isEmpty()) {
                throw new IllegalArgumentException("baseDt가 null이나 empty이다.");
            }
        	
        	//2. dataIds가 null인 경우
            if (currentDataIds == null || currentDataIds.trim().isEmpty()) {
                throw new IllegalArgumentException("dataIds가 null이나 empty이다.");
            }
            // SQL query문 실행
            dao.sqlexe("s_selCorrelation", false); // Use the SQL query ID for prices
            ListParam result = dao.getNowListParam(); // Get the result of the query

            // result가 없는 경우를 확인한다.
            if (result.rowSize() == 0) {
                JSONObject errorJson = new JSONObject();
                errorJson.put("error", "URL에 필요한 parameter를 제대로 입력하지 않았습니다.");
                dao.setValue("response", errorJson.toMap());
                log.info(errorJson.toString());
                throw new IllegalArgumentException("dataIds나 baseDt를 제대로 입력하지 않았습니다.");
            }

            // JSON구조를 초기화 한다.
            JSONObject finalJson = new JSONObject();
            JSONArray pricesArray = new JSONArray();

            // 결과에서 각 row를 처리한다.
            for (int i = 0; i < result.rowSize(); i++) {
                JSONObject priceObject = new JSONObject();
                priceObject.put("baseDt", result.getValue(i, "baseDt", ""));
                priceObject.put("dataSetId", result.getValue(i, "dataSetId", ""));
                priceObject.put("matrixId", result.getValue(i, "matrixId", ""));
                priceObject.put("dataId1", result.getValue(i, "dataId1", ""));
                priceObject.put("dataId2", result.getValue(i, "dataId2", ""));
                //priceObject.put("corr", result.getValue(i, "corr", ""));
                
                String corrStr = result.getValue(i, "corr", "");
                double corrNum = Double.parseDouble(corrStr);  // string을 double로 바꾼다.
                priceObject.put("corr", corrNum);  
                pricesArray.put(priceObject);
            }

            finalJson.put("correlations", pricesArray);
            Map<String, Object> hashMap = finalJson.toMap();
            dao.setValue("response", hashMap);
            log.info("HashMap contents: " + hashMap.toString());

        } catch (SQLServiceException e) {
            log.error("Error processing prices", e);
            dao.setError("Error processing prices: " + e.getMessage());
        } catch (IllegalArgumentException e) {//IllegalArgumentException을 사용할 경우, 특정 error 메시지를 띄울 수 있다.
			log.error("Validation error: " + e.getMessage(), e);
			dao.setError(e.getMessage());
		}
    }
}
