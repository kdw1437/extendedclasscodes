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
            // SQL query문 실행
            dao.sqlexe("s_selCorrelation", false); // Use the SQL query ID for prices
            ListParam result = dao.getNowListParam(); // Get the result of the query

            // result가 없는 경우를 확인한다.
            if (result.rowSize() == 0) {
                JSONObject errorJson = new JSONObject();
                errorJson.put("error", "URL에 필요한 parameter를 제대로 입력하지 않았습니다.");
                dao.setValue("response", errorJson.toMap());
                log.info(errorJson.toString());
                return;
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
        }
    }
}
