package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;

public class ProcessDividends {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
            // SQL query를 실핸한다.
            dao.sqlexe("s_selectDividends_v1", false); // 가격에 대한 sqlqueryId를 사용
            ListParam result = dao.getNowListParam(); // query의 결과를 얻는다.

            // 결과 없음을 확인한다.
            if (result.rowSize() == 0) {
                JSONObject errorJson = new JSONObject();
                errorJson.put("error", "URL에 필요한 parameter를 제대로 입력하지 않았습니다.");
                dao.setValue("response", errorJson.toMap());
                log.info(errorJson.toString());
                return;
            }

            // JSON구조를 초기화한다.
            JSONObject finalJson = new JSONObject();
            JSONArray pricesArray = new JSONArray();

            // 결과에서 각 row를 처리한다.
            for (int i = 0; i < result.rowSize(); i++) {
                JSONObject priceObject = new JSONObject();
                priceObject.put("baseDt", result.getValue(i, "baseDt", ""));
                priceObject.put("dataSetId", result.getValue(i, "dataSetId", ""));
                priceObject.put("dataId", result.getValue(i, "dataId", ""));
                //priceObject.put("yield", result.getValue(i, "yield", ""));
                String yieldStr = result.getValue(i, "yield", "");
                double yieldNum = Double.parseDouble(yieldStr);  // string을 double로 바꾼다.
                priceObject.put("yield", yieldNum);  // Automatically uses numeric JSON representation


                pricesArray.put(priceObject);
            }

            finalJson.put("dividendYields", pricesArray);
            Map<String, Object> hashMap = finalJson.toMap();
            dao.setValue("response", hashMap);
            log.info("HashMap contents: " + hashMap.toString());

        } catch (SQLServiceException e) {
            log.error("Error processing prices", e);
            dao.setError("Error processing prices: " + e.getMessage());
        }
    }
}
