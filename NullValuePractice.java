package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;

public class NullValuePractice {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
            // SQL query를 실핸한다.
            dao.sqlexe("s_selectOTCLEGMSTR_practice", false); // 가격에 대한 sqlqueryId를 사용
            ListParam result = dao.getNowListParam(); // query의 결과를 얻는다.
            
         // JSON구조를 초기화한다.
            JSONObject finalJson = new JSONObject();
            JSONArray LegsArray = new JSONArray();
            
            for (int i = 0; i < result.rowSize(); i++) {
            	JSONObject legObject = new JSONObject();
            	legObject.put("gdsId", result.getValue(i, "gdsId", ""));
            	legObject.put("legNo", result.getValue(i, "legNo", ""));
            	legObject.put("payRecvTp", transformEmptyStringToNull(result.getValue(i, "payRecvTp", "")));
            	
            	LegsArray.put(legObject);
            }
            finalJson.put("legsArray", LegsArray);
            Map<String, Object> hashMap = finalJson.toMap();
            dao.setValue("response", hashMap);
        } catch (Exception e) {
        	dao.setError("Error processing nullpractice: " + e.getMessage());
        }
    }
    
    private Object transformEmptyStringToNull(String value) {
        return (value == null || value.isEmpty()) ? JSONObject.NULL : value;
    }
}