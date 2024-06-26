package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;
import javax.xml.bind.ValidationException;

public class ProcessHisVols {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
    	
    	try {
    		dao.sqlexe("s_selectHistoricalVol_v1", false);
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
            	throw new ValidationException("No data");
            }
            
            JSONObject finalJson = new JSONObject();
            JSONArray volsArray = new JSONArray();
            
            for (int i = 0; i < result.rowSize(); i++) {
            	JSONObject hisVolObject = new JSONObject();
            	hisVolObject.put("baseDt", result.getValue(i, "baseDt", ""));
            	hisVolObject.put("dataSetId", result.getValue(i, "dataSetId", ""));
            	hisVolObject.put("dataId", result.getValue(i, "dataId", ""));
            	//string타입으로 나오는 데이터를 double타입 (숫자 형태)으로 바꿔준다.
            	String vltlRtStr = result.getValue(i, "vltlRt", "");
            	double vltlRtNum = Double.parseDouble(vltlRtStr);
            	hisVolObject.put("historicalVol", vltlRtNum);
            	
            	volsArray.put(hisVolObject);
            }
            
            finalJson.put("historicalVols", volsArray);
            Map<String, Object> hashMap = finalJson.toMap();
            dao.setValue("response", hashMap);
            log.debug("HashMap contents: " + hashMap.toString());
    	} catch (ValidationException e) {
    		log.error("validation error", e);
    		dao.setError(e.getMessage());
    	} catch (SQLServiceException e) {
    		log.error("sql exception error", e);
    		dao.setError("sqlException: " + e.getMessage());
    	} catch (IllegalArgumentException e) {
    		dao.setError(e.getMessage());
    	}
    	
    }
}
