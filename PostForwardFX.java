package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;
import javax.xml.bind.ValidationException;

public class PostForwardFX {
	Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) { 
    	//모든 try문에서 공통적으로 사용할 변수는 try문 바깥에서 선언한다.
    	//structured json에서 jsonArray, jsonObject는 jsonArray, jsonObject 객체로 받는다.
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
                
        try {
        	
        	// null또는 빈 변수 확인
            if (dataSetId == null || dataSetId.trim().isEmpty() || baseDt == null || baseDt.trim().isEmpty()) {
                throw new ValidationException("DataSetId or BaseDt cannot be null or empty.");
            }
            
        	String jsonStr = dao.getStringValue("a");
        	
        	JSONArray jsonArray = new JSONArray(jsonStr);
        	String[] columns = {"BASE_DT", "DATA_SET_ID", "DATA_ID", "EXPR_VAL", "ERRT", "RISK_FCTR_CODE"};
        	
        	ListParam listParam = new ListParam(columns);
        	
        	for (int i = 0; i < jsonArray.length(); i++) {
        		JSONObject jsonObject = jsonArray.getJSONObject(i);
        		String dataId = jsonObject.getString("dataId");
        		JSONArray yieldsArray = jsonObject.getJSONArray("yields");
        		
        		for (int j = 0; j < yieldsArray.length(); j++) {
        			JSONObject yieldObject = yieldsArray.getJSONObject(j);
        			double tenor = yieldObject.getDouble("tenor");
        			double value = yieldObject.getDouble("value");
        			
        			int dayCount = (int) Math.round(tenor * 360);
                    String formattedDayCount = String.format("%05d", dayCount);  // 문자열이 최소 5자리 이상이 되도록, 필요 시 앞에 0을 채워넣음
                    String riskFctrCode = dataId + "_" + formattedDayCount;
                    
                    //log.info("Risk Factor Code: " + riskFctrCode);
                    
                    int rowIdx = listParam.createRow();
                    listParam.setValue(rowIdx, "BASE_DT", baseDt);
                    listParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
	            	listParam.setValue(rowIdx, "DATA_ID", dataId);
	            	listParam.setValue(rowIdx, "EXPR_VAL", tenor);
	            	listParam.setValue(rowIdx, "ERRT", value);
	            	listParam.setValue(rowIdx, "RISK_FCTR_CODE", riskFctrCode);
        		}
        		
        	}
        	
        	//log.debug(listParam.toString());
        	
        	log.debug(listParam.toString());
        	
        	dao.setValue("insertForwardFXTp", listParam);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertForwardFX", false);
        } catch (Exception e) {
            log.error("Error processing JSON data into listParam", e);
            dao.setError(e.getMessage());
        }

    }
}
