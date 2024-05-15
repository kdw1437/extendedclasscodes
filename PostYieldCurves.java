package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;

public class PostYieldCurves {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        
        String jsonStr = dao.getStringValue("a");
        try {
            // Validation
            if (dataSetId == null || dataSetId.trim().isEmpty()) {
                throw new IllegalArgumentException("dataSetId가 empty string이거나 없습니다.");
            }
            if (baseDt == null || baseDt.trim().isEmpty()) {
                throw new IllegalArgumentException("baseDt가 empty string이거나 없습니다.");
            }
	        //JSON string을 ListParam객체로 바꾼다.
	        JSONArray jsonArray = new JSONArray(jsonStr);
	        ListParam listParam = new ListParam(new String[]{"BASE_DT", "DATA_SET_ID", "DATA_ID", "EXPR_VAL", "ERRT"});
	
	        for (int i = 0; i < jsonArray.length(); i++) {
	            JSONObject jsonObject = jsonArray.getJSONObject(i);
	            String dataId = jsonObject.getString("dataId");
	            String currency = jsonObject.getString("currency");
	            JSONArray yieldsArray = jsonObject.getJSONArray("yields");
	
	            for (int j = 0; j < yieldsArray.length(); j++) {
	                JSONObject yieldObject = yieldsArray.getJSONObject(j);
	                double tenor = yieldObject.getDouble("tenor");
	                double rate = yieldObject.getDouble("rate");
	
	                // 각 yield entry에 대해서 ListParam객체에 새로운 row(객체)를 추가한다.
	                int rowIdx = listParam.createRow();
	                listParam.setValue(rowIdx, "BASE_DT", baseDt);
	                listParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
	                listParam.setValue(rowIdx, "DATA_ID", dataId);
	                listParam.setValue(rowIdx, "EXPR_VAL", tenor);
	                listParam.setValue(rowIdx, "ERRT", rate);
	            }
	        }
	        log.info(listParam.toString());
	        // Set the converted ListParam in the DaoService object
	        dao.setValue("insertYieldCurvesTp", listParam);
	       
	        // Attempt to execute the SQL statement
	        
	            dao.sqlexe("s_insertYieldCurves2", false);
        } catch (IllegalArgumentException e) {
            log.error("Validation error: " + e.getMessage(), e);
            dao.setError(e.getMessage());
        } catch (SQLServiceException e) {
            log.error("SQL execution error: " + e.getMessage(), e);
            dao.setError(e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error: " + e.getMessage(), e);
            dao.setError(e.getMessage());
        }
    }
}
