package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;

public class PostDividendStream {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        
        String jsonStr = dao.getStringValue("a");
        
        try {
            // Validation
            if (dataSetId == null || dataSetId.trim().isEmpty() ||
                baseDt == null || baseDt.trim().isEmpty() ||
                jsonStr == null || jsonStr.trim().isEmpty()) {
                throw new IllegalArgumentException("Missing required parameters");
            }
            //JSON string을 ListParam객체로 바꾼다.
	        JSONArray jsonArray = new JSONArray(jsonStr);
	        ListParam listParam = new ListParam(new String[]{"BASE_DT", "DATA_SET_ID", "DATA_ID", "DVID_DT", "DVIDA"});
	
	        
	        for (int i = 0; i < jsonArray.length(); i++) {
	            JSONObject jsonObject = jsonArray.getJSONObject(i);
	            String dataId = jsonObject.getString("dataId");
	            
	            JSONArray yieldsArray = jsonObject.getJSONArray("dividends");
	
	            for (int j = 0; j < yieldsArray.length(); j++) {
	                JSONObject yieldObject = yieldsArray.getJSONObject(j);
	                String date = yieldObject.getString("date");
	                double value = yieldObject.getDouble("value");
	
	                // 각 yield entry에 대해서 ListParam객체에 새로운 row(객체)를 추가한다.
	                int rowIdx = listParam.createRow();
	                listParam.setValue(rowIdx, "BASE_DT", baseDt);
	                listParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
	                listParam.setValue(rowIdx, "DATA_ID", dataId);
	                listParam.setValue(rowIdx, "DVID_DT", date);
	                listParam.setValue(rowIdx, "DVIDA", value);
	            }
	        }
	        log.info(listParam.toString());
	        // 변환된 ListParam개게를 DaoService 객체에 set한다.
	        dao.setValue("insertDividendStreamTp", listParam);
       
	        // SQL statement를 execute한다.
            dao.sqlexe("s_insertDividendStream", false);
        } catch (SQLServiceException e) {
            e.printStackTrace();
            dao.setError(e.getMessage());
        } catch (IllegalArgumentException e) {
        	dao.setError(e.getMessage());
        } catch (Exception e) {
        	dao.setError(e.getMessage());
        }
    }
}
