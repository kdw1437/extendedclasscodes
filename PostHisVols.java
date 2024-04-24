package com.jurosys.extension.com;

import javax.xml.bind.ValidationException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class PostHisVols {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        
        try {
        	//validation
        	if (dataSetId == null || dataSetId.trim().isEmpty() || baseDt == null || baseDt.trim().isEmpty()) {
                throw new ValidationException("DataSetId or BaseDt cannot be null or empty.");
            }
        	
        	String jsonStr = dao.getStringValue("a");
        	String[] columns = {"BASE_DT", "DATA_SET_ID", "DATA_ID", "VLTL_RT"};
            ListParam listParam = new ListParam(columns);
        	
            JSONArray jsonArray = new JSONArray(jsonStr);
            for (int i = 0; i < jsonArray.length(); i++) {
            	JSONObject jsonObject = jsonArray.getJSONObject(i);
            	String dataId = jsonObject.getString("dataId");
            	double vltlrt = jsonObject.getDouble("historicalVol");
            	
            	int rowIdx = listParam.createRow();
            	listParam.setValue(rowIdx, "BASE_DT", baseDt);
            	listParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
            	listParam.setValue(rowIdx, "DATA_ID", dataId);
            	listParam.setValue(rowIdx, "VLTL_RT", vltlrt);
            			
            	
            }
        	
            log.debug(listParam.toString());
            
            dao.setValue("insertHisVolsTp", listParam);
            
            dao.sqlexe("s_insertHisVols", false);
        } catch (ValidationException e) {
        	log.error("Error from validation", e);
        	dao.setError(e.getMessage());
    	} catch (ParamException | SQLServiceException e) {
    		log.error("Error executing PostHisVols");
    		dao.setError(e.getMessage());
    	}
}
}