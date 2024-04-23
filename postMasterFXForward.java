package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class postMasterFXForward {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
    	
    	//String jsonStr = null;
    	
    	try {
	    		String jsonStr = dao.getStringValue("a");
		    	   
		    	JSONArray jsonArray = new JSONArray(jsonStr);
	            String[] columns = {"DATA_ID", "DATA_NM", "CRNC_CODE", "RELT_CRNC_CODE"};
	            ListParam listParam = new ListParam(columns);
	            
	            for (int i = 0; i < jsonArray.length(); i++) {
	            	JSONObject jsonObj = jsonArray.getJSONObject(i);
	            	String dataId = jsonObj.getString("dataId");
	            	String dataNm = jsonObj.getString("dataNM");
	            	String crncCode = jsonObj.getString("crnc");
	            	String reltCrnc = jsonObj.getString("reltCrnc");
	            	
	            	int rowIdx = listParam.createRow();
	            	listParam.setValue(rowIdx, "DATA_ID", dataId);
	            	listParam.setValue(rowIdx, "DATA_NM", dataNm);
	            	listParam.setValue(rowIdx, "CRNC_CODE", crncCode);
	            	listParam.setValue(rowIdx, "RELT_CRNC_CODE", reltCrnc);
	            }
	            //log.info(listParam.toString());
	            log.info(listParam.toString());
	            
	            dao.setValue("insertFXForwardMasterTp", listParam);
	            
	            dao.sqlexe("s_insertFXForwardMaster", false);
    		  
    	} catch (Exception e) {
    		log.error("Error processing jsondata into listParam", e);
    	}
    	
}
}