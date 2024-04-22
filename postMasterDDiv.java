package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class postMasterDDiv {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
    	
    	String jsonStr = null;
    	
    	try {
    		jsonStr = dao.getStringValue("a");
    	} catch (Exception e) {
    		log.error("Error getting jsonString", e);
    	}
    	
    	try { 
    		  if(jsonStr != null) {
	    		 JSONArray jsonArray = new JSONArray(jsonStr);
	             String[] columns = {"DATA_ID", "DATA_NM", "DVID_TP", "CRNC_CODE"};
	             ListParam listParam = new ListParam(columns);
    		  } else {
    			  throw new Exception("jsonStr is null"); //Correctly throw an exception
    		  }
    		  
    	} catch (Exception e) {
    		log.error("Error processing jsondata into listParam", e);
    	}
    	
}
}