package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.service.sql.SQLParam;

public class PostYieldCurve2 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
    	
    	// Extract DATA_SET_ID and BASE_DT from the URL parameters
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        
        String jsonStr = dao.getStringValue("a");
        JSONArray jsonArray = new JSONArray(jsonStr);

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String dataId = jsonObject.getString("dataId");
            JSONArray yieldsArray = jsonObject.getJSONArray("yields");

            for (int j = 0; j < yieldsArray.length(); j++) {
                JSONObject yieldObject = yieldsArray.getJSONObject(j);
                double tenor = yieldObject.getDouble("tenor");
                double rate = yieldObject.getDouble("rate");

                // Prepare SQLParam for each yield entry
                SQLParam sqlParam = new SQLParam();
                sqlParam.addValue("DATA_SET_ID", dataSetId); // Set DATA_SET_ID
                sqlParam.addValue("BASE_DT", baseDt); // Set BASE_DT
                sqlParam.addValue("DATA_ID", dataId);
                sqlParam.addValue("EXPR_VAL", tenor);
                sqlParam.addValue("ERRT", rate);

                // Set the SQLParam in the DaoService object for SQL execution
                dao.setSqlParam(sqlParam);
                
                // Execute the SQL statement for each yield entry
                try {
                	log.info("Executing SQL for BASE_DT: " + baseDt + ", DATA_SET_ID: " + dataSetId + ", DATA_ID: " + dataId + ", EXPR_VAL: " + tenor + ", ERRT: " + rate);
                    dao.sqlexe("s_insertYieldCurves", true);
                } catch (SQLServiceException e) {
                    log.error("SQL execution error: ", e);
                }
            }
        }
    }
}
