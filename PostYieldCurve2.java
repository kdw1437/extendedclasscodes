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
    	
    	// URL parameter로 부터 DATA_SET_ID와 BASE_DT를 뽑아낸다.
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

                // 각 yield entry에 대해서 sqlParam객체를 준비한다.
                SQLParam sqlParam = new SQLParam();
                sqlParam.addValue("DATA_SET_ID", dataSetId); // Set DATA_SET_ID
                sqlParam.addValue("BASE_DT", baseDt); // Set BASE_DT
                sqlParam.addValue("DATA_ID", dataId);
                sqlParam.addValue("EXPR_VAL", tenor);
                sqlParam.addValue("ERRT", rate);

                // SQL execution을 위해 DaoService객체에 SQLParam을 세팅한다.
                dao.setSqlParam(sqlParam);
                
                // 각 yield entry에 대해서 SQL문을 Execute한다.
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
//이 코드는 데이터 입력은 되는데 마지막에 에러가 뜨는 문제가 발생한다. 데이터 Post는 원활하게 잘 이루어진다. 하지만 마지막에 에러가 뜬다.