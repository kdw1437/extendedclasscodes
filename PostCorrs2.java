package com.jurosys.extension.com;

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;
import com.uro.util.JsonUtil;

public class PostCorrs2 {
	
	Logger log = LoggerMg.getInstance().getLogger();
	
	public void execute(DaoService dao) {
		
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        String matrixId = dao.getRequest().getParameter("matrixId");
        
        String jsonStr = dao.getStringValue("a");
        
        try {
        	
                // Step 1: Validation
                if (dataSetId == null || dataSetId.trim().isEmpty() || baseDt == null || baseDt.trim().isEmpty()|| matrixId == null || matrixId.trim().isEmpty() ||
                		jsonStr == null || jsonStr.trim().isEmpty()) {
                    throw new IllegalArgumentException("Missing required parameters");
                }

                // Step 2: Parse JSON string to a JSONArray
                JSONArray dataArray = new JSONArray(jsonStr);

                // Step 3: Create ListParam object and populate it with data
                ListParam listParam = new ListParam(new String[]{"BASE_DT", "DATA_SET_ID", "DATA_ID", "CRLT_CFCN_MATX_ID", "TH01_DATA_ID", "TH02_DATA_ID", "CRLT_CFCN"});

                for (int i = 0; i < dataArray.length(); i++) {
                    JSONObject dataItem = dataArray.getJSONObject(i);
                    String dataId1 = dataItem.getString("dataId1");
                    String dataId2 = dataItem.getString("dataId2");
                    Double corr = dataItem.getDouble("corr");

                    listParam.createRow();
                    listParam.setValue(listParam.rowSize() - 1, "BASE_DT", baseDt);
                    listParam.setValue(listParam.rowSize() - 1, "DATA_SET_ID", dataSetId);
                    listParam.setValue(listParam.rowSize() - 1, "DATA_ID", dataId1 + ":" + dataId2);
                    listParam.setValue(listParam.rowSize() - 1, "CRLT_CFCN_MATX_ID", matrixId);
                    listParam.setValue(listParam.rowSize() - 1, "TH01_DATA_ID", dataId1);
                    listParam.setValue(listParam.rowSize() - 1, "TH02_DATA_ID", dataId2);
                    listParam.setValue(listParam.rowSize() - 1, "CRLT_CFCN", corr);
                }

                // Step 4: Database operation - perform the operation only once
                dao.setSqlParamAddValue("InsertCorrsTp", listParam);
                dao.sqlexe("s_insertCorrs", true);
        } catch (Exception e) {
        	dao.setError(e.getMessage());
        }
	}
}
