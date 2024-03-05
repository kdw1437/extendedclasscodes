package com.jurosys.extension.com;

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;
import com.uro.util.JsonUtil;

public class PostCorrs {
    Logger log = LoggerMg.getInstance().getLogger();
    @SuppressWarnings("unchecked")
    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        String matrixId = dao.getRequest().getParameter("matrixId");
        
        String jsonStr = dao.getStringValue("a");
        
        try {
            // JSON string을 HashMap의 List로 파싱한다.
        	List<HashMap<String, Object>> dataList = (List<HashMap<String, Object>>) (Object) JsonUtil.getObjectList(HashMap.class, jsonStr);

            // dataList에서 각 item을 Iterate하고 sql query를 execute한다.
            for (HashMap<String, Object> dataItem : dataList) {
                String dataId1 = (String) dataItem.get("dataId1");
                String dataId2 = (String) dataItem.get("dataId2");
                Double corr = (Double) dataItem.get("corr");

                // SQL query를 위한 parameter 세팅
                ListParam listParam = new ListParam(new String[]{"BASE_DT", "DATA_SET_ID", "DATA_ID", "CRLT_CFCN_MATX_ID", "TH01_DATA_ID", "TH02_DATA_ID", "CRLT_CFCN"});
                listParam.createRow();
                listParam.setValue(0, "BASE_DT", baseDt);
                listParam.setValue(0, "DATA_SET_ID", dataSetId);
                listParam.setValue(0, "DATA_ID", dataId1 + ":" + dataId2); // DATA_ID포맷
                listParam.setValue(0, "CRLT_CFCN_MATX_ID", matrixId);
                listParam.setValue(0, "TH01_DATA_ID", dataId1);
                listParam.setValue(0, "TH02_DATA_ID", dataId2);
                listParam.setValue(0, "CRLT_CFCN", corr);

                // DaoService의 setSqlParamAddValue메소드 사용.
                dao.setSqlParamAddValue("InsertCorrsTp", listParam); //query에 listParam객체를 InsertCorrsTp key로 넘겨준다.
                dao.sqlexe("s_insertCorrs", true); //sql query execution
            }
        } catch (Exception e) {
            log.error("Error posting correlations: " + e.getMessage(), e);
        }
        
}
}
