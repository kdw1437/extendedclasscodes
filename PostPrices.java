package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;


public class PostPrices {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        //jsonStr이 DaoService 객체에서 받은 JSON string이다.
        String jsonStr = dao.getStringValue("a");
        
        
        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            String[] columns = {"BASE_DT", "DATA_SET_ID", "DATA_ID", "CLOSE_PRIC"};
            ListParam listParam = new ListParam(columns);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObj = jsonArray.getJSONObject(i);
                String dataId = jsonObj.getString("dataId");
                String closePric = String.valueOf(jsonObj.getDouble("price"));

                int rowIdx = listParam.createRow();
                listParam.setValue(rowIdx, "BASE_DT", baseDt);
                listParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
                listParam.setValue(rowIdx, "DATA_ID", dataId);
                listParam.setValue(rowIdx, "CLOSE_PRIC", closePric);
            }

            log.info(listParam.toString());

            dao.setValue("insertPricesTp", listParam);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertPrices", false);
        } catch (ParamException | SQLServiceException e) {
            log.error("Error executing PostPrices", e);
            // 예외를 다룬다.
        }
}
}
