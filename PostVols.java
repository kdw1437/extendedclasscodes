package com.jurosys.extension.com;


import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class PostVols {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        // jsonStr은 DaoService 객체로 부터 받은 JSON string이다.
        String jsonStr = dao.getStringValue("a");

        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            // ListParam객체에 대한 column을 정의한다.
            String[] columns = {"BASE_DT", "DATA_SET_ID", "DATA_ID", "EXPR_VAL", "VLTL_RT", "SWAP_EXPR_VAL"};
            ListParam listParam = new ListParam(columns);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject dataObject = jsonArray.getJSONObject(i);
                String dataId = dataObject.getString("dataId");
                JSONArray volCurves = dataObject.getJSONArray("volCurves");

                for (int j = 0; j < volCurves.length(); j++) {
                    JSONObject volCurve = volCurves.getJSONObject(j);
                    double volFactor = volCurve.getDouble("volFactor");
                    JSONArray termVols = volCurve.getJSONArray("termVols");

                    for (int k = 0; k < termVols.length(); k++) {
                        JSONObject termVol = termVols.getJSONObject(k);
                        double tenor = termVol.getDouble("tenor");
                        double vol = termVol.getDouble("vol");

                        int rowIdx = listParam.createRow();
                        listParam.setValue(rowIdx, "BASE_DT", baseDt);
                        listParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
                        listParam.setValue(rowIdx, "DATA_ID", dataId);
                        listParam.setValue(rowIdx, "EXPR_VAL", tenor);
                        listParam.setValue(rowIdx, "VLTL_RT", vol);
                        listParam.setValue(rowIdx, "SWAP_EXPR_VAL", volFactor);
                    }
                }
            }

            log.info(listParam.toString());

            
            dao.setValue("insertTermVolsTp", listParam);

            // sql statement 실행
            dao.sqlexe("s_insertTermVols", false);
        } catch (ParamException | SQLServiceException e) {
            log.error("Error executing PostTermVols", e);
            // exception 핸들링
        }
    }
}
