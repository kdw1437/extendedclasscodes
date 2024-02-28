package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;

public class PostYieldCurves {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {

        // Assume jsonStr is the JSON string received in the DaoService object
        String jsonStr = dao.getStringValue("a");

        // Convert the JSON string to a ListParam object for the top-level array
        JSONArray jsonArray = new JSONArray(jsonStr);
        ListParam listParam = new ListParam(new String[]{"dataId", "currency", "tenor", "rate"});

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String dataId = jsonObject.getString("dataId");
            String currency = jsonObject.getString("currency");
            JSONArray yieldsArray = jsonObject.getJSONArray("yields");

            for (int j = 0; j < yieldsArray.length(); j++) {
                JSONObject yieldObject = yieldsArray.getJSONObject(j);
                double tenor = yieldObject.getDouble("tenor");
                double rate = yieldObject.getDouble("rate");

                // Create a new row in ListParam for each yield entry
                int rowIdx = listParam.createRow();
                listParam.setValue(rowIdx, "dataId", dataId);
                listParam.setValue(rowIdx, "currency", currency);
                listParam.setValue(rowIdx, "tenor", tenor);
                listParam.setValue(rowIdx, "rate", rate);
            }
        }
        log.info(listParam.toString());
        // Set the converted ListParam in the DaoService object
        dao.setValue("insertYieldCurvesTp", listParam);
       
        // Attempt to execute the SQL statement
        try {
            dao.sqlexe("s_insertYieldCurves", false);
        } catch (SQLServiceException e) {
            e.printStackTrace();
        }
    }
}
