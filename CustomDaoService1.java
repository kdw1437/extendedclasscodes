package com.jurosys.extension.com;

import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONObject;

public class CustomDaoService1 extends DaoService {
	@Override
    public ListParam jsonToListParam(String jsonStr) throws ParamException {
        // Define the columns for the ListParam object
        String[] columns = new String[]{"dataId", "currency", "tenor", "rate"};
        ListParam listParam = new ListParam(columns);

        // Parse the JSON string
        JSONArray jsonArray = new JSONArray(jsonStr);

        // Iterate over each item in the JSON array
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String dataId = jsonObject.getString("dataId");
            String currency = jsonObject.getString("currency");
            JSONArray yieldsArray = jsonObject.getJSONArray("yields");

            // Iterate over each yield in the "yields" array
            for (int j = 0; j < yieldsArray.length(); j++) {
                JSONObject yieldObject = yieldsArray.getJSONObject(j);
                double tenor = yieldObject.getDouble("tenor");
                double rate = yieldObject.getDouble("rate");

                // Add a new row to the ListParam for each yield
                Object[] rowValues = new Object[]{dataId, currency, tenor, rate};
                listParam.addRow(rowValues);
            }
        }

        return listParam;
    }
}
