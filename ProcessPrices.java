package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;

public class ProcessPrices {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
            // Execute the SQL query
            dao.sqlexe("s_selectPrices_v1", false); // Use the SQL query ID for prices
            ListParam result = dao.getNowListParam(); // Get the result of the query

            // Check for no results
            if (result.rowSize() == 0) {
                JSONObject errorJson = new JSONObject();
                errorJson.put("error", "URL에 필요한 parameter를 제대로 입력하지 않았습니다.");
                dao.setValue("response", errorJson.toMap());
                log.info(errorJson.toString());
                return;
            }

            // Initialize the JSON structure
            JSONObject finalJson = new JSONObject();
            JSONArray pricesArray = new JSONArray();

            // Process each row in the result
            for (int i = 0; i < result.rowSize(); i++) {
                JSONObject priceObject = new JSONObject();
                priceObject.put("baseDt", result.getValue(i, "baseDt", ""));
                priceObject.put("dataSetId", result.getValue(i, "dataSetId", ""));
                priceObject.put("dataId", result.getValue(i, "dataId", ""));
                //priceObject.put("currency", result.getValue(i, "currency", ""));
                String currencyStr = result.getValue(i, "currency", "");
                if (!currencyStr.isEmpty()) {
                    priceObject.put("currency", currencyStr);
                } else {
                    priceObject.put("currency", JSONObject.NULL);  // Set to null if empty
                }
                //priceObject.put("baseCurrency", result.getValue(i, "baseCurrency", ""));
                String baseCurrencyStr = result.getValue(i, "baseCurrency", "");
                if (!baseCurrencyStr.isEmpty()) {
                    priceObject.put("baseCurrency", baseCurrencyStr);
                } else {
                    priceObject.put("baseCurrency", JSONObject.NULL);
                }
                //priceObject.put("price", result.getValue(i, "price", ""));

                String priceStr = result.getValue(i, "price", "");
                
                if (!priceStr.isEmpty()) {
                	double priceNum = Double.parseDouble(priceStr);  // Converts string to double
                	priceObject.put("price", priceNum);  // Automatically uses numeric JSON representation
                } else {
                	priceObject.put("price", JSONObject.NULL);
                }
                pricesArray.put(priceObject);
                
                
                }

            finalJson.put("prices", pricesArray);
            Map<String, Object> hashMap = finalJson.toMap();
            dao.setValue("response", hashMap);
            log.info("HashMap contents: " + hashMap.toString());

        } catch (SQLServiceException e) {
            log.error("Error processing prices", e);
            dao.setError("Error processing prices: " + e.getMessage());
        }
    }
}
