package com.jurosys.extension.com;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import java.util.*;

import javax.xml.bind.ValidationException;

public class ProcessPrices {
    Logger log = LoggerMg.getInstance().getLogger("fw");

    public void execute(DaoService dao) {
        try {
            // SQL query를 실핸한다.
            dao.sqlexe("s_selectPrices_v1", false); // 가격에 대해 sqlqueryId를 실행한다.
            ListParam result = dao.getNowListParam(); // query결과를 얻는다.

            //1. data validation
        	String forValidBaseDt = dao.getStringValue("baseDt");
            String forValidDataIds = dao.getStringValue("dataIds");
            
            //1.1. baseDt가 null인 경우
            if (forValidBaseDt == null || forValidBaseDt.trim().isEmpty()) {
                throw new IllegalArgumentException("baseDt가 null이나 empty이다.");
            }
        	
        	//1.2. dataIds가 null인 경우
            if (forValidDataIds == null || forValidDataIds.trim().isEmpty()) {
                throw new IllegalArgumentException("dataIds가 null이나 empty이다.");
            }
            
            //validation
            if (result.rowSize() == 0) {
            	throw new IllegalArgumentException("No data");
            }

            // JSON 구조를 초기화한다.
            JSONObject finalJson = new JSONObject();
            JSONArray pricesArray = new JSONArray();

            // 결과에서 각 row를 처리한다.
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
                    priceObject.put("currency", JSONObject.NULL);  // 비었으면 null로 처리한다.
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
                	double priceNum = Double.parseDouble(priceStr);  // string을 double로 바꾼다.
                	priceObject.put("price", priceNum);  // numeric JSON representation을 자동으로 사용한다.
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
        }  catch (IllegalArgumentException e) {
    		dao.setError(e.getMessage());
    	}
    }
}
