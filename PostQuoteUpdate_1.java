package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import java.net.HttpURLConnection;
import java.net.URL;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class PostQuoteUpdate_1 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        //jsonStr이 DaoService 객체에서 받은 JSON string이다.
        String jsonStr = dao.getStringValue("a");
        
        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            String[] columns = {"productId", "exercisePrice", "couponRate", "redemday", "settlement"};
            ListParam listParam = new ListParam(columns);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                int productId = jsonObject.getInt("productId");
                double annualCoupon = jsonObject.getDouble("coupon");
                String[] exercisePrices = jsonObject.getString("exercisePrices").split("-");
                int earlyRedempCycleMonths = jsonObject.getInt("earlyRedempCycle");
                int settleDateOffset = jsonObject.getInt("settleDateOffset");
                LocalDate effectiveDate = LocalDate.parse(jsonObject.getString("effectiveDate"), DateTimeFormatter.ofPattern("yyyyMMdd"));

                for (int j = 0; j < exercisePrices.length; j++) {
                    double decimalPrice = Double.parseDouble(exercisePrices[j].trim()) / 100.0;
                    double termCoupon = (annualCoupon / 2) * (j + 1); // Calculate the coupon rate for the term

                    LocalDate redemptionDate = effectiveDate.plusMonths(earlyRedempCycleMonths * (j + 1));
                    LocalDate settlementDate = redemptionDate.plusDays(settleDateOffset);
                    // Adjust for weekends and holidays if necessary

                    int rowIdx = listParam.createRow();
                    listParam.setValue(rowIdx, "productId", productId);
                    listParam.setValue(rowIdx, "exercisePrice", decimalPrice);
                    listParam.setValue(rowIdx, "couponRate", termCoupon);
                    listParam.setValue(rowIdx, "redemday", redemptionDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                    listParam.setValue(rowIdx, "settlement", settlementDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
                }
            }
            
            log.debug(listParam.toString());
            log.debug(listParam.toString());
        }catch (Exception e) {
        	log.error("Error processing JSON data", e);
        }
        
    }
}
