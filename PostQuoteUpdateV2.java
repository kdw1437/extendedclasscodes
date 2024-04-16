package com.jurosys.extension.com;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;

public class PostQuoteUpdateV2 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        String jsonStr = dao.getStringValue("a");
        
        try {
        	JSONArray jsonArray = new JSONArray(jsonStr);
        	String[] columns = {"GDS_ID", "ACTP_RT", "CPN_RT", "EVLT_DT", "SETL_DT", "SQNC"};
        	ListParam listParam = new ListParam(columns);
        	
            DecimalFormat decimalFormat = new DecimalFormat("0.000"); // Format to three decimal places
            decimalFormat.setRoundingMode(RoundingMode.HALF_UP);
            
        	for (int i = 0; i < jsonArray.length(); i++) {
        		JSONObject jsonObject = jsonArray.getJSONObject(i);
        		if (jsonObject.isNull("exercisePrices")) {
        			continue;
        		}
        		int productId = jsonObject.getInt("productId");
        		double annualCoupon = jsonObject.getDouble("coupon");
        		String exercisePricesRaw = jsonObject.getString("exercisePrices");
        		int earlyRedempCycleMonths = jsonObject.getInt("earlyRedempCycle");
        		int settleDateOffset = jsonObject.getInt("settleDateOffset");
        		LocalDate effectiveDate = LocalDate.parse(jsonObject.getString("effectiveDate"), DateTimeFormatter.ofPattern("yyyyMMdd"));
        		
        		LocalDate lastAdjustedDate = effectiveDate;
        		int SQNC = 1;
        		
        		String[] pricesWithParentheses = exercisePricesRaw.split("-");
        		
        		Pattern pattern = Pattern.compile("\\d+(?=\\()");
        		List<String> exercisePrices = new ArrayList<>();
        		
        		for (String price : pricesWithParentheses) {
        			Matcher matcher = pattern.matcher(price);
        			if (matcher.find()) {
        				exercisePrices.add(matcher.group());
        			} else {
        				exercisePrices.add(price.replaceAll("\\D", ""));
        			}
        		}
        		
        		String[] exercisePricesArray = exercisePrices.toArray(new String[0]);
        		
        		log.debug(Arrays.toString(exercisePricesArray));
        		
        		for (int j=0; j < exercisePricesArray.length; j++) {
        			
        		}
        		
        		//log.debug(uniqueExercisePrices.toString());
        		
        	}
        } catch (Exception e) {
        	log.error("Error processing JSON data", e);
        }

}
}
