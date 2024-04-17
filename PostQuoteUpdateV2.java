package com.jurosys.extension.com;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
        			double decimalPrice = Double.parseDouble(exercisePricesArray[j].trim())/100.0;
        			BigDecimal termCouponBD = new BigDecimal(annualCoupon / 2)
        					.multiply(new BigDecimal(j+1))
        					.divide(new BigDecimal(100), 3, RoundingMode.HALF_UP);
        			termCouponBD = termCouponBD.setScale(3, RoundingMode.HALF_UP);
        			
        			LocalDate redemptionDate = lastAdjustedDate.plusMonths(earlyRedempCycleMonths);
        			redemptionDate = adjustForWeekendAndHolidays(redemptionDate);
        			lastAdjustedDate = redemptionDate;
        			
        			LocalDate settlementDate = redemptionDate.plusDays(settleDateOffset);
        			settlementDate = adjustForWeekendAndHolidays(settlementDate);
        		
        			int rowIdx = listParam.createRow();
        			listParam.setValue(rowIdx, "GDS_ID", productId);
        			listParam.setValue(rowIdx, "ACTP_RT", decimalPrice);
        			listParam.setValue(rowIdx, "CPN_RT", termCouponBD.doubleValue());
        			listParam.setValue(rowIdx, "EVLT_DT", redemptionDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        			listParam.setValue(rowIdx, "SETL_DT", settlementDate.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        			listParam.setValue(rowIdx, "SQNC", SQNC);
                    SQNC++;
        		
                    
        		}
        		
        		//log.debug(uniqueExercisePrices.toString());
        		
        	}
        	log.debug("Updated ListParam: {}", listParam.toString());
            
            log.debug("Updated ListParam: {}", listParam.toString());
        } catch (Exception e) {
        	log.error("Error processing JSON data", e);
        }
        
        try {
        	JSONArray jsonArray = new JSONArray(jsonStr);
        	String[] columns = {"CNTR_ID", "SEQ", "UNAS_ID"};
        	ListParam listParam = new ListParam(columns);
        	
        	for (int i = 0; i < jsonArray.length(); i++) {
        		JSONObject jsonObject = jsonArray.getJSONObject(i);
        		int productId = jsonObject.getInt("productId");
        		ArrayList<String> unasIds = new ArrayList<String>();
        	    String underlyingAsset1 = jsonObject.optString("underlyingAsset1", null);
        	    String underlyingAsset2 = jsonObject.optString("underlyingAsset2", null);
        	    String underlyingAsset3 = jsonObject.optString("underlyingAsset3", null);

        	    // Add to list if not null
        	    if (underlyingAsset1 != null) unasIds.add(underlyingAsset1);
        	    if (underlyingAsset2 != null) unasIds.add(underlyingAsset2);
        	    if (underlyingAsset3 != null) unasIds.add(underlyingAsset3);
        		int seq = 1;
        		for (String item : unasIds) {
        			if (item != null) {
        				
        				int rowIdx = listParam.createRow();
        				listParam.setValue(rowIdx, "CNTR_ID", productId);
        				listParam.setValue(rowIdx, "SEQ", seq);
        				listParam.setValue(rowIdx, "UNAS_ID", item);
        				seq++;
        			} else {
        				continue;
        			}
        		}
        	}
        	
        	log.debug("Updated ListParam: {}", listParam.toString());
            
            log.debug("Updated ListParam: {}", listParam.toString());	
        } catch (Exception e) {
        	log.error("Error processing JSON data", e);
        }

}
    private LocalDate adjustForWeekendAndHolidays(LocalDate date) {
        while (date.getDayOfWeek() == java.time.DayOfWeek.SATURDAY ||
                date.getDayOfWeek() == java.time.DayOfWeek.SUNDAY ||
                isHoliday(date)) {
            date = date.plusDays(1);
        }
        return date;
    }

    private boolean isHoliday(LocalDate date) {
        try {
            String apiKey = "uC4peG%2F98qVjSVKNVtW6WJC0lMM9KtZBxEid%2BhieYXDn7B3zxC0DkfNW9e2qVOPSuD9%2BI8EyF7D4gzXzx6NV9g%3D%3D";
            String requestUrl = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
                    + "?solYear=" + date.getYear()
                    + "&solMonth=" + String.format("%02d", date.getMonthValue())
                    + "&ServiceKey=" + apiKey;
            
            log.debug("Making API call to check holidays for the date: {}", date);
            URL url = new URL(requestUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.connect();

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("HttpResponseCode: " + responseCode);
            } else {
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(conn.getInputStream());
                doc.getDocumentElement().normalize();
                
                NodeList nList = doc.getElementsByTagName("item");
                for (int temp = 0; temp < nList.getLength(); temp++) {
                    Node nNode = nList.item(temp);
                    if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element eElement = (Element) nNode;
                        String locdate = eElement.getElementsByTagName("locdate").item(0).getTextContent();
                        String isHoliday = eElement.getElementsByTagName("isHoliday").item(0).getTextContent();
                        if (locdate.equals(date.format(DateTimeFormatter.ofPattern("yyyyMMdd"))) && "Y".equals(isHoliday)) {
                            log.debug("{} is a holiday", date);
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error checking if {} is a holiday", date, e);
        }
        return false;
    }
}
