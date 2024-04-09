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

public class PostQuoteData {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        //jsonStr이 DaoService 객체에서 받은 JSON string이다.
        String jsonStr = dao.getStringValue("a");
        
        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            String[] columns = {
                    "1stredemday", "2ndredemday", "3rdredemday", "4thredemday", "5thredemday", "6thredemday",
                    "1stsettlement", "2ndsettlement", "3rdsettlement", "4thsettlement", "5thsettlement", "6thsettlement",
                    "1stexercisePrice", "2ndexercisePrice", "3rdexercisePrice", "4thexercisePrice", "5thexercisePrice", "6thexercisePrice"
                };
                ListParam listParam = new ListParam(columns);

                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject jsonObj = jsonArray.getJSONObject(i);
                    String effectiveDateStr = jsonObj.getString("effectiveDate");
                    int earlyRedempCycleMonths = jsonObj.getInt("earlyRedempCycle");
                    int settleDateOffset = jsonObj.getInt("settleDateOffset");
                    String exercisePricesStr = jsonObj.getString("exercisePrices");
                    
                    LocalDate effectiveDate = LocalDate.parse(effectiveDateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                    List<String> dates = calculateFutureDates(effectiveDate, earlyRedempCycleMonths);
                    List<String> settlementDates = calculateSettlementDates(dates, settleDateOffset);
                    List<String> exercisePrices = Arrays.asList(exercisePricesStr.split("-")); //Array.asList 메소드는 parameter(array)를 List 타입으로 바꾸어 준다.

                    // listParam 새로운 열을 생성
                    int rowIndex = listParam.createRow();

                    // 각 column에 맞게 date를 할당
                    for (int j = 0; j < 6; j++) {
                        listParam.setValue(rowIndex, columns[j], dates.get(j));
                        listParam.setValue(rowIndex, columns[6 + j], settlementDates.get(j));
                        listParam.setValue(rowIndex, columns[12 + j], exercisePrices.get(j));
                    }

                    log.debug("Calculated future dates for JSON object {}: {}", i, String.join(", ", dates));
                    log.debug("Calculated settlement dates for JSON object {}: {}", i, String.join(", ", settlementDates));
                    log.debug("Exercise Prices for JSON object {}: {}", i, String.join(", ", exercisePrices));
                }

                log.debug("Final ListParam with multiple rows: {}", listParam.toString());
            
                log.debug("Final ListParam with multiple rows: {}", listParam.toString());
        } catch (Exception e) {
        	log.error("Error executing PostQuoteData", e);
        }
        
    }
    
    private List<String> calculateFutureDates(LocalDate startDate, int monthsBetween) {
    	List<String> dates = new ArrayList<>();
    	LocalDate date = startDate;
    	for (int i = 0; i < 6; i++) { //6개의 element가 arrayList에 들어간다.
    		date = date.plusMonths(monthsBetween);
    		date = adjustForWeekendAndHolidays(date);
    		dates.add(date.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    	}
    	return dates;
    }
    
    private List<String> calculateSettlementDates(List<String> futureDates, int settleDateOffset) {
        List<String> settlementDates = new ArrayList<>();
        for (String dateStr : futureDates) {
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
            date = date.plusDays(settleDateOffset);
            date = adjustForWeekendAndHolidays(date);
            settlementDates.add(date.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        }
        return settlementDates;
    }

    private LocalDate adjustForWeekendAndHolidays(LocalDate date) {
    	//date가 holiday인지 확인하고, 날짜를 조정해준다.
    	while (date.getDayOfWeek() == java.time.DayOfWeek.SATURDAY ||
    			date.getDayOfWeek() == java.time.DayOfWeek.SUNDAY ||
    			isHoliday(date)) {
    		date = date.plusDays(1);
    	}
    	return date;
    }
    
    private boolean isHoliday(LocalDate date) {
        try {
            String apiKey = "uC4peG%2F98qVjSVKNVtW6WJC0lMM9KtZBxEid%2BhieYXDn7B3zxC0DkfNW9e2qVOPSuD9%2BI8EyF7D4gzXzx6NV9g%3D%3D"; // Encoded API Key
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
                            return true; // 휴일입니다.
                        }
                    }
                }
            }
        } catch (Exception e) {
        	log.error("Error checking if {} is a holiday", date, e);
        }
        log.debug("{} is not a holiday", date);
        return false; // Not a holiday or error occurred
    }
    


}
