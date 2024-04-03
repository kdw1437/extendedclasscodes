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

public class PostQuoteData {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        //jsonStr이 DaoService 객체에서 받은 JSON string이다.
        String jsonStr = dao.getStringValue("a");
        
        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            String[] columns = {"1stredemday", "2ndredemday", "3rdredemday", "4thredemday", "5thredemday", "6thredemday"};
            ListParam listParam = new ListParam(columns);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObj = jsonArray.getJSONObject(i);
                String effectiveDateStr = jsonObj.getString("effectiveDate");
                int earlyRedempCycleMonths = jsonObj.getInt("earlyRedempCycle");
                int settleDateOffset = jsonObj.getInt("settleDateOffset");
                
                LocalDate effectiveDate = LocalDate.parse(effectiveDateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                List<String> dates = calculateFutureDates(effectiveDate, earlyRedempCycleMonths);

                // 새로운 열을 생성한다.
                int rowIndex = listParam.createRow();

                // 예상대로 dates list가 정확히 6개의 element를 가지는지 확인한다.
                if (dates.size() != columns.length) {
                    throw new IllegalArgumentException("The dates list must contain exactly 6 elements.");
                }

                // 리스트로 부터 각각의 date를 각 column에 할당한다.
                for (int j = 0; j < columns.length; j++) {
                    listParam.setValue(rowIndex, columns[j], dates.get(j));
                }

                String datesString = String.join(", ", dates);
                log.debug("Calculated future dates for JSON object {}: {}", i, datesString);
            }

            // 여러 row를 가진 listParam 객체를 사용한다.
            // listParam객체를 daoService의 setValue method를 통해서 데이터베이스의 테이블에 넣을 수 있다.
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
    	return false;
    }
    


}
