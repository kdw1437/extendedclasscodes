package com.jurosys.extension.com;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;
public class QuoteProcessMethods {
	private static Logger log = LoggerMg.getInstance().getLogger();
	private static LunarCalendar lunarCalendar = new LunarCalendar();
    // jsonObject(instance of class)의 특정 key(field)의 value가 null인 경우, null을 return하는 static 메소드
	// static method가 특정 클래스 내에서 선언되면, 클래스 이름을 붙이지 않고 클래스 내의 다른 메소드에서 사용되어 질 수 있다.
    public static Integer getNullableInteger(JSONObject jsonObject, String key) {
        if (!jsonObject.isNull(key)) {
            return jsonObject.optInt(key);
        } else {
            return null;
        }
    }
	
    public static Double getNullableDouble(JSONObject jsonObject, String key) {
        if (!jsonObject.isNull(key)) {
            return jsonObject.optDouble(key);  // This method returns a primitive double
        } else {
            return null;  // Return null when the value is JSON null
        }
    }
    
/*	private static List<LocalDate> holidays = Arrays.asList(
            LocalDate.of(2023, 1, 1), // New Year's Day            
            LocalDate.of(2023, 12, 25) // Christmas
        );
*/ 
/*    private static List<Holiday> holidays = Arrays.asList(
            new Holiday(1, 1),  // 신년
            
            new Holiday(12, 25) // 크리스마스
        );
*/    
    public static LocalDate adjustForWeekendAndHolidays(LocalDate date) {
        while (isWeekend(date) || isHoliday(date)) {
            date = date.plusDays(1);
        }
        return date;
    }
    
    //위에 꺼는 weekend, holiday 둘다. 밑에 꺼는 weekend만 adjust
    public static LocalDate adjustForWeekend(LocalDate date) {
    	while (isWeekend(date)) {
    		date = date.plusDays(1);
    	}
    	return date;
    }
    
    public static boolean isWeekend(LocalDate date) {
        DayOfWeek day = date.getDayOfWeek();
        return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
    }

    private static boolean isHoliday(LocalDate date) {
        return lunarCalendar.isHoliday(date);
    }
    /*    private static boolean isHoliday(LocalDate date) {
        return holidays.contains(date);
    }*/
    
/*    public static boolean isHoliday(LocalDate date) {
        Holiday currentDay = new Holiday(date.getMonthValue(), date.getDayOfMonth());
        return holidays.contains(currentDay);
    }*/
	//static method로 선언해서 사용. 객체 생성 필요없이 함수처럼 사용하면 된다.
	//StepDown 상품인 경우 여기서 처리한다.
	//data processing을 메소드에서 처리하는 것이 더 낫다.
    public static Map<String, BigDecimal> fetchIDs(DaoService dao) throws Exception {
        Map<String, BigDecimal> ids = new HashMap<>();
        ids.put("cntrID", fetchID(dao, "s_selectOTCSEQCNTRID"));
        ids.put("gdsID", fetchID(dao, "s_selectCNTRGDSID"));
        return ids;
    }
    
    public static BigDecimal fetchID(DaoService dao, String query) throws Exception {
        dao.sqlexe(query, false);
        ListParam param = dao.getNowListParam();
        return (BigDecimal) param.getRow(0)[0];
    }
    
    public static void createAndExecuteListParam(DaoService dao, String[] columns, Object[] values, String listParamName, String sqlCommand) throws Exception {
        ListParam listParam = new ListParam(columns);
        int rowIdx = listParam.createRow();
        
        for (int i = 0; i < columns.length; i++) {
            listParam.setValue(rowIdx, columns[i], values[i]);
        }
        
        dao.setValue(listParamName, listParam);
        dao.sqlexe(sqlCommand, false);
    }

    
	public static String performStepDownInsert(DaoService dao, JSONObject jsonObject) {
		String cntrCode = null;
		try {
			Integer productId = getNullableInteger(jsonObject, "productId");
			String effectiveDate = jsonObject.optString("effectiveDate", null);
			String productType = jsonObject.optString("productType", null);
			Integer earlyRedempCycle = getNullableInteger(jsonObject, "earlyRedempCycle");
			Integer settleDateOffset =  getNullableInteger(jsonObject, "settleDateOffset");
			Integer maturityEvaluationDays = getNullableInteger(jsonObject, "maturityEvaluationDays");
			String underlyingAsset1 = jsonObject.optString("underlyingAsset1", null);
			String underlyingAsset2 = jsonObject.optString("underlyingAsset2", null);
			String underlyingAsset3 = jsonObject.optString("underlyingAsset3", null);
			String exercisePrices = jsonObject.optString("exercisePrices", null);
			Double coupon = getNullableDouble(jsonObject, "coupon");
			Double lizardCoupon = getNullableDouble(jsonObject, "lizardCoupon");
			Integer lossParticipationRate = getNullableInteger(jsonObject, "lossParticipationRate");
			Integer kiBarrier = getNullableInteger(jsonObject, "kiBarrier");
			String calculationCurrency = jsonObject.optString("calculationCurrency", null);
			
			// effectiveDate를 파싱한다. (json파싱하듯이 (String을 jsonArray나 object객체로 변경) (String을 DateTimeFormatter로 바꿔줘야 한다.)
	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	        LocalDate effectiveDateParsed = LocalDate.parse(effectiveDate, formatter);
			//endDate를 계산한다.
			//logic은 다음과 같다.
			//1. effectiveDate로부터 earlyRedempCycle만큼 개월 수를 더해준다.
			//2. exercisePrices를 '-'을 기준으로 쪼갯을 시, 나오는 갯수를 구한다.
			//3. 모든 exerciesDates를 구해준다. list형태나 array형태로 얻어낸다. 계산 시, 주말이나 공휴일이 나오면 다음 날로 밀리고, 구해진 날로부터 earlyRedempCycle만큼 더해주면서
			//모든 exerciesDates를 구한다.
			//4. exerciseDates의 마지막 element를 endDate변수에 할당한다.
			
			
			String[] prices = exercisePrices.split("-");
	        List<LocalDate> exerciseDates = new ArrayList<>();
	        List<LocalDate> adjustedExerciseDates = new ArrayList<>();
	        
	        LocalDate exerciseDate = effectiveDateParsed;  // Start from the effective date
	        //이거 날짜 세아리는 거 과거 방식 코드
	        /*for (int i = 0; i < prices.length; i++) {
	            exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);  // Add the cycle to the previous exercise date
	            exerciseDate = adjustForWeekendAndHolidays(exerciseDate);  // Adjust for weekends and holidays
	            exerciseDates.add(exerciseDate);
	        }*/ 
	        
	        //원 exerciseDate
	        for (String price : prices) {
	        	exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);
	        	exerciseDates.add(exerciseDate);
	        }
	        
	        //휴일 조정된 exerciseDate
	        for (LocalDate date : exerciseDates) {
	            adjustedExerciseDates.add(adjustForWeekend(date));
	        }
	        
	        String endDate = adjustedExerciseDates.get(adjustedExerciseDates.size() - 1).format(formatter);
	        
	        // Logging or using the endDate
	        log.debug(exerciseDates.toString());
			
	        /*dao.sqlexe("s_selectOTCSEQCNTRID", false);
	        ListParam CNTRIDParam = dao.getNowListParam();
	        BigDecimal cntrID = (BigDecimal) CNTRIDParam.getRow(0)[0];
	        
	        dao.sqlexe("s_selectCNTRGDSID", false);
	        ListParam CNTRGDSIDParam = dao.getNowListParam();
	        BigDecimal gdsID = (BigDecimal) CNTRGDSIDParam.getRow(0)[0];*/
	        Map<String, BigDecimal> ids = fetchIDs(dao);
	        BigDecimal cntrID = ids.get("cntrID");
	        BigDecimal gdsID = ids.get("gdsID");
	        
	        cntrCode = "QUOTE" + cntrID.toString(); 
	        
	        
	        /*
			String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
			ListParam listParam1 = new ListParam(columns1);
			
			int rowIdx1 = listParam1.createRow();
			listParam1.setValue(rowIdx1, "GDS_ID", gdsID);
			listParam1.setValue(rowIdx1, "CNTR_HSTR_NO", 1);
			listParam1.setValue(rowIdx1, "SEQ", 1);
			listParam1.setValue(rowIdx1, "CNTR_ID", cntrID);
			listParam1.setValue(rowIdx1, "GDS_TYPE_TP", "STD");
			listParam1.setValue(rowIdx1, "BUY_SELL_TP", "1");
			
            dao.setValue("insertOTCGDSMSTRTp", listParam1);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertGdsMstr", false);
            
            log.debug("insertGdsMstrquerydone");
            */
	        //첫번째 테이블: OTC_GDS_MSTR
	        String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
	        Object[] values1 = {gdsID, 1, 1, cntrID, "STD", "1"};
	        createAndExecuteListParam(dao, columns1, values1, "insertOTCGDSMSTRTp", "s_insertGdsMstr");
	        log.debug("insertGdsMstrquerydone");
	        
            //두번째 테이블: OTC_CNTR_MSTR
            /*String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
            		"BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
			ListParam listParam2 = new ListParam(columns2);
			
			int rowIdx2 = listParam2.createRow();
			listParam2.setValue(rowIdx2, "CNTR_ID", cntrID);
            listParam2.setValue(rowIdx2, "CNTR_HSTR_NO", 1);
            listParam2.setValue(rowIdx2, "ISIN_CODE", "1");
            listParam2.setValue(rowIdx2, "CNTR_CODE", cntrCode);
            listParam2.setValue(rowIdx2, "CNTR_TYPE_TP", "ELS");
            listParam2.setValue(rowIdx2, "GDS_TMPL_TP", kiBarrier != null ? "001" : "007");
            listParam2.setValue(rowIdx2, "DEAL_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "AVLB_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "END_DT", endDate);
            listParam2.setValue(rowIdx2, "BUY_SELL_TP", "1");
            listParam2.setValue(rowIdx2, "NMNL_AMT", 30000000);
            listParam2.setValue(rowIdx2, "NMNL_AMT_CRNC_CODE", calculationCurrency);
            listParam2.setValue(rowIdx2, "FV_LEVL_TP", "3");
            listParam2.setValue(rowIdx2, "INSD_OTSD_EVLT_TP", "I");
            listParam2.setValue(rowIdx2, "BASEP_DTRM_DT", effectiveDate);
            
            dao.setValue("insertOTCCNTRMSTR", listParam2);
            
            dao.sqlexe("s_insertOTCCNTRMSTR", false);
            */
	        //두번째 테이블: OTC_CNTR_MSTR
	        String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
	                "BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
	        Object[] values2 = {cntrID, 1, "1", cntrCode, "ELS", (kiBarrier != null ? "001" : "007"), effectiveDate, effectiveDate, endDate, "1", 30000000, calculationCurrency, "3", "I", effectiveDate};
	        createAndExecuteListParam(dao, columns2, values2, "insertOTCCNTRMSTR", "s_insertOTCCNTRMSTR");
	        log.debug("insertOTCCNTRMSTR");
            
			//세번째 테이블: OTC_LEG_MSTR
			String[] columns3 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO"};
            ListParam listParam3 = new ListParam(columns3);
            
            int rowIdx3 = listParam3.createRow();
            listParam3.setValue(rowIdx3, "GDS_ID", gdsID);
            listParam3.setValue(rowIdx3, "CNTR_HSTR_NO", 1);
            listParam3.setValue(rowIdx3, "LEG_NO", 0);
            
            dao.setValue("insertOTCLEGMSTR", listParam3);
            
            dao.sqlexe("s_insertOTCLEGMSTR", false);
            
            log.debug("insertOTCLEGMSTRdone");
            
            //네번째 테이블: OTC_CNTR_UNAS_PRTC
            String[] columns4 = {"CNTR_ID", "CNTR_HSTR_NO", "SEQ", "UNAS_ID", "LEG_NO"};
            ListParam listParam4 = new ListParam(columns4);
            
            String[] underlyingAssets= {underlyingAsset1, underlyingAsset2, underlyingAsset3};
            
            int seq = 1;
            for (String asset: underlyingAssets) {
            	if (asset != null) {
            		int rowIdx4 = listParam4.createRow();
            		listParam4.setValue(rowIdx4, "CNTR_ID", cntrID);
            		listParam4.setValue(rowIdx4, "CNTR_HSTR_NO", 1);
            		listParam4.setValue(rowIdx4, "SEQ", seq++);
            		listParam4.setValue(rowIdx4, "UNAS_ID", asset);
            		listParam4.setValue(rowIdx4, "LEG_NO", 0);
            	}
            }
            
            dao.setValue("insertOTCCNTRUNASPRTC", listParam4);
            
            dao.sqlexe("s_insertOTCCNTRUNASPRTC", false);
            
            log.debug("insertOTCCNTRUNASPRTC done");
            
            //5번째 테이블: OTC_EXEC_MSTR
            
            String[] columns5 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "YY_CPN_RT",
            		"DUMY_CPN_RT", "LOSS_PART_RT"};
            ListParam listParam5 = new ListParam(columns5);
            
            int rowIdx5 = listParam5.createRow();
            listParam5.setValue(rowIdx5, "GDS_ID", gdsID);
            listParam5.setValue(rowIdx5, "CNTR_HSTR_NO", 1);
            listParam5.setValue(rowIdx5, "LEG_NO", 0);
            listParam5.setValue(rowIdx5, "EXEC_TP", "A");
            listParam5.setValue(rowIdx5, "EXEC_GDS_NO", "1");
            listParam5.setValue(rowIdx5, "SRC_COND_TP", "W");
            listParam5.setValue(rowIdx5, "COND_RANGE_TP", "IO");
            listParam5.setValue(rowIdx5, "YY_CPN_RT", coupon/100);
            
            double dummyCouponRate = (kiBarrier != null) ? (coupon / 100.0 * earlyRedempCycle * prices.length / 12.0) : 0;
            
            listParam5.setValue(rowIdx5, "DUMY_CPN_RT", dummyCouponRate);
            listParam5.setValue(rowIdx5, "LOSS_PART_RT", lossParticipationRate);
            
            dao.setValue("insertOTCEXECMSTR", listParam5);
            
            dao.sqlexe("s_insertOTCEXECMSTR", false);
            
            log.debug("insertOTCEXECMSTR done");
            
            //6번째 테이블: OTC_EXEC_SCHD_PRTC
            
            String[] columns6 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SQNC", "EVLT_DT", "ACTP_RT", "CPN_RT", "SETL_DT"};
            ListParam listParam6 = new ListParam(columns6);
            int sqnc = 1;
            for (int i = 0; i < prices.length; i++) {
            	int rowIdx6 = listParam6.createRow();
            	listParam6.setValue(rowIdx6, "GDS_ID", gdsID);
            	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
            	listParam6.setValue(rowIdx6, "LEG_NO", 0);
            	listParam6.setValue(rowIdx6, "EXEC_TP", "A");
            	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "1");
            	listParam6.setValue(rowIdx6, "SQNC", sqnc++);
            	listParam6.setValue(rowIdx6, "EVLT_DT", adjustedExerciseDates.get(i).format(formatter));
            	listParam6.setValue(rowIdx6, "ACTP_RT", Double.parseDouble(prices[i]));
            	listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*(i+1)/2.0);
            	LocalDate initialDate = adjustedExerciseDates.get(i);
            	LocalDate adjustedDate = initialDate.plusDays(settleDateOffset);
            	adjustedDate = adjustForWeekend(adjustedDate);
            	
            	listParam6.setValue(rowIdx6, "SETL_DT", adjustedDate.format(formatter));
            }
            
            dao.setValue("insertOTCEXECSCHDPRTC", listParam6);
            
            dao.sqlexe("s_insertOTCEXECSCHDPRTC", false);
            
            log.debug("insertOTCEXECSCHDPRTC done");
            
            //7번째, 8번째 테이블
            if (kiBarrier != null) {
            	String[] columns7 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "OBRA_PRIC_TYPE_TP"};
            	ListParam listParam7= new ListParam(columns7);
            	
            	int rowIdx7 = listParam7.createRow();
            	listParam7.setValue(rowIdx7, "GDS_ID", gdsID);
            	listParam7.setValue(rowIdx7, "CNTR_HSTR_NO", 1);
            	listParam7.setValue(rowIdx7, "LEG_NO", 0);
            	listParam7.setValue(rowIdx7, "BRR_TP", "KI");
            	listParam7.setValue(rowIdx7, "BRR_GDS_NO","1");
            	listParam7.setValue(rowIdx7, "SRC_COND_TP", "W");
            	listParam7.setValue(rowIdx7, "COND_RANGE_TP", "OI");
            	listParam7.setValue(rowIdx7, "OBRA_PRIC_TYPE_TP", "CP");
            	
            	dao.setValue("insertOTCBRRMSTR", listParam7);
            	
            	dao.sqlexe("s_insertOTCBRRMSTR", false);
            	
            	log.debug("insertOTCBRRMSTR done");
            	
            	String[] columns8 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
            			"OBRA_END_DT", "BRR_RT"};
            	ListParam listParam8 = new ListParam(columns8);
            	
            	int rowIdx8 = listParam8.createRow();
            	listParam8.setValue(rowIdx8, "GDS_ID", gdsID);
            	listParam8.setValue(rowIdx8, "CNTR_HSTR_NO", 1);
            	listParam8.setValue(rowIdx8, "LEG_NO", 0);
            	listParam8.setValue(rowIdx8, "BRR_TP", "KI");
            	listParam8.setValue(rowIdx8, "BRR_GDS_NO", "1");
            	listParam8.setValue(rowIdx8, "SQNC", 1);
            	listParam8.setValue(rowIdx8, "OBRA_STRT_DT", effectiveDate);
            	listParam8.setValue(rowIdx8, "OBRA_END_DT", endDate);
            	listParam8.setValue(rowIdx8, "BRR_RT", kiBarrier);
            	
            	dao.setValue("insertOTCBRRSCHDPRTC", listParam8);
            	
            	dao.sqlexe("s_insertOTCBRRSCHDPRTC", false);
            	
            	log.debug("insertOTCBRRSCHDPRTC done");
            } else {
            	//No operation(NOP)
            }
            
            dao.commit();
            log.debug("Transaction committed successfully.");
            
		} catch (Exception e) {
			dao.rollback();
			
			e.printStackTrace();
			log.error("Error performing database operation, rollback initiated.", e);
		}
		return cntrCode;
	}
	
	
	public static String performLizardInsert(DaoService dao, JSONObject jsonObject) {
		String cntrCode = null;
		try {
			Integer productId = getNullableInteger(jsonObject, "productId");
			String effectiveDate = jsonObject.optString("effectiveDate", null);
			String productType = jsonObject.optString("productType", null);
			Integer earlyRedempCycle = getNullableInteger(jsonObject, "earlyRedempCycle");
			Integer settleDateOffset =  getNullableInteger(jsonObject, "settleDateOffset");
			Integer maturityEvaluationDays = getNullableInteger(jsonObject, "maturityEvaluationDays");
			String underlyingAsset1 = jsonObject.optString("underlyingAsset1", null);
			String underlyingAsset2 = jsonObject.optString("underlyingAsset2", null);
			String underlyingAsset3 = jsonObject.optString("underlyingAsset3", null);
			String exercisePrices = jsonObject.optString("exercisePrices", null);
			Double coupon = getNullableDouble(jsonObject, "coupon");
			Double lizardCoupon = getNullableDouble(jsonObject, "lizardCoupon");
			Integer lossParticipationRate = getNullableInteger(jsonObject, "lossParticipationRate");
			Integer kiBarrier = getNullableInteger(jsonObject, "kiBarrier");
			String calculationCurrency = jsonObject.optString("calculationCurrency", null);
			
			if (exercisePrices == null) {
				throw new IllegalArgumentException("exercisePrices is null");
			}
			
			String[] priceComponents = exercisePrices.split("-");
			List<String> mainPrices = new ArrayList<>();
			List<String> barrierPrices = new ArrayList<>();
			List<Integer> barrierLocations = new ArrayList<>();
			for (int i = 0; i < priceComponents.length; i++) {
				String component = priceComponents[i];
				if (component.contains("(")) {
					String mainPrice = component.substring(0, component.indexOf("("));
					String barrierPrice = component.substring(component.indexOf("(") + 1, component.indexOf(")"));
					
					mainPrices.add(mainPrice);
					barrierPrices.add(barrierPrice);
					barrierLocations.add(i);
							
				} else {
					mainPrices.add(component);
				}
			}
			//lizard회차()로 표시된, 회차(몇 회차인지)도 array이든지, arraylist형태의 목록으로 뽑아내야 한다.
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	        LocalDate effectiveDateParsed = LocalDate.parse(effectiveDate, formatter);
	        
	        List<LocalDate> exerciseDates = new ArrayList<>();
	        List<LocalDate> adjustedExerciseDates = new ArrayList<>();
	        
	        LocalDate exerciseDate = effectiveDateParsed;  // Start from the effective date
	        //이거 날짜 세아리는 거 과거 방식 코드
	        /*for (int i = 0; i < prices.length; i++) {
	            exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);  // Add the cycle to the previous exercise date
	            exerciseDate = adjustForWeekendAndHolidays(exerciseDate);  // Adjust for weekends and holidays
	            exerciseDates.add(exerciseDate);
	        }*/ 
	        
	        //원 exerciseDate
	        for (String price : priceComponents) {
	        	exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);
	        	exerciseDates.add(exerciseDate);
	        }
	        
	        //조정된 exerciseDate
	        for (LocalDate date : exerciseDates) {
	            adjustedExerciseDates.add(adjustForWeekend(date));
	        }
	        
	        String endDate = adjustedExerciseDates.get(adjustedExerciseDates.size() - 1).format(formatter);
	        
	        // Logging or using the endDate
	        log.debug(adjustedExerciseDates.toString());
	        	        
	        /*List<LocalDate> exerciseDates = new ArrayList<>();

	        LocalDate exerciseDate = effectiveDateParsed;  // Start from the effective date
	        for (int i = 0; i < priceComponents.length; i++) {
	            exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);  // Add the cycle to the previous exercise date
	            exerciseDate = adjustForWeekendAndHolidays(exerciseDate);  // Adjust for weekends and holidays
	            exerciseDates.add(exerciseDate);
	        }
	        
	        String endDate = exerciseDates.get(exerciseDates.size() - 1).format(formatter);
	        */
	        
	        Map<String, BigDecimal> ids = fetchIDs(dao);
	        BigDecimal cntrID = ids.get("cntrID");
	        BigDecimal gdsID = ids.get("gdsID");
	        
	        cntrCode = "QUOTE" + cntrID.toString();
	        
	        // Logging or using the endDate
	        log.debug(exerciseDates.toString());
			//첫번째 테이블: OTC_GDS_MSTR
			String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
			ListParam listParam1 = new ListParam(columns1);
			
			int rowIdx1 = listParam1.createRow();
			listParam1.setValue(rowIdx1, "GDS_ID", gdsID);
			listParam1.setValue(rowIdx1, "CNTR_HSTR_NO", 1);
			listParam1.setValue(rowIdx1, "SEQ", 1);
			listParam1.setValue(rowIdx1, "CNTR_ID", cntrID);
			listParam1.setValue(rowIdx1, "GDS_TYPE_TP", "STD");
			listParam1.setValue(rowIdx1, "BUY_SELL_TP", "1");
			
            dao.setValue("insertOTCGDSMSTRTp", listParam1);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertGdsMstr", false);
            
            log.debug("insertGdsMstrquerydone");
            
            //두번째 테이블: OTC_CNTR_MSTR
            String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
            		"BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
			ListParam listParam2 = new ListParam(columns2);
			
			int rowIdx2 = listParam2.createRow();
			listParam2.setValue(rowIdx2, "CNTR_ID", cntrID);
            listParam2.setValue(rowIdx2, "CNTR_HSTR_NO", 1);
            listParam2.setValue(rowIdx2, "ISIN_CODE", "1");
            listParam2.setValue(rowIdx2, "CNTR_CODE", cntrCode);
            listParam2.setValue(rowIdx2, "CNTR_TYPE_TP", "ELS");
            listParam2.setValue(rowIdx2, "GDS_TMPL_TP", kiBarrier != null ? "005" : "011");
            listParam2.setValue(rowIdx2, "DEAL_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "AVLB_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "END_DT", endDate);
            listParam2.setValue(rowIdx2, "BUY_SELL_TP", "1");
            listParam2.setValue(rowIdx2, "NMNL_AMT", 30000000);
            listParam2.setValue(rowIdx2, "NMNL_AMT_CRNC_CODE", calculationCurrency);
            listParam2.setValue(rowIdx2, "FV_LEVL_TP", "3");
            listParam2.setValue(rowIdx2, "INSD_OTSD_EVLT_TP", "I");
            listParam2.setValue(rowIdx2, "BASEP_DTRM_DT", effectiveDate);
            
            dao.setValue("insertOTCCNTRMSTR", listParam2);
            
            dao.sqlexe("s_insertOTCCNTRMSTR", false);
            
            log.debug("insertOTCCNTRMSTR");
			//세번째 테이블: OTC_LEG_MSTR
			String[] columns3 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO"};
            ListParam listParam3 = new ListParam(columns3);
            
            int rowIdx3 = listParam3.createRow();
            listParam3.setValue(rowIdx3, "GDS_ID", gdsID);
            listParam3.setValue(rowIdx3, "CNTR_HSTR_NO", 1);
            listParam3.setValue(rowIdx3, "LEG_NO", 0);
            
            dao.setValue("insertOTCLEGMSTR", listParam3);
            
            dao.sqlexe("s_insertOTCLEGMSTR", false);
            
            log.debug("insertOTCLEGMSTRdone");
            
            //네번째 테이블: OTC_CNTR_UNAS_PRTC
            String[] columns4 = {"CNTR_ID", "CNTR_HSTR_NO", "SEQ", "UNAS_ID", "LEG_NO"};
            ListParam listParam4 = new ListParam(columns4);
            
            String[] underlyingAssets= {underlyingAsset1, underlyingAsset2, underlyingAsset3};
            
            int seq = 1;
            for (String asset: underlyingAssets) {
            	if (asset != null) {
            		int rowIdx4 = listParam4.createRow();
            		listParam4.setValue(rowIdx4, "CNTR_ID", cntrID);
            		listParam4.setValue(rowIdx4, "CNTR_HSTR_NO", 1);
            		listParam4.setValue(rowIdx4, "SEQ", seq++);
            		listParam4.setValue(rowIdx4, "UNAS_ID", asset);
            		listParam4.setValue(rowIdx4, "LEG_NO", 0);
            	}
            }
            
            dao.setValue("insertOTCCNTRUNASPRTC", listParam4);
            
            dao.sqlexe("s_insertOTCCNTRUNASPRTC", false);
            
            log.debug("insertOTCCNTRUNASPRTC done");
            
            //5번째 테이블: OTC_EXEC_MSTR
            
            String[] columns5 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "YY_CPN_RT",
            		"DUMY_CPN_RT", "LOSS_PART_RT"};
            ListParam listParam5 = new ListParam(columns5);
            
            int rowIdx5 = listParam5.createRow();
            listParam5.setValue(rowIdx5, "GDS_ID", gdsID);
            listParam5.setValue(rowIdx5, "CNTR_HSTR_NO", 1);
            listParam5.setValue(rowIdx5, "LEG_NO", 0);
            listParam5.setValue(rowIdx5, "EXEC_TP", "A");
            listParam5.setValue(rowIdx5, "EXEC_GDS_NO", "1");
            listParam5.setValue(rowIdx5, "SRC_COND_TP", "W");
            listParam5.setValue(rowIdx5, "COND_RANGE_TP", "IO");
            listParam5.setValue(rowIdx5, "YY_CPN_RT", coupon/100);
            
            double dummyCouponRate = (kiBarrier != null) ? (coupon / 100.0 * earlyRedempCycle * mainPrices.size() / 12.0) : 0;
            
            listParam5.setValue(rowIdx5, "DUMY_CPN_RT", dummyCouponRate);
            listParam5.setValue(rowIdx5, "LOSS_PART_RT", lossParticipationRate);
            
            dao.setValue("insertOTCEXECMSTR", listParam5);
            
            dao.sqlexe("s_insertOTCEXECMSTR", false);
            
            log.debug("insertOTCEXECMSTR done");
            
            //6번째 테이블: OTC_EXEC_SCHD_PRTC
            
            String[] columns6 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SQNC", "EVLT_DT", "ACTP_RT", "CPN_RT", "SETL_DT"};
            ListParam listParam6 = new ListParam(columns6);
            int sqnc = 1;
            for (int i = 0; i < mainPrices.size(); i++) { //Price(barrier)의 arraylist를 사용하기 때문에, length에서 size()로 바꾼다. length는 array의 길이 return. size()는 arrayList의 길이 return.
            	int rowIdx6 = listParam6.createRow();
            	listParam6.setValue(rowIdx6, "GDS_ID", gdsID);
            	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
            	listParam6.setValue(rowIdx6, "LEG_NO", 0);
            	listParam6.setValue(rowIdx6, "EXEC_TP", "A");
            	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "1");
            	listParam6.setValue(rowIdx6, "SQNC", sqnc++);
            	listParam6.setValue(rowIdx6, "EVLT_DT", adjustedExerciseDates.get(i).format(formatter));
            	listParam6.setValue(rowIdx6, "ACTP_RT", Double.parseDouble(mainPrices.get(i))); //[i] 대신 get메소드 이용, [i]는 array의 객체 return, get(i)는 arrayList의 i번째 element return
            	listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*(i+1)/2.0);
            	LocalDate initialDate = adjustedExerciseDates.get(i);
            	LocalDate adjustedDate = initialDate.plusDays(settleDateOffset);
            	adjustedDate = adjustForWeekend(adjustedDate);
            	
            	listParam6.setValue(rowIdx6, "SETL_DT", adjustedDate.format(formatter));
            }
            
            dao.setValue("insertOTCEXECSCHDPRTC", listParam6);
            
            dao.sqlexe("s_insertOTCEXECSCHDPRTC", false);
            
            log.debug("insertOTCEXECSCHDPRTC done");
            
            //7번째, 8번째 테이블
            if (kiBarrier != null) {
            	String[] columns7 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "OBRA_PRIC_TYPE_TP"};
            	ListParam listParam7= new ListParam(columns7);
            	
            	int rowIdx7 = listParam7.createRow();
            	listParam7.setValue(rowIdx7, "GDS_ID", gdsID);
            	listParam7.setValue(rowIdx7, "CNTR_HSTR_NO", 1);
            	listParam7.setValue(rowIdx7, "LEG_NO", 0);
            	listParam7.setValue(rowIdx7, "BRR_TP", "KI");
            	listParam7.setValue(rowIdx7, "BRR_GDS_NO","1");
            	listParam7.setValue(rowIdx7, "SRC_COND_TP", "W");
            	listParam7.setValue(rowIdx7, "COND_RANGE_TP", "OI");
            	listParam7.setValue(rowIdx7, "OBRA_PRIC_TYPE_TP", "CP");
            	
            	dao.setValue("insertOTCBRRMSTR", listParam7);
            	
            	dao.sqlexe("s_insertOTCBRRMSTR", false);
            	
            	log.debug("insertOTCBRRMSTR done");
            	
            	String[] columns8 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
            			"OBRA_END_DT", "BRR_RT"};
            	ListParam listParam8 = new ListParam(columns8);
            	
            	int rowIdx8 = listParam8.createRow();
            	listParam8.setValue(rowIdx8, "GDS_ID", gdsID);
            	listParam8.setValue(rowIdx8, "CNTR_HSTR_NO", 1);
            	listParam8.setValue(rowIdx8, "LEG_NO", 0);
            	listParam8.setValue(rowIdx8, "BRR_TP", "KI");
            	listParam8.setValue(rowIdx8, "BRR_GDS_NO", "1");
            	listParam8.setValue(rowIdx8, "SQNC", 1);
            	listParam8.setValue(rowIdx8, "OBRA_STRT_DT", effectiveDate);
            	listParam8.setValue(rowIdx8, "OBRA_END_DT", endDate);
            	listParam8.setValue(rowIdx8, "BRR_RT", kiBarrier);
            	
            	dao.setValue("insertOTCBRRSCHDPRTC", listParam8);
            	
            	dao.sqlexe("s_insertOTCBRRSCHDPRTC", false);
            	
            	log.debug("insertOTCBRRSCHDPRTC done");
            }
            
            //OTC_BRR_MSTR (Lizard인 경우) (9번째 테이블의 경우) (Lizard의 경우를 수용하도록 수정할 필요가 있음)
            String[] columns9 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "OBRA_PRIC_TYPE_TP"};
        	ListParam listParam9= new ListParam(columns9);
        	
        	int rowIdx9 = listParam9.createRow();
        	listParam9.setValue(rowIdx9, "GDS_ID", gdsID);
        	listParam9.setValue(rowIdx9, "CNTR_HSTR_NO", 1);
        	listParam9.setValue(rowIdx9, "LEG_NO", 0);
        	listParam9.setValue(rowIdx9, "BRR_TP", "LZ");
        	listParam9.setValue(rowIdx9, "BRR_GDS_NO","1");
        	listParam9.setValue(rowIdx9, "SRC_COND_TP", "W");
        	listParam9.setValue(rowIdx9, "COND_RANGE_TP", "OI");
        	listParam9.setValue(rowIdx9, "OBRA_PRIC_TYPE_TP", "CP");
        	
        	dao.setValue("insertOTCBRRMSTR", listParam9);
        	
        	dao.sqlexe("s_insertOTCBRRMSTR", false);
        	
        	log.debug("insertOTCBRRMSTR2nd done");
        	
        	//OTC_BRR_SCHD_PRTC (Lizard인 경우) (10번째 테이블의 경우)
        	String[] columns10 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
        			"OBRA_END_DT", "BRR_RT", "CPN_RT"};
        	ListParam listParam10 = new ListParam(columns10);
        	
        	// Formatter for the new format
        	DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyyMMdd");
        	
        	int sqnc2 = 1;
        	for (int i = 0; i < barrierPrices.size(); i++) {
        		int rowIdx10 = listParam10.createRow();
	        	listParam10.setValue(rowIdx10, "GDS_ID", gdsID);
	        	listParam10.setValue(rowIdx10, "CNTR_HSTR_NO", 1);
	        	listParam10.setValue(rowIdx10, "LEG_NO", 0);
	        	listParam10.setValue(rowIdx10, "BRR_TP", "LZ");
	        	listParam10.setValue(rowIdx10, "BRR_GDS_NO", "1");
	        	listParam10.setValue(rowIdx10, "SQNC", sqnc2++);
	        	listParam10.setValue(rowIdx10, "OBRA_STRT_DT", effectiveDate);
	        	//OBRA_END_DT 데이터 포맷 상의 문제가 있다. 수정해줘야 한다.
	        	listParam10.setValue(rowIdx10, "OBRA_END_DT", adjustedExerciseDates.get(barrierLocations.get(i)).format(formatter)); // Format the date from exerciseDates
	        	listParam10.setValue(rowIdx10, "BRR_RT", Double.parseDouble(barrierPrices.get(i))/100);
	        	listParam10.setValue(rowIdx10, "CPN_RT", ((barrierLocations.get(i) + 1) * 6 / 12.0) * (lizardCoupon / 100.0));
        	}
        	//CPN_RT의 추가로 다른 sql query를 사용하던지, sql query를 CPN_RT가 있을 때와 없을 때 모두 수용가능 하도록 작성할 필요가 있다.
        	dao.setValue("insertOTCBRRSCHDPRTC2", listParam10);
        	
        	dao.sqlexe("s_insertOTCBRRSCHDPRTC2", false);
        	
        	log.debug("insertOTCBRRSCHDPRTC2nd done");
		
        	 dao.commit();
             log.debug("Transaction committed successfully.");
             
 		} catch (Exception e) {
 			dao.rollback();
 			
 			e.printStackTrace();
 			log.error("Error performing database operation, rollback initiated.", e);
		}
		return cntrCode;
	}
	//KnockOut option insert param construction
	//5번째 테이블, 6번째 테이블 query만 수정해주면 된다.
	public static String performKnockOutInsert(DaoService dao, JSONObject jsonObject) {
		String cntrCode = null;
		try {
			Integer productId = getNullableInteger(jsonObject, "productId");
			String effectiveDate = jsonObject.optString("effectiveDate", null);
			String productType = jsonObject.optString("productType", null);
			Integer earlyRedempCycle = getNullableInteger(jsonObject, "earlyRedempCycle");
			Integer settleDateOffset =  getNullableInteger(jsonObject, "settleDateOffset");
			Integer maturityEvaluationDays = getNullableInteger(jsonObject, "maturityEvaluationDays");
			String underlyingAsset1 = jsonObject.optString("underlyingAsset1", null);
			String underlyingAsset2 = jsonObject.optString("underlyingAsset2", null);
			String underlyingAsset3 = jsonObject.optString("underlyingAsset3", null);
			//String exercisePrices = jsonObject.optString("exercisePrices", null);
			//Double coupon = getNullableDouble(jsonObject, "coupon");
			//Double lizardCoupon = getNullableDouble(jsonObject, "lizardCoupon");
			//Integer lossParticipationRate = getNullableInteger(jsonObject, "lossParticipationRate");
			//Integer kiBarrier = getNullableInteger(jsonObject, "kiBarrier");
			String calculationCurrency = jsonObject.optString("calculationCurrency", null);
			Integer principalProtectedRate = getNullableInteger(jsonObject, "principalProtectedRate");
			Integer callBarrier = getNullableInteger(jsonObject, "callBarrier");
			Integer callParticipationRate = getNullableInteger(jsonObject, "callParticipationRate");
			Integer koBarrierUpSide = getNullableInteger(jsonObject, "koBarrierUpSide");
			Integer dummyCouponUpSide = getNullableInteger(jsonObject, "dummyCouponUpSide");
			
	        // effectiveDate 파싱
	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	        LocalDate effectiveDateParsed = LocalDate.parse(effectiveDate, formatter);
	        
	        // 조기 상환 주기 개월 수를 더해줌으로써 end date를 계산하고, 주말 조정을 해준다.
	        LocalDate endDate = adjustForWeekend(effectiveDateParsed.plusMonths(earlyRedempCycle));
	        
	        LocalDate datePlusTwoDays = endDate.plusDays(2);

	        // datePlusTwoDays를 주말 조정을 해준다.
	        LocalDate adjustedDatePlusTwoDays = adjustForWeekend(datePlusTwoDays);
	        
	        //cntrID, gdsID, cntrCode값을 부여 받는다.
	        Map<String, BigDecimal> ids = fetchIDs(dao);
	        BigDecimal cntrID = ids.get("cntrID");
	        BigDecimal gdsID = ids.get("gdsID");
	        
	        cntrCode = "QUOTE" + cntrID.toString();
	        
			//첫번째 테이블: OTC_GDS_MSTR
			String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
			ListParam listParam1 = new ListParam(columns1);
			
			int rowIdx1 = listParam1.createRow();
			listParam1.setValue(rowIdx1, "GDS_ID", gdsID);
			listParam1.setValue(rowIdx1, "CNTR_HSTR_NO", 1);
			listParam1.setValue(rowIdx1, "SEQ", 1);
			listParam1.setValue(rowIdx1, "CNTR_ID", cntrID);
			listParam1.setValue(rowIdx1, "GDS_TYPE_TP", "VKO"); //이거 VKO(Knock Out으로 수정)
			listParam1.setValue(rowIdx1, "BUY_SELL_TP", "1");
			
            dao.setValue("insertOTCGDSMSTRTp", listParam1);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertGdsMstr", false);
            
            log.debug("insertGdsMstrquerydone");
            
            //두번째 테이블: OTC_CNTR_MSTR
            String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
            		"BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
			ListParam listParam2 = new ListParam(columns2);
			
			int rowIdx2 = listParam2.createRow();
			listParam2.setValue(rowIdx2, "CNTR_ID", cntrID);
            listParam2.setValue(rowIdx2, "CNTR_HSTR_NO", 1);
            listParam2.setValue(rowIdx2, "ISIN_CODE", "1");
            listParam2.setValue(rowIdx2, "CNTR_CODE", cntrCode);
            listParam2.setValue(rowIdx2, "CNTR_TYPE_TP", "ELS");
            listParam2.setValue(rowIdx2, "GDS_TMPL_TP", "059"); //KnockOut 번호로 수정
            listParam2.setValue(rowIdx2, "DEAL_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "AVLB_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "END_DT", endDate.format(formatter)); //endDate는 effectiveDate로 부터 12개월 뒤, 12개월 뒤가 휴일일 시, 휴일에서 밀려서 workingday로 지정됨.
            listParam2.setValue(rowIdx2, "BUY_SELL_TP", "1");
            listParam2.setValue(rowIdx2, "NMNL_AMT", 30000000);
            listParam2.setValue(rowIdx2, "NMNL_AMT_CRNC_CODE", calculationCurrency);
            listParam2.setValue(rowIdx2, "FV_LEVL_TP", "3");
            listParam2.setValue(rowIdx2, "INSD_OTSD_EVLT_TP", "I");
            listParam2.setValue(rowIdx2, "BASEP_DTRM_DT", effectiveDate);
            
            dao.setValue("insertOTCCNTRMSTR", listParam2);
            
            dao.sqlexe("s_insertOTCCNTRMSTR", false);
            
            log.debug("insertOTCCNTRMSTR");
            
            //세번째 테이블: OTC_LEG_MSTR
			String[] columns3 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO"};
            ListParam listParam3 = new ListParam(columns3);
            
            int rowIdx3 = listParam3.createRow();
            listParam3.setValue(rowIdx3, "GDS_ID", gdsID);
            listParam3.setValue(rowIdx3, "CNTR_HSTR_NO", 1);
            listParam3.setValue(rowIdx3, "LEG_NO", 0);
            
            dao.setValue("insertOTCLEGMSTR", listParam3);
            
            dao.sqlexe("s_insertOTCLEGMSTR", false);
            
            log.debug("insertOTCLEGMSTRdone");
            
          //네번째 테이블: OTC_CNTR_UNAS_PRTC
            String[] columns4 = {"CNTR_ID", "CNTR_HSTR_NO", "SEQ", "UNAS_ID", "LEG_NO"};
            ListParam listParam4 = new ListParam(columns4);
            
            String[] underlyingAssets= {underlyingAsset1, underlyingAsset2, underlyingAsset3};
            
            int seq = 1;
            for (String asset: underlyingAssets) {
            	if (asset != null) {
            		int rowIdx4 = listParam4.createRow();
            		listParam4.setValue(rowIdx4, "CNTR_ID", cntrID);
            		listParam4.setValue(rowIdx4, "CNTR_HSTR_NO", 1);
            		listParam4.setValue(rowIdx4, "SEQ", seq++);
            		listParam4.setValue(rowIdx4, "UNAS_ID", asset);
            		listParam4.setValue(rowIdx4, "LEG_NO", 0);
            	}
            }
            
            dao.setValue("insertOTCCNTRUNASPRTC", listParam4);
            
            dao.sqlexe("s_insertOTCCNTRUNASPRTC", false);
            
            log.debug("insertOTCCNTRUNASPRTC done");
            
        	//5번째 테이블: OTC_EXEC_MSTR, 이거 column수정이 있어서, null값을 column에 넣어주거나, query를 수정할 필요가 있다.
            //query를 수정해야 한다. 새로운 column, UP_PART_RT가 들어옴. 이거 upsert query로 처리해주어야 한다.
            
            String[] columns5 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "UP_PART_RT"};
            ListParam listParam5 = new ListParam(columns5);
            
            //Double callParticipationRateDouble = callParticipationRate != null ? callParticipationRate / 100.0 : null;
            
            int rowIdx5 = listParam5.createRow();
            listParam5.setValue(rowIdx5, "GDS_ID", gdsID);
            listParam5.setValue(rowIdx5, "CNTR_HSTR_NO", 1);
            listParam5.setValue(rowIdx5, "LEG_NO", 0);
            listParam5.setValue(rowIdx5, "EXEC_TP", "V");
            listParam5.setValue(rowIdx5, "EXEC_GDS_NO", "1");
            listParam5.setValue(rowIdx5, "SRC_COND_TP", "W");
            listParam5.setValue(rowIdx5, "COND_RANGE_TP", "IO");
            //이거 단위가 100단위로 들어가는 것 같은데, (1부터 해서 소수점으로 들어가는 것이 아님) 확인이 필요하다.
            listParam5.setValue(rowIdx5, "UP_PART_RT", callParticipationRate);
            
            dao.setValue("insertOTCEXECMSTR2", listParam5);
            
            dao.sqlexe("s_insertOTCEXECMSTR2", false);
            
            log.debug("insertOTCEXECMSTR2 done");
            
            //6번째 테이블: OTC_EXEC_SCHD_PRTC
            //이거 앞에 StepDown ELS와는 다르게 loop필요없이 한 값만 넣어주면 된다. (knockout의 경우, twoway knockout의 경우에는 2개의 값 넣어주기) 
            //CPN_RT가 빠졌기 때문에, SQL query를 따로 작성해 주어야 한다.
            String[] columns6 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SQNC", "EVLT_DT", "ACTP_RT", "SETL_DT"};
            ListParam listParam6 = new ListParam(columns6);
            //int sqnc = 1;
            //for (int i = 0; i < mainPrices.size(); i++) { //Price(barrier)의 arraylist를 사용하기 때문에, length에서 size()로 바꾼다. length는 array의 길이 return. size()는 arrayList의 길이 return.
        	int rowIdx6 = listParam6.createRow();
        	listParam6.setValue(rowIdx6, "GDS_ID", gdsID);
        	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
        	listParam6.setValue(rowIdx6, "LEG_NO", 0);
        	listParam6.setValue(rowIdx6, "EXEC_TP", "V");
        	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "1");
        	listParam6.setValue(rowIdx6, "SQNC", 1);
        	listParam6.setValue(rowIdx6, "EVLT_DT", endDate.format(formatter));
        	
        	listParam6.setValue(rowIdx6, "ACTP_RT", callBarrier/100.0); //callBarrier가 행사가율로 생각되어 진다.
        	//listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*(i+1)/2.0);
        	//LocalDate initialDate = exerciseDates.get(i);
        	LocalDate endDatePlusTwoDays = endDate.plusDays(2);
        	LocalDate adjustedEndDatePlusTwoDays = adjustForWeekend(endDatePlusTwoDays);
        	
        	listParam6.setValue(rowIdx6, "SETL_DT", adjustedEndDatePlusTwoDays.format(formatter));
            //}
        	        	
            dao.setValue("insertOTCEXECSCHDPRTC2", listParam6);
            
            dao.sqlexe("s_insertOTCEXECSCHDPRTC2", false);
            
            log.debug("insertOTCEXECSCHDPRTC2 done");
            
            //7번째 테이블: OTC_BRR_MSTR
            String[] columns7 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "OBRA_PRIC_TYPE_TP"};
        	ListParam listParam7= new ListParam(columns7);
        	
        	int rowIdx7 = listParam7.createRow();
        	listParam7.setValue(rowIdx7, "GDS_ID", gdsID);
        	listParam7.setValue(rowIdx7, "CNTR_HSTR_NO", 1);
        	listParam7.setValue(rowIdx7, "LEG_NO", 0);
        	listParam7.setValue(rowIdx7, "BRR_TP", "KO");
        	listParam7.setValue(rowIdx7, "BRR_GDS_NO","1");
        	listParam7.setValue(rowIdx7, "SRC_COND_TP", "W");
        	listParam7.setValue(rowIdx7, "COND_RANGE_TP", "OI");
        	listParam7.setValue(rowIdx7, "OBRA_PRIC_TYPE_TP", "CP");
        	
        	dao.setValue("insertOTCBRRMSTR", listParam7);
        	
        	dao.sqlexe("s_insertOTCBRRMSTR", false);
        	
        	log.debug("insertOTCBRRMSTR done");
        	
        	//8번째 테이블: OTC_BRR_SCHD_PRTC
        	String[] columns8 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
        			"OBRA_END_DT", "BRR_RT"};
        	ListParam listParam8 = new ListParam(columns8);
        	
        	int rowIdx8 = listParam8.createRow();
        	listParam8.setValue(rowIdx8, "GDS_ID", gdsID);
        	listParam8.setValue(rowIdx8, "CNTR_HSTR_NO", 1);
        	listParam8.setValue(rowIdx8, "LEG_NO", 0);
        	listParam8.setValue(rowIdx8, "BRR_TP", "KO");
        	listParam8.setValue(rowIdx8, "BRR_GDS_NO", "1");
        	listParam8.setValue(rowIdx8, "SQNC", 1);
        	listParam8.setValue(rowIdx8, "OBRA_STRT_DT", effectiveDate);
        	listParam8.setValue(rowIdx8, "OBRA_END_DT", endDate.format(formatter));
        	listParam8.setValue(rowIdx8, "BRR_RT", koBarrierUpSide/100.0);
        	
        	dao.setValue("insertOTCBRRSCHDPRTC", listParam8);
        	
        	dao.sqlexe("s_insertOTCBRRSCHDPRTC", false);
        	
        	log.debug("insertOTCBRRSCHDPRTC done");
        	
        	
            //오류 없이 제대로 sql문이 실행되었을 때, commit. 오류 발생시는 catch문으로 Exception잡아주고, catch문의 rollback실행해서, 모든 sql operation을 되돌린다.
			dao.commit();
            log.debug("Transaction committed successfully.");
            
		} catch (Exception e) {
			dao.rollback();
 			
 			e.printStackTrace();
 			log.error("Error performing database operation, rollback initiated.", e);
		}
		return cntrCode;
	}
	//4번까지 작성. 5번부터 작성해 주어야 한다.
	public static String performTwoWayKnockOutInsert(DaoService dao, JSONObject jsonObject) {
		String cntrCode = null;
		try {
			Integer productId = getNullableInteger(jsonObject, "productId");
			String effectiveDate = jsonObject.optString("effectiveDate", null);
			String productType = jsonObject.optString("productType", null);
			Integer earlyRedempCycle = getNullableInteger(jsonObject, "earlyRedempCycle");
			Integer settleDateOffset =  getNullableInteger(jsonObject, "settleDateOffset");
			Integer maturityEvaluationDays = getNullableInteger(jsonObject, "maturityEvaluationDays");
			String underlyingAsset1 = jsonObject.optString("underlyingAsset1", null);
			String underlyingAsset2 = jsonObject.optString("underlyingAsset2", null);
			String underlyingAsset3 = jsonObject.optString("underlyingAsset3", null);
			//String exercisePrices = jsonObject.optString("exercisePrices", null);
			//Double coupon = getNullableDouble(jsonObject, "coupon");
			//Double lizardCoupon = getNullableDouble(jsonObject, "lizardCoupon");
			//Integer lossParticipationRate = getNullableInteger(jsonObject, "lossParticipationRate");
			//Integer kiBarrier = getNullableInteger(jsonObject, "kiBarrier");
			String calculationCurrency = jsonObject.optString("calculationCurrency", null);
			Integer principalProtectedRate = getNullableInteger(jsonObject, "principalProtectedRate");
			Integer callBarrier = getNullableInteger(jsonObject, "callBarrier");
			Integer callParticipationRate = getNullableInteger(jsonObject, "callParticipationRate");
			Integer koBarrierUpSide = getNullableInteger(jsonObject, "koBarrierUpSide");
			Integer dummyCouponUpSide = getNullableInteger(jsonObject, "dummyCouponUpSide");
			Integer putBarrier = getNullableInteger(jsonObject, "putBarrier");
			Integer putParticipationRate = getNullableInteger(jsonObject, "putParticipationRate");
			Integer koBarrierDownSide = getNullableInteger(jsonObject, "koBarrierDownSide");
			Integer dummyCouponDownSide = getNullableInteger(jsonObject, "dummyCouponDownSide");
	        // Parsing the effective date
	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	        LocalDate effectiveDateParsed = LocalDate.parse(effectiveDate, formatter);
	        
	        // Calculating the end date by adding early redemption cycle months and adjusting for weekends and holidays
	        LocalDate endDate = adjustForWeekend(effectiveDateParsed.plusMonths(earlyRedempCycle));
	        
	        LocalDate datePlusTwoDays = endDate.plusDays(2);

	        // Adjust the datePlusTwoDays for weekends and holidays
	        LocalDate adjustedDatePlusTwoDays = adjustForWeekend(datePlusTwoDays);
			
	        Map<String, BigDecimal> ids = fetchIDs(dao);
	        BigDecimal cntrID = ids.get("cntrID");
	        BigDecimal gdsID = ids.get("gdsID");
	        
	        cntrCode = "QUOTE" + cntrID.toString();
	        
			//첫번째 테이블: OTC_GDS_MSTR
			String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
			ListParam listParam1 = new ListParam(columns1);
			
			int rowIdx1 = listParam1.createRow();
			listParam1.setValue(rowIdx1, "GDS_ID", gdsID);
			listParam1.setValue(rowIdx1, "CNTR_HSTR_NO", 1);
			listParam1.setValue(rowIdx1, "SEQ", 1);
			listParam1.setValue(rowIdx1, "CNTR_ID", cntrID);
			listParam1.setValue(rowIdx1, "GDS_TYPE_TP", "WAY"); //이거 WAY(TwoWay Knock Out으로 수정)
			listParam1.setValue(rowIdx1, "BUY_SELL_TP", "1");
			
            dao.setValue("insertOTCGDSMSTRTp", listParam1);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertGdsMstr", false);
            
            log.debug("insertGdsMstrquerydone");
            
            //두번째 테이블: OTC_CNTR_MSTR
            String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
            		"BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
			ListParam listParam2 = new ListParam(columns2);
			
			int rowIdx2 = listParam2.createRow();
			listParam2.setValue(rowIdx2, "CNTR_ID", cntrID);
            listParam2.setValue(rowIdx2, "CNTR_HSTR_NO", 1);
            listParam2.setValue(rowIdx2, "ISIN_CODE", "1");
            listParam2.setValue(rowIdx2, "CNTR_CODE", cntrCode);
            listParam2.setValue(rowIdx2, "CNTR_TYPE_TP", "ELS");
            listParam2.setValue(rowIdx2, "GDS_TMPL_TP", "025"); //Twoway KnockOut 번호로 수정
            listParam2.setValue(rowIdx2, "DEAL_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "AVLB_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "END_DT", endDate.format(formatter)); //endDate는 effectiveDate로 부터 12개월 뒤, 12개월 뒤가 휴일일 시, 휴일에서 밀려서 workingday로 지정됨.
            listParam2.setValue(rowIdx2, "BUY_SELL_TP", "1");
            listParam2.setValue(rowIdx2, "NMNL_AMT", 30000000);
            listParam2.setValue(rowIdx2, "NMNL_AMT_CRNC_CODE", calculationCurrency);
            listParam2.setValue(rowIdx2, "FV_LEVL_TP", "3");
            listParam2.setValue(rowIdx2, "INSD_OTSD_EVLT_TP", "I");
            listParam2.setValue(rowIdx2, "BASEP_DTRM_DT", effectiveDate);
            
            dao.setValue("insertOTCCNTRMSTR", listParam2);
            
            dao.sqlexe("s_insertOTCCNTRMSTR", false);
            
            log.debug("insertOTCCNTRMSTR");
            
            //세번째 테이블: OTC_LEG_MSTR
			String[] columns3 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO"};
            ListParam listParam3 = new ListParam(columns3);
            
            int rowIdx3 = listParam3.createRow();
            listParam3.setValue(rowIdx3, "GDS_ID", gdsID);
            listParam3.setValue(rowIdx3, "CNTR_HSTR_NO", 1);
            listParam3.setValue(rowIdx3, "LEG_NO", 0);
            
            dao.setValue("insertOTCLEGMSTR", listParam3);
            
            dao.sqlexe("s_insertOTCLEGMSTR", false);
            
            log.debug("insertOTCLEGMSTRdone");
            
          //네번째 테이블: OTC_CNTR_UNAS_PRTC
            String[] columns4 = {"CNTR_ID", "CNTR_HSTR_NO", "SEQ", "UNAS_ID", "LEG_NO"};
            ListParam listParam4 = new ListParam(columns4);
            
            String[] underlyingAssets= {underlyingAsset1, underlyingAsset2, underlyingAsset3};
            
            int seq = 1;
            for (String asset: underlyingAssets) {
            	if (asset != null) {
            		int rowIdx4 = listParam4.createRow();
            		listParam4.setValue(rowIdx4, "CNTR_ID", cntrID);
            		listParam4.setValue(rowIdx4, "CNTR_HSTR_NO", 1);
            		listParam4.setValue(rowIdx4, "SEQ", seq++);
            		listParam4.setValue(rowIdx4, "UNAS_ID", asset);
            		listParam4.setValue(rowIdx4, "LEG_NO", 0);
            	}
            }
            
            dao.setValue("insertOTCCNTRUNASPRTC", listParam4);
            
            dao.sqlexe("s_insertOTCCNTRUNASPRTC", false);
            
            log.debug("insertOTCCNTRUNASPRTC done");
            
            //5번째 테이블: OTC_EXEC_MSTR, 이거 column수정이 있어서, null값을 column에 넣어주거나, query를 수정할 필요가 있다.
            //query를 수정해야 한다. 새로운 column, UP_PART_RT가 들어옴. 이거 upsert query로 처리해주어야 한다.
            //위의 knockout과 같은 query를 사용할 수 있는지 확인해야 한다. (같은 query를 사용할 수 있는 것 같다. listParam객체인데 2개의 row로 이루어진 객체를 만들어주면 된다.
            //twoWayKnockOut 2개의 row, KnockOut 1개의 row.
            String[] columns5 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "UP_PART_RT"};
            ListParam listParam5 = new ListParam(columns5);
            
            //Double callParticipationRateDouble = callParticipationRate != null ? callParticipationRate / 100.0 : null;
            
            int rowIdx5 = listParam5.createRow();
            listParam5.setValue(rowIdx5, "GDS_ID", gdsID);
            listParam5.setValue(rowIdx5, "CNTR_HSTR_NO", 1);
            listParam5.setValue(rowIdx5, "LEG_NO", 0);
            listParam5.setValue(rowIdx5, "EXEC_TP", "V");
            listParam5.setValue(rowIdx5, "EXEC_GDS_NO", "1");
            listParam5.setValue(rowIdx5, "SRC_COND_TP", "W");
            listParam5.setValue(rowIdx5, "COND_RANGE_TP", "IO");
            //이거 단위가 100단위로 들어가는 것 같은데, (1부터 해서 소수점으로 들어가는 것이 아님) 확인이 필요하다.
            listParam5.setValue(rowIdx5, "UP_PART_RT", callParticipationRate);
            
            rowIdx5 = listParam5.createRow();
            listParam5.setValue(rowIdx5, "GDS_ID", gdsID);
            listParam5.setValue(rowIdx5, "CNTR_HSTR_NO", 1);
            listParam5.setValue(rowIdx5, "LEG_NO", 0);
            listParam5.setValue(rowIdx5, "EXEC_TP", "V");
            listParam5.setValue(rowIdx5, "EXEC_GDS_NO", "2"); //이 값이 6번째 테이블에서 twoway knockout의 call, put 식별자로 사용되어 진다.
            listParam5.setValue(rowIdx5, "SRC_COND_TP", "W");
            listParam5.setValue(rowIdx5, "COND_RANGE_TP", "OI");
            //이거 단위가 100단위로 들어가는 것 같은데, (1부터 해서 소수점으로 들어가는 것이 아님) 확인이 필요하다.
            listParam5.setValue(rowIdx5, "UP_PART_RT", putParticipationRate);
            
            dao.setValue("insertOTCEXECMSTR2", listParam5);
            
            dao.sqlexe("s_insertOTCEXECMSTR2", false);
            
            log.debug("insertOTCEXECMSTR2 done");
            
            //6번째 테이블: OTC_EXEC_SCHD_PRTC
            //이거 앞에 StepDown ELS와는 다르게 loop필요없이 한 값만 넣어주면 된다. (knockout의 경우, twoway knockout의 경우에는 2개의 값 넣어주기) 
            //CPN_RT가 빠졌기 때문에, SQL query를 따로 작성해 주어야 한다.
            String[] columns6 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SQNC", "EVLT_DT", "ACTP_RT", "SETL_DT"};
            ListParam listParam6 = new ListParam(columns6);
            //int sqnc = 1;
            //for (int i = 0; i < mainPrices.size(); i++) { //Price(barrier)의 arraylist를 사용하기 때문에, length에서 size()로 바꾼다. length는 array의 길이 return. size()는 arrayList의 길이 return.
        	int rowIdx6 = listParam6.createRow();
        	listParam6.setValue(rowIdx6, "GDS_ID", gdsID);
        	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
        	listParam6.setValue(rowIdx6, "LEG_NO", 0);
        	listParam6.setValue(rowIdx6, "EXEC_TP", "V");
        	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "1");
        	listParam6.setValue(rowIdx6, "SQNC", 1);
        	listParam6.setValue(rowIdx6, "EVLT_DT", endDate.format(formatter));
        	
        	listParam6.setValue(rowIdx6, "ACTP_RT", callBarrier/100.0); //callBarrier가 행사가율로 생각되어 진다.
        	//listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*(i+1)/2.0);
        	//LocalDate initialDate = exerciseDates.get(i);
        	LocalDate endDatePlusTwoDays = endDate.plusDays(2);
        	LocalDate adjustedEndDatePlusTwoDays = adjustForWeekend(endDatePlusTwoDays);
        	
        	listParam6.setValue(rowIdx6, "SETL_DT", adjustedEndDatePlusTwoDays.format(formatter));
            //}
        	rowIdx6 = listParam6.createRow();
        	listParam6.setValue(rowIdx6, "GDS_ID", gdsID);
        	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
        	listParam6.setValue(rowIdx6, "LEG_NO", 0);
        	listParam6.setValue(rowIdx6, "EXEC_TP", "V");
        	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "2"); //이 값만 2way knockout에서 다르다. (knockout과 차이)
        	listParam6.setValue(rowIdx6, "SQNC", 1);
        	listParam6.setValue(rowIdx6, "EVLT_DT", endDate.format(formatter));
        	
        	listParam6.setValue(rowIdx6, "ACTP_RT", callBarrier/100.0); //callBarrier가 행사가율로 생각되어 진다.
        	//listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*(i+1)/2.0);
        	//LocalDate initialDate = exerciseDates.get(i);
        	
        	
        	listParam6.setValue(rowIdx6, "SETL_DT", adjustedEndDatePlusTwoDays.format(formatter));
        	
            dao.setValue("insertOTCEXECSCHDPRTC2", listParam6);
            
            dao.sqlexe("s_insertOTCEXECSCHDPRTC2", false);
            
            log.debug("insertOTCEXECSCHDPRTC2 done");
            
            //7번째 테이블: OTC_BRR_MSTR
            String[] columns7 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "OBRA_PRIC_TYPE_TP"};
        	ListParam listParam7= new ListParam(columns7);
        	
        	int rowIdx7 = listParam7.createRow();
        	listParam7.setValue(rowIdx7, "GDS_ID", gdsID);
        	listParam7.setValue(rowIdx7, "CNTR_HSTR_NO", 1);
        	listParam7.setValue(rowIdx7, "LEG_NO", 0);
        	listParam7.setValue(rowIdx7, "BRR_TP", "KO");
        	listParam7.setValue(rowIdx7, "BRR_GDS_NO","1");
        	listParam7.setValue(rowIdx7, "SRC_COND_TP", "W");
        	listParam7.setValue(rowIdx7, "COND_RANGE_TP", "OI");
        	listParam7.setValue(rowIdx7, "OBRA_PRIC_TYPE_TP", "CP");
        	
        	rowIdx7 = listParam7.createRow();
        	listParam7.setValue(rowIdx7, "GDS_ID", gdsID);
        	listParam7.setValue(rowIdx7, "CNTR_HSTR_NO", 1);
        	listParam7.setValue(rowIdx7, "LEG_NO", 0);
        	listParam7.setValue(rowIdx7, "BRR_TP", "KO");
        	listParam7.setValue(rowIdx7, "BRR_GDS_NO","2"); //lizard의 경우에는 BRR_GDS_NO는 같고, SQNC가 다르다. Two way knockout의 경우에는 BRR_GDS_NO를 다른 값으로 처리한다.
        	listParam7.setValue(rowIdx7, "SRC_COND_TP", "W");
        	listParam7.setValue(rowIdx7, "COND_RANGE_TP", "IO"); //이거 Two way knockout에서 추가됨.
        	listParam7.setValue(rowIdx7, "OBRA_PRIC_TYPE_TP", "CP");
        	
        	dao.setValue("insertOTCBRRMSTR", listParam7);
        	
        	dao.sqlexe("s_insertOTCBRRMSTR", false);
        	
        	log.debug("insertOTCBRRMSTR done");
        	
        	//8번째 테이블: OTC_BRR_SCHD_PRTC
        	String[] columns8 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
        			"OBRA_END_DT", "BRR_RT"};
        	ListParam listParam8 = new ListParam(columns8);
        	
        	int rowIdx8 = listParam8.createRow();
        	listParam8.setValue(rowIdx8, "GDS_ID", gdsID);
        	listParam8.setValue(rowIdx8, "CNTR_HSTR_NO", 1);
        	listParam8.setValue(rowIdx8, "LEG_NO", 0);
        	listParam8.setValue(rowIdx8, "BRR_TP", "KO");
        	listParam8.setValue(rowIdx8, "BRR_GDS_NO", "1");
        	listParam8.setValue(rowIdx8, "SQNC", 1);
        	listParam8.setValue(rowIdx8, "OBRA_STRT_DT", effectiveDate);
        	listParam8.setValue(rowIdx8, "OBRA_END_DT", endDate.format(formatter));
        	listParam8.setValue(rowIdx8, "BRR_RT", koBarrierUpSide/100.0);
        	
        	rowIdx8 = listParam8.createRow();
        	listParam8.setValue(rowIdx8, "GDS_ID", gdsID);
        	listParam8.setValue(rowIdx8, "CNTR_HSTR_NO", 1);
        	listParam8.setValue(rowIdx8, "LEG_NO", 0);
        	listParam8.setValue(rowIdx8, "BRR_TP", "KO");
        	listParam8.setValue(rowIdx8, "BRR_GDS_NO", "2"); //이거 knockout call, put을 다르게 처리
        	listParam8.setValue(rowIdx8, "SQNC", 1); //SQNC는 하나로 처리. Lizard처럼 2 sequence가 아니다.
        	listParam8.setValue(rowIdx8, "OBRA_STRT_DT", effectiveDate);
        	listParam8.setValue(rowIdx8, "OBRA_END_DT", endDate.format(formatter));
        	listParam8.setValue(rowIdx8, "BRR_RT", koBarrierDownSide/100.0);
        	
        	dao.setValue("insertOTCBRRSCHDPRTC", listParam8);
        	
        	dao.sqlexe("s_insertOTCBRRSCHDPRTC", false);
        	
        	log.debug("insertOTCBRRSCHDPRTC done");
        	
			dao.commit();
			log.debug("Transaction committed successfully.");
		} catch (Exception e) {
			dao.rollback();
			
			e.printStackTrace();
			log.error("Error performing database operation, rollback initiated.", e);
		}
		return cntrCode;
	}
	//MonthlyCoupon 상품을 처리해준다.
	public static String performMonthlyCouponInsert(DaoService dao, JSONObject jsonObject) {
		String cntrCode = null;
		try {
			Integer productId = getNullableInteger(jsonObject, "productId");
			String effectiveDate = jsonObject.optString("effectiveDate", null);
			String productType = jsonObject.optString("productType", null);
			Integer earlyRedempCycle = getNullableInteger(jsonObject, "earlyRedempCycle");
			Integer settleDateOffset =  getNullableInteger(jsonObject, "settleDateOffset");
			Integer maturityEvaluationDays = getNullableInteger(jsonObject, "maturityEvaluationDays");
			String underlyingAsset1 = jsonObject.optString("underlyingAsset1", null);
			String underlyingAsset2 = jsonObject.optString("underlyingAsset2", null);
			String underlyingAsset3 = jsonObject.optString("underlyingAsset3", null);
			String exercisePrices = jsonObject.optString("exercisePrices", null);
			//monthlyCoupon에는 monthlyPaymentBarrier가 추가된다.
			Double monthlyPaymentBarrier = getNullableDouble(jsonObject, "monthlyPaymentBarrier");
			Double coupon = getNullableDouble(jsonObject, "coupon");
			Double lizardCoupon = getNullableDouble(jsonObject, "lizardCoupon");
			Integer lossParticipationRate = getNullableInteger(jsonObject, "lossParticipationRate");
			Integer kiBarrier = getNullableInteger(jsonObject, "kiBarrier");
			String calculationCurrency = jsonObject.optString("calculationCurrency", null);
			
			// effectiveDate를 파싱한다. (json파싱하듯이 (String을 jsonArray나 object객체로 변경) (String을 DateTimeFormatter로 바꿔줘야 한다.)
	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	        LocalDate effectiveDateParsed = LocalDate.parse(effectiveDate, formatter);
			//endDate를 계산한다.
			//logic은 다음과 같다.
			//1. effectiveDate로부터 earlyRedempCycle만큼 개월 수를 더해준다.
			//2. exercisePrices를 '-'을 기준으로 쪼갯을 시, 나오는 갯수를 구한다.
			//3. 모든 exerciesDates를 구해준다. list형태나 array형태로 얻어낸다. 계산 시, 주말이나 공휴일이 나오면 다음 날로 밀리고, 구해진 날로부터 earlyRedempCycle만큼 더해주면서
			//모든 exerciesDates를 구한다.
			//4. exerciseDates의 마지막 element를 endDate변수에 할당한다.
			
			
	        String[] prices = exercisePrices.split("-");
	        List<LocalDate> exerciseDates = new ArrayList<>();
	        List<LocalDate> adjustedExerciseDates = new ArrayList<>();
	        
	        LocalDate exerciseDate = effectiveDateParsed;  // Start from the effective date
	        //이거 날짜 세아리는 거 과거 방식 코드
	        /*for (int i = 0; i < prices.length; i++) {
	            exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);  // Add the cycle to the previous exercise date
	            exerciseDate = adjustForWeekendAndHolidays(exerciseDate);  // Adjust for weekends and holidays
	            exerciseDates.add(exerciseDate);
	        }*/ 
	        
	        //원 exerciseDate
	        for (String price : prices) {
	        	exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);
	        	exerciseDates.add(exerciseDate);
	        }
	        
	        //휴일 조정된 exerciseDate
	        for (LocalDate date : exerciseDates) {
	            adjustedExerciseDates.add(adjustForWeekend(date));
	        }
	        
	        String endDate = adjustedExerciseDates.get(adjustedExerciseDates.size() - 1).format(formatter);
	        
	        // Logging or using the endDate
	        log.debug(exerciseDates.toString());
	        
	        /*dao.sqlexe("s_selectOTCSEQCNTRID", false);
	        ListParam CNTRIDParam = dao.getNowListParam();
	        BigDecimal cntrID = (BigDecimal) CNTRIDParam.getRow(0)[0];
	        
	        dao.sqlexe("s_selectCNTRGDSID", false);
	        ListParam CNTRGDSIDParam = dao.getNowListParam();
	        BigDecimal gdsID = (BigDecimal) CNTRGDSIDParam.getRow(0)[0];*/
	        Map<String, BigDecimal> ids = fetchIDs(dao);
	        BigDecimal cntrID = ids.get("cntrID");
	        BigDecimal gdsID = ids.get("gdsID");
	        
	        cntrCode = "QUOTE" + cntrID.toString(); 
	        
	        
	        /*
			String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
			ListParam listParam1 = new ListParam(columns1);
			
			int rowIdx1 = listParam1.createRow();
			listParam1.setValue(rowIdx1, "GDS_ID", gdsID);
			listParam1.setValue(rowIdx1, "CNTR_HSTR_NO", 1);
			listParam1.setValue(rowIdx1, "SEQ", 1);
			listParam1.setValue(rowIdx1, "CNTR_ID", cntrID);
			listParam1.setValue(rowIdx1, "GDS_TYPE_TP", "STD");
			listParam1.setValue(rowIdx1, "BUY_SELL_TP", "1");
			
            dao.setValue("insertOTCGDSMSTRTp", listParam1);

            // SQL문을 실행한다.
            dao.sqlexe("s_insertGdsMstr", false);
            
            log.debug("insertGdsMstrquerydone");
            */
	        //첫번째 테이블: OTC_GDS_MSTR
	        String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
	        Object[] values1 = {gdsID, 1, 1, cntrID, "STD", "1"};
	        createAndExecuteListParam(dao, columns1, values1, "insertOTCGDSMSTRTp", "s_insertGdsMstr");
	        log.debug("insertGdsMstrquerydone");
	        
            //두번째 테이블: OTC_CNTR_MSTR
            /*String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
            		"BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
			ListParam listParam2 = new ListParam(columns2);
			
			int rowIdx2 = listParam2.createRow();
			listParam2.setValue(rowIdx2, "CNTR_ID", cntrID);
            listParam2.setValue(rowIdx2, "CNTR_HSTR_NO", 1);
            listParam2.setValue(rowIdx2, "ISIN_CODE", "1");
            listParam2.setValue(rowIdx2, "CNTR_CODE", cntrCode);
            listParam2.setValue(rowIdx2, "CNTR_TYPE_TP", "ELS");
            listParam2.setValue(rowIdx2, "GDS_TMPL_TP", kiBarrier != null ? "001" : "007");
            listParam2.setValue(rowIdx2, "DEAL_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "AVLB_DT", effectiveDate);
            listParam2.setValue(rowIdx2, "END_DT", endDate);
            listParam2.setValue(rowIdx2, "BUY_SELL_TP", "1");
            listParam2.setValue(rowIdx2, "NMNL_AMT", 30000000);
            listParam2.setValue(rowIdx2, "NMNL_AMT_CRNC_CODE", calculationCurrency);
            listParam2.setValue(rowIdx2, "FV_LEVL_TP", "3");
            listParam2.setValue(rowIdx2, "INSD_OTSD_EVLT_TP", "I");
            listParam2.setValue(rowIdx2, "BASEP_DTRM_DT", effectiveDate);
            
            dao.setValue("insertOTCCNTRMSTR", listParam2);
            
            dao.sqlexe("s_insertOTCCNTRMSTR", false);
            */
	        //두번째 테이블: OTC_CNTR_MSTR, GDS_TMPL_TP 수정 (Monthly Cpn)
	        String[] columns2 = {"CNTR_ID", "CNTR_HSTR_NO", "ISIN_CODE", "CNTR_CODE", "CNTR_TYPE_TP", "GDS_TMPL_TP", "DEAL_DT", "AVLB_DT", "END_DT",
	                "BUY_SELL_TP", "NMNL_AMT", "NMNL_AMT_CRNC_CODE", "FV_LEVL_TP", "INSD_OTSD_EVLT_TP", "BASEP_DTRM_DT"};
	        Object[] values2 = {cntrID, 1, "1", cntrCode, "ELS", (kiBarrier != null ? "003" : "009"), effectiveDate, effectiveDate, endDate, "1", 30000000, calculationCurrency, "3", "I", effectiveDate};
	        createAndExecuteListParam(dao, columns2, values2, "insertOTCCNTRMSTR", "s_insertOTCCNTRMSTR");
	        log.debug("insertOTCCNTRMSTR");
            
			//세번째 테이블: OTC_LEG_MSTR
			String[] columns3 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO"};
            ListParam listParam3 = new ListParam(columns3);
            
            int rowIdx3 = listParam3.createRow();
            listParam3.setValue(rowIdx3, "GDS_ID", gdsID);
            listParam3.setValue(rowIdx3, "CNTR_HSTR_NO", 1);
            listParam3.setValue(rowIdx3, "LEG_NO", 0);
            
            dao.setValue("insertOTCLEGMSTR", listParam3);
            
            dao.sqlexe("s_insertOTCLEGMSTR", false);
            
            log.debug("insertOTCLEGMSTRdone");
            
            //네번째 테이블: OTC_CNTR_UNAS_PRTC
            String[] columns4 = {"CNTR_ID", "CNTR_HSTR_NO", "SEQ", "UNAS_ID", "LEG_NO"};
            ListParam listParam4 = new ListParam(columns4);
            
            String[] underlyingAssets= {underlyingAsset1, underlyingAsset2, underlyingAsset3};
            
            int seq = 1;
            for (String asset: underlyingAssets) {
            	if (asset != null) {
            		int rowIdx4 = listParam4.createRow();
            		listParam4.setValue(rowIdx4, "CNTR_ID", cntrID);
            		listParam4.setValue(rowIdx4, "CNTR_HSTR_NO", 1);
            		listParam4.setValue(rowIdx4, "SEQ", seq++);
            		listParam4.setValue(rowIdx4, "UNAS_ID", asset);
            		listParam4.setValue(rowIdx4, "LEG_NO", 0);
            	}
            }
            
            dao.setValue("insertOTCCNTRUNASPRTC", listParam4);
            
            dao.sqlexe("s_insertOTCCNTRUNASPRTC", false);
            
            log.debug("insertOTCCNTRUNASPRTC done");
            
            //5번째 테이블: OTC_EXEC_MSTR
            
            String[] columns5 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "YY_CPN_RT",
            		"DUMY_CPN_RT", "LOSS_PART_RT"};
            ListParam listParam5 = new ListParam(columns5);
            
            int rowIdx5 = listParam5.createRow();
            listParam5.setValue(rowIdx5, "GDS_ID", gdsID);
            listParam5.setValue(rowIdx5, "CNTR_HSTR_NO", 1);
            listParam5.setValue(rowIdx5, "LEG_NO", 0);
            listParam5.setValue(rowIdx5, "EXEC_TP", "A");
            listParam5.setValue(rowIdx5, "EXEC_GDS_NO", "1");
            listParam5.setValue(rowIdx5, "SRC_COND_TP", "W");
            listParam5.setValue(rowIdx5, "COND_RANGE_TP", "IO");
            //YY_CPN_RT 월지급 쿠폰인 경우 0으로 수정
            listParam5.setValue(rowIdx5, "YY_CPN_RT", 0);
            
            double dummyCouponRate = (kiBarrier != null) ? (coupon / 100.0 * earlyRedempCycle * prices.length / 12.0) : 0;
            //dummyCouponRate도 0으로 수정
            //listParam5.setValue(rowIdx5, "DUMY_CPN_RT", dummyCouponRate);
            listParam5.setValue(rowIdx5, "DUMY_CPN_RT", 0);
            listParam5.setValue(rowIdx5, "LOSS_PART_RT", lossParticipationRate);
            
            dao.setValue("insertOTCEXECMSTR", listParam5);
            
            dao.sqlexe("s_insertOTCEXECMSTR", false);
            
            log.debug("insertOTCEXECMSTR done");
            
            //6번째 테이블: OTC_EXEC_SCHD_PRTC
            
            String[] columns6 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "EXEC_TP", "EXEC_GDS_NO", "SQNC", "EVLT_DT", "ACTP_RT", "CPN_RT", "SETL_DT"};
            ListParam listParam6 = new ListParam(columns6);
            int sqnc = 1;
            for (int i = 0; i < prices.length; i++) {
            	int rowIdx6 = listParam6.createRow();
            	listParam6.setValue(rowIdx6, "GDS_ID", gdsID);
            	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
            	listParam6.setValue(rowIdx6, "LEG_NO", 0);
            	listParam6.setValue(rowIdx6, "EXEC_TP", "A");
            	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "1");
            	listParam6.setValue(rowIdx6, "SQNC", sqnc++);
            	listParam6.setValue(rowIdx6, "EVLT_DT", exerciseDates.get(i).format(formatter));
            	listParam6.setValue(rowIdx6, "ACTP_RT", Double.parseDouble(prices[i]));
            	//월지급 쿠폰의 CPN_RT의 경우: 0으로 매핑
            	listParam6.setValue(rowIdx6, "CPN_RT", 0);
            	//listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*(i+1)/2.0);
            	LocalDate initialDate = adjustedExerciseDates.get(i);
            	LocalDate adjustedDate = initialDate.plusDays(settleDateOffset);
            	adjustedDate = adjustForWeekend(adjustedDate);
            	
            	listParam6.setValue(rowIdx6, "SETL_DT", adjustedDate.format(formatter));
            	
	        }
            
            dao.setValue("insertOTCEXECSCHDPRTC", listParam6);
            
            dao.sqlexe("s_insertOTCEXECSCHDPRTC", false);
            
            log.debug("insertOTCEXECSCHDPRTC done");
            
            //7번째, 8번째 테이블
            if (kiBarrier != null) {
            	String[] columns7 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "OBRA_PRIC_TYPE_TP"};
            	ListParam listParam7= new ListParam(columns7);
            	
            	int rowIdx7 = listParam7.createRow();
            	listParam7.setValue(rowIdx7, "GDS_ID", gdsID);
            	listParam7.setValue(rowIdx7, "CNTR_HSTR_NO", 1);
            	listParam7.setValue(rowIdx7, "LEG_NO", 0);
            	listParam7.setValue(rowIdx7, "BRR_TP", "KI");
            	listParam7.setValue(rowIdx7, "BRR_GDS_NO","1");
            	listParam7.setValue(rowIdx7, "SRC_COND_TP", "W");
            	listParam7.setValue(rowIdx7, "COND_RANGE_TP", "OI");
            	listParam7.setValue(rowIdx7, "OBRA_PRIC_TYPE_TP", "CP");
            	
            	dao.setValue("insertOTCBRRMSTR", listParam7);
            	
            	dao.sqlexe("s_insertOTCBRRMSTR", false);
            	
            	log.debug("insertOTCBRRMSTR done");
            	
            	String[] columns8 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
            			"OBRA_END_DT", "BRR_RT"};
            	ListParam listParam8 = new ListParam(columns8);
            	
            	int rowIdx8 = listParam8.createRow();
            	listParam8.setValue(rowIdx8, "GDS_ID", gdsID);
            	listParam8.setValue(rowIdx8, "CNTR_HSTR_NO", 1);
            	listParam8.setValue(rowIdx8, "LEG_NO", 0);
            	listParam8.setValue(rowIdx8, "BRR_TP", "KI");
            	listParam8.setValue(rowIdx8, "BRR_GDS_NO", "1");
            	listParam8.setValue(rowIdx8, "SQNC", 1);
            	listParam8.setValue(rowIdx8, "OBRA_STRT_DT", effectiveDate);
            	listParam8.setValue(rowIdx8, "OBRA_END_DT", endDate);
            	listParam8.setValue(rowIdx8, "BRR_RT", kiBarrier);
            	
            	dao.setValue("insertOTCBRRSCHDPRTC", listParam8);
            	
            	dao.sqlexe("s_insertOTCBRRSCHDPRTC", false);
            	
            	log.debug("insertOTCBRRSCHDPRTC done");
            } else {
            	//No operation(NOP)
            }
            
            //이거 MonthlyCoupon 매핑 시작 (YY_CPN_RT 추가)
            String[] columns9 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SRC_COND_TP", "COND_RANGE_TP", "YY_CPN_RT", "OBRA_PRIC_TYPE_TP"};
            ListParam listParam9= new ListParam(columns9);
            
            int rowIdx9 = listParam9.createRow();
        	listParam9.setValue(rowIdx9, "GDS_ID", gdsID);
        	listParam9.setValue(rowIdx9, "CNTR_HSTR_NO", 1);
        	listParam9.setValue(rowIdx9, "LEG_NO", 0);
        	//이거 수정됨. (MC로)
        	listParam9.setValue(rowIdx9, "BRR_TP", "MC");
        	listParam9.setValue(rowIdx9, "BRR_GDS_NO","1");
        	listParam9.setValue(rowIdx9, "SRC_COND_TP", "W");
        	//이거 수정됨. (IO로)
        	listParam9.setValue(rowIdx9, "COND_RANGE_TP", "IO");
        	//이거 YY_CPN_RT가 추가됨.
        	listParam9.setValue(rowIdx9, "YY_CPN_RT", monthlyPaymentBarrier/100.0);
        	listParam9.setValue(rowIdx9, "OBRA_PRIC_TYPE_TP", "CP");
        	
        	//이거 sqlquery YY_CPN_RT에 대해 업데이트 된 것 추가해야 됨.
        	dao.setValue("insertOTCBRRMSTR_M", listParam9);
        	
        	dao.sqlexe("s_insertOTCBRRMSTR_M", false);
        	
        	log.debug("insertOTCBRRMSTR_M done");
        	
        	//OTC_BRR_SCHD_PRTC (for monthly coupon) CPN_RT, SETL_DT 추가
        	String[] columns10 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO", "BRR_TP", "BRR_GDS_NO", "SQNC", "OBRA_STRT_DT",
        			"OBRA_END_DT", "BRR_RT", "CPN_RT", "SETL_DT"};
        	ListParam listParam10= new ListParam(columns10);
        	LocalDate monthlyCouponDate = effectiveDateParsed;
        	int sqnc2 = 1;
        	for (int i = 0; i < prices.length*earlyRedempCycle; i++ ) {
        		int rowIdx10 = listParam10.createRow();
        		
        		listParam10.setValue(rowIdx10, "GDS_ID", gdsID);
            	listParam10.setValue(rowIdx10, "CNTR_HSTR_NO", 1);
            	listParam10.setValue(rowIdx10, "LEG_NO", 0);
            	//이거 MC(Monthly Coupon으로 조정 들어감)
            	listParam10.setValue(rowIdx10, "BRR_TP", "MC");
            	listParam10.setValue(rowIdx10, "BRR_GDS_NO", "1");
            	listParam10.setValue(rowIdx10, "SQNC", sqnc2++);
            	listParam10.setValue(rowIdx10, "OBRA_STRT_DT", monthlyCouponDate.format(formatter));
            	monthlyCouponDate = monthlyCouponDate.plusMonths(1);
            	listParam10.setValue(rowIdx10, "OBRA_END_DT", monthlyCouponDate.format(formatter));
            	listParam10.setValue(rowIdx10, "BRR_RT", monthlyPaymentBarrier/100.0);
            	listParam10.setValue(rowIdx10, "CPN_RT", (coupon/100.0)/12.0);
            	listParam10.setValue(rowIdx10, "SETL_DT", (monthlyCouponDate.plusDays(settleDateOffset)).format(formatter));
        	}
        	
        	dao.setValue("insertOTCBRRSCHDPRTC_M", listParam10);
        	
        	dao.sqlexe("s_insertOTCBRRSCHDPRTC_M", false);
        	
        	log.debug("insertOTCBRRSCHDPRTC_M done");
        	
            dao.commit();
            log.debug("Transaction committed successfully.");
            
		} catch (Exception e) {
			dao.rollback();
			
			e.printStackTrace();
			log.error("Error performing database operation, rollback initiated.", e);
		}
		return cntrCode;
	}
}
