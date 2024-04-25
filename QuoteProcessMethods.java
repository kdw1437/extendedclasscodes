package com.jurosys.extension.com;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;
public class QuoteProcessMethods {
	private static Logger log = LoggerMg.getInstance().getLogger();
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
    
    private static List<LocalDate> holidays = Arrays.asList(
            LocalDate.of(2023, 1, 1), // New Year's Day
            LocalDate.of(2023, 12, 25) // Christmas
        );
    
    public static LocalDate adjustForWeekendAndHolidays(LocalDate date) {
        while (isWeekend(date) || isHoliday(date)) {
            date = date.plusDays(1);
        }
        return date;
    }

    private static boolean isWeekend(LocalDate date) {
        DayOfWeek day = date.getDayOfWeek();
        return day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY;
    }

    private static boolean isHoliday(LocalDate date) {
        return holidays.contains(date);
    }
	//static method로 선언해서 사용. 객체 생성 필요없이 함수처럼 사용하면 된다.
	//StepDown 상품인 경우 여기서 처리한다.
	//data processing을 메소드에서 처리하는 것이 더 낫다.
	public static void performStepDownInsert(DaoService dao, JSONObject jsonObject) {
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

	        LocalDate exerciseDate = effectiveDateParsed;  // Start from the effective date
	        for (int i = 0; i < prices.length; i++) {
	            exerciseDate = exerciseDate.plusMonths(earlyRedempCycle);  // Add the cycle to the previous exercise date
	            exerciseDate = adjustForWeekendAndHolidays(exerciseDate);  // Adjust for weekends and holidays
	            exerciseDates.add(exerciseDate);
	        }
	        
	        String endDate = exerciseDates.get(exerciseDates.size() - 1).format(formatter);
	        
	        // Logging or using the endDate
	        log.debug(exerciseDates.toString());
			//첫번째 테이블: OTC_GDS_MSTR
			String[] columns1 = {"GDS_ID", "CNTR_HSTR_NO", "SEQ", "CNTR_ID", "GDS_TYPE_TP", "BUY_SELL_TP"};
			ListParam listParam1 = new ListParam(columns1);
			
			int rowIdx1 = listParam1.createRow();
			listParam1.setValue(rowIdx1, "GDS_ID", productId);
			listParam1.setValue(rowIdx1, "CNTR_HSTR_NO", 1);
			listParam1.setValue(rowIdx1, "SEQ", 1);
			listParam1.setValue(rowIdx1, "CNTR_ID", productId);
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
			listParam2.setValue(rowIdx2, "CNTR_ID", productId);
            listParam2.setValue(rowIdx2, "CNTR_HSTR_NO", 1);
            listParam2.setValue(rowIdx2, "ISIN_CODE", '1');
            listParam2.setValue(rowIdx2, "CNTR_CODE", String.valueOf(productId));
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
            
            log.debug("insertOTCCNTRMSTR");
			//세번째 테이블: OTC_LEG_MSTR
			String[] columns3 = {"GDS_ID", "CNTR_HSTR_NO", "LEG_NO"};
            ListParam listParam3 = new ListParam(columns3);
            
            int rowIdx3 = listParam3.createRow();
            listParam3.setValue(rowIdx3, "GDS_ID", productId);
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
            		listParam4.setValue(rowIdx4, "CNTR_ID", productId);
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
            listParam5.setValue(rowIdx5, "GDS_ID", productId);
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
            	listParam6.setValue(rowIdx6, "GDS_ID", productId);
            	listParam6.setValue(rowIdx6, "CNTR_HSTR_NO", 1);
            	listParam6.setValue(rowIdx6, "LEG_NO", 0);
            	listParam6.setValue(rowIdx6, "EXEC_TP", "A");
            	listParam6.setValue(rowIdx6, "EXEC_GDS_NO", "1");
            	listParam6.setValue(rowIdx6, "SQNC", sqnc++);
            	listParam6.setValue(rowIdx6, "EVLT_DT", exerciseDates.get(i).format(formatter));
            	listParam6.setValue(rowIdx6, "ACTP_RT", Double.parseDouble(prices[i]));
            	listParam6.setValue(rowIdx6, "CPN_RT", coupon/100.0*i/2.0);
            	LocalDate initialDate = exerciseDates.get(i);
            	LocalDate adjustedDate = initialDate.plusDays(2);
            	adjustedDate = adjustForWeekendAndHolidays(adjustedDate);
            	
            	listParam6.setValue(rowIdx6, "SETL_DT", adjustedDate.format(formatter));
            }
            
            dao.setValue("insertOTCEXECSCHDPRTC", listParam6);
            
            dao.sqlexe("s_insertOTCEXECSCHDPRTC", false);
            
            log.debug("insertOTCEXECSCHDPRTC done");
            
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void performLizardInsert(DaoService dao, JSONObject jsonObject) {
		try {
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
