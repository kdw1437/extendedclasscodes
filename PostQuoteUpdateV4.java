package com.jurosys.extension.com;

import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;

public class PostQuoteUpdateV4 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        String jsonStr = dao.getStringValue("a");

        try {
            JSONArray jsonArray = new JSONArray(jsonStr);
            JSONArray responseArray = new JSONArray();
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String productType = jsonObject.getString("productType");
                String cntrCode = null;

                try {
                    switch (productType) {
                        case "StepDown":
                            cntrCode = QuoteProcessMethods.performStepDownInsert(dao, jsonObject);
                            break;
                        case "Lizard":
                            cntrCode = QuoteProcessMethods.performLizardInsert(dao, jsonObject);
                            break;
                        case "KnockOut":
                            cntrCode = QuoteProcessMethods.performKnockOutInsert(dao, jsonObject);
                            break;
                        case "TwoWayKnockOut":
                            cntrCode = QuoteProcessMethods.performTwoWayKnockOutInsert(dao, jsonObject);
                            break;
                        case "MonthlyCoupon":
                            cntrCode = QuoteProcessMethods.performMonthlyCouponInsert(dao, jsonObject);
                            break;
                        case "StepDown(Swap)":
                        	cntrCode = QuoteProcessMethods.performStepDownSwapInsert(dao, jsonObject);
                        	break;
                        case "Lizard(Swap)":
                        	cntrCode = QuoteProcessMethods.performLizardSwapInsert(dao, jsonObject);
                        	break;
                        case "MonthlyCoupon(Swap)":
                        	cntrCode = QuoteProcessMethods.performMonthlyCouponsSwapInsert(dao, jsonObject);
                        	break;
                        default:
                            log.debug("Unhandled product type: " + productType);
                            break;
                    }
                    if (cntrCode != null) {
                        JSONObject responseObject = new JSONObject();
                        responseObject.put("cntrCode", cntrCode);
                        responseArray.put(responseObject);
                    }
                } catch (Exception e) {
                    log.error("Error processing product type " + productType + " at index " + i, e);
                    dao.setError("Error: " + e.getMessage());
                    dao.rollback();
                    continue;
                }
            }

            
            String finalJson = responseArray.toString();
            log.debug(finalJson);
            
            HttpServletResponse response = dao.getResponse();
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(finalJson);

            log.debug("Preparing to send response to client");

            //에러 발생하는데 에러 발생 원인 잘 모르겠음. 작동에는 문제가 없어서 에러 해결 없이 사용중.
            //dao.setForceTargetUrlYn(false);
        } catch (Exception e) {
            log.error("Error parsing JSON or processing data", e);
            dao.setError("Error: " + e.getMessage());
        }
    }
}
