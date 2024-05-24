package com.jurosys.extension.com;

import java.io.IOException;
import javax.servlet.http.HttpSession;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import com.uro.DaoService;
import com.uro.log.LoggerMg;

public class PostQuoteUpdateV3 {
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
            dao.setMessage(finalJson);

            log.debug("Preparing to send response to client");

            // response 쓰기 전에 session관련된 operation 수행 (이 방식으로 error해결해 보려 했으나 해결이 잘 안됨)
            HttpSession session = dao.getRequest().getSession(false);
            if (session != null) {
                // 기존 session attribute 가져오기
                String user = (String) session.getAttribute("user");
                if (user != null) {
                    log.debug("Session에서 가져온 사용자: " + user);
                } else {
                    log.debug("Session에서 사용자를 찾을 수 없음.");
                }

                // 새로운 session attribute 설정
                session.setAttribute("transactionStatus", "completed");
            }

            // 필요한 모든 session 상호작용을 이 지점 전에 완료하도록 함
            dao.getResponse().setCharacterEncoding("UTF-8");
            dao.getResponse().setContentType("application/json");
            dao.getResponse().getWriter().write(finalJson);
            dao.getResponse().getWriter().flush();
            dao.getResponse().getWriter().close();

            log.debug("클라이언트에게 전송된 응답: " + finalJson);
            //에러 발생하는데 에러 발생 원인 잘 모르겠음. 작동에는 문제가 없어서 에러 해결 없이 사용중.
            //dao.setForceTargetUrlYn(false);
        } catch (Exception e) {
            log.error("Error parsing JSON or processing data", e);
            dao.setError("Error: " + e.getMessage());
        }
    }
}
