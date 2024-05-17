package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class PostDividendYield {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String baseDt = dao.getParam().getString("baseDt");
        String dataSetId = dao.getParam().getString("dataSetId");
        String jsonString = dao.getParam().getString("a");

        if (jsonString == null || baseDt == null || dataSetId == null) {
            log.error("Required parameters are missing");
            throw new IllegalArgumentException();
        }

        try {
            JSONArray jsonArray = new JSONArray(jsonString);
            ListParam listParam = createListParam(baseDt, dataSetId, jsonArray);
            log.debug(listParam.toString());

            dao.setSqlParamAddValue("insertDividendTp", listParam);
            dao.sqlexe("s_insertDividends", true);
        } catch (ParamException | SQLServiceException e) {
            log.error("Error while creating ListParam or executing SQL", e);
            dao.setError("에러가 발생했습니다." + e.getMessage());
        } catch (IllegalArgumentException e) {
        	log.error("IllegalArguementException error", e);
        	dao.setError(e.getMessage());
        }
    }

    private ListParam createListParam(String baseDt, String dataSetId, JSONArray jsonArray) throws ParamException {
        String[] columns = {"BASE_DT", "DATA_SET_ID", "DATA_ID", "CONT_DVIDRT"};
        ListParam listParam = new ListParam(columns);

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String dataId = jsonObject.getString("dataId");
            double yield = jsonObject.getDouble("yield");

            listParam.createRow();
            listParam.setValue(i, "BASE_DT", baseDt);
            listParam.setValue(i, "DATA_SET_ID", dataSetId);
            listParam.setValue(i, "DATA_ID", dataId);
            listParam.setValue(i, "CONT_DVIDRT", yield);
        }

        log.info("ListParam created: " + listParam.toString());
        return listParam;
    }
}
