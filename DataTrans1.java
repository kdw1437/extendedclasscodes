package com.jurosys.extension.com;

import org.slf4j.Logger;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import org.json.JSONObject;
public class DataTrans1 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {

        // Assume jsonStr is the JSON string received in the DaoService object
        String jsonStr = dao.getStringValue("json_data");

        // Convert the JSON string to a ListParam object
        ListParam listParam = dao.jsonToListParam(jsonStr);

        // Set the converted ListParam in the DaoService object
        dao.setValue("InsertUserJobTp", listParam);

        // Attempt to execute the SQL statement
        try {
            dao.sqlexe("s_insertUserJobTpHstr", false);
        } catch (SQLServiceException e) {
            e.printStackTrace();
        }
    }
}
