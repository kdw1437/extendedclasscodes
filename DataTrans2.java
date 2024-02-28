package com.jurosys.extension.com;

import org.slf4j.Logger;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.Param;

public class DataTrans2 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {

        String a = dao.getStringValue("a");
        log.info("Received JSON String: " + a);
        String[] aArray = a.split("[&]"); // Splitting the string by '&'

        String[] columnArr = aArray[0].split("\\|"); // Getting column names from the first part
        ListParam aa = new ListParam(columnArr);

        // Start from the second element in aArray since the first one is just column names
        for (int i = 1; i < aArray.length; i++) {
            String[] rowValues = aArray[i].split("\\|");
            aa.createRow();
            for (int j = 0; j < rowValues.length; j++) {
                aa.setValue(i - 1, columnArr[j], rowValues[j]);
            }
        }

        if (aa != null) {
            log.info("Converted ListParam: " + aa.toString());
        } else {
            log.error("Conversion failed, ListParam is null");
        }

        dao.setValue("InsertUserJobTp", aa);
        try {
            dao.sqlexe("s_insertUserJobTpHstr1", true);
        } catch (SQLServiceException e) {
            e.printStackTrace();
        }
    }
}
