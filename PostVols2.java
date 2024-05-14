package com.jurosys.extension.com;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class PostVols2 {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        String jsonStr = dao.getStringValue("a");

        try {
            // Validation
            if (dataSetId == null || dataSetId.trim().isEmpty() || 
                baseDt == null || baseDt.trim().isEmpty() || 
                jsonStr == null || jsonStr.trim().isEmpty()) {
                throw new IllegalArgumentException("Missing required parameters");
            }
            
            JSONArray jsonArray = new JSONArray(jsonStr);
            String[] columns = {"BASE_DT", "DATA_SET_ID", "DATA_ID", "EXPR_VAL", "VLTL_RT", "SWAP_EXPR_VAL"};

            final int CHUNK_SIZE = 100;
            ListParam chunkParam = new ListParam(columns);

            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject dataObject = jsonArray.getJSONObject(i);
                String dataId = dataObject.getString("dataId");
                JSONArray volCurves = dataObject.getJSONArray("volCurves");

                for (int j = 0; j < volCurves.length(); j++) {
                    JSONObject volCurve = volCurves.getJSONObject(j);
                    double volFactor = volCurve.getDouble("volFactor");
                    JSONArray termVols = volCurve.getJSONArray("termVols");

                    for (int k = 0; k < termVols.length(); k++) {
                        JSONObject termVol = termVols.getJSONObject(k);
                        double tenor = termVol.getDouble("tenor");
                        double vol = termVol.getDouble("vol");

                        int rowIdx = chunkParam.createRow();
                        chunkParam.setValue(rowIdx, "BASE_DT", baseDt);
                        chunkParam.setValue(rowIdx, "DATA_SET_ID", dataSetId);
                        chunkParam.setValue(rowIdx, "DATA_ID", dataId);
                        chunkParam.setValue(rowIdx, "EXPR_VAL", tenor);
                        chunkParam.setValue(rowIdx, "VLTL_RT", vol);
                        chunkParam.setValue(rowIdx, "SWAP_EXPR_VAL", volFactor);
                    }

                    // Process chunk if size limit is reached
                    if (chunkParam.rowSize() >= CHUNK_SIZE) {
                        log.debug("Processing chunk: " + (i / CHUNK_SIZE + 1));
                        dao.setValue("insertTermVolsTp", chunkParam);
                        dao.sqlexe("s_insertTermVols", false);
                        chunkParam = new ListParam(columns); // Reset for next chunk
                    }
                }
            }

            // Process any remaining data in the last chunk
            if (chunkParam.rowSize() > 0) {
                log.debug("Processing final chunk");
                dao.setValue("insertTermVolsTp", chunkParam);
                dao.sqlexe("s_insertTermVols", false);
            }

        } catch (IllegalArgumentException e) {
            log.error("Validation error: " + e.getMessage(), e);
            dao.setError(e.getMessage());
        } catch (ParamException | SQLServiceException e) {
            log.error("Error executing PostVols", e);
            dao.setError(e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error: " + e.getMessage(), e);
            dao.setError(e.getMessage());
        }
    }
}
