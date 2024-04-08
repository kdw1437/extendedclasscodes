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

import java.net.HttpURLConnection;
import java.net.URL;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class PostGdsMstr {
    Logger log = LoggerMg.getInstance().getLogger();

    public void execute(DaoService dao) {
        String dataSetId = dao.getRequest().getParameter("dataSetId");
        String baseDt = dao.getRequest().getParameter("baseDt");
        //jsonStr이 DaoService 객체에서 받은 JSON string이다.
        String jsonStr = dao.getStringValue("a");
        
        try {
        	JSONArray products = new JSONArray(jsonStr); //jsonStr을 JSONArray객체로 받는다. JSONArray 객체로 받은 후, 처리한다.
        	ListParam listParam = new ListParam(new String[]{"GDS_ID", "GDS_TYPE_TP"});
        	
        	for (int i = 0; i < products.length(); i++) {
        		JSONObject product = products.getJSONObject(i);
        		
        		int productId = product.getInt("productId");
        		String rawProductType = product.getString("productType");
        		String productType;
        		
                switch (rawProductType) {
                case "StepDown":
                case "Lizard":
                    productType = "STD";
                    break;
                case "KnockOut":
                    productType = "VKO";
                    break;
                case "TwoWayKnockOut":
                    productType = "WAY";
                    break;
                default:
                    productType = rawProductType; // Or you can set a default value
                    break;
                }
                
        		listParam.createRow();
        		listParam.setValue(i, "GDS_ID", productId);
        		listParam.setValue(i, "GDS_TYPE_TP", productType);
        	}
        	
        	log.info(listParam.toString());
        	
        	dao.setValue("insertGdsMstrTp", listParam);
        	
        	dao.sqlexe("s_insertGdsMstr", false);
        } catch (Exception e) {
        	log.error("Error: " + e.getMessage());
        }
        
        try {
        	JSONArray products2 = new JSONArray(jsonStr); //jsonStr을 JSONArray객체로 받는다. JSONArray 객체로 받은 후, 처리한다.
        	ListParam listParam2 = new ListParam(new String[]{"GDS_ID", "GDS_TYPE_TP"});
        	
        	for (int i = 0; i < products2.length(); i++) {
        		JSONObject product = products2.getJSONObject(i);
        		
        		int productId = product.getInt("productId");
        		String rawProductType = product.getString("productType");
        		String productType;
        		
                switch (rawProductType) {
                case "StepDown":
                case "Lizard":
                    productType = "STD";
                    break;
                case "KnockOut":
                    productType = "VKO";
                    break;
                case "TwoWayKnockOut":
                    productType = "WAY";
                    break;
                default:
                    productType = rawProductType; // Or you can set a default value
                    break;
                }
                
        		listParam2.createRow();
        		listParam2.setValue(i, "GDS_ID", productId);
        		listParam2.setValue(i, "GDS_TYPE_TP", productType);
        	}
        	
        	log.info(listParam2.toString());
        	
        	dao.setValue("insertGdsMstrTp", listParam2);
        	
        	dao.sqlexe("s_insertGdsMstr", false);
        } catch (Exception e) {
        	log.error("Error: " + e.getMessage());
        }
}
}
