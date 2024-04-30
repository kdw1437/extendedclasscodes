package com.jurosys.extension.com;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.transfer.ListParam;
import com.jurosys.extension.com.QuoteProcessMethods;

public class PostQuoteUpdateV3 {
	Logger log = LoggerMg.getInstance().getLogger();
	
	public void execute(DaoService dao) {
		String dataSetId = dao.getRequest().getParameter("dataSetId");
		String baseDt = dao.getRequest().getParameter("baseDt");
		String jsonStr = dao.getStringValue("a");
		
		try {
			JSONArray jsonArray = new JSONArray(jsonStr);
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject jsonObject = jsonArray.getJSONObject(i);
				String productType = jsonObject.getString("productType");
				
				
				try {
				switch (productType) {
					case "StepDown":
						QuoteProcessMethods.performStepDownInsert(dao, jsonObject);
						break;	
					case "Lizard": 
						QuoteProcessMethods.performLizardInsert(dao, jsonObject);
						break;
					case "KnockOut": //이거 query문 추가가 필요하다. sql query easyFrame서버에서 등록하고 확장클래스에 등록해주면 된다.
						QuoteProcessMethods.performKnockOutInsert(dao, jsonObject);
					case "TwoWayKnockOut":
						QuoteProcessMethods.performTwoWayKnockOutInsert(dao, jsonObject);
					default:
						log.debug("Unhandled product type: " + productType);
						break;
				}	
				} catch(Exception e) {
							log.error("Error StepDown" + i, e);
							dao.setError("Error :" + e.getMessage() );
							dao.rollback();
							continue;
						}
						
				}
			
		} catch (Exception e) {
			log.error("Error parsing JSON or processing data", e);
			dao.setError("Error :" + e.getMessage());
		}
	}
}
