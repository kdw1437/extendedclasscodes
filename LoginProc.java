package com.jurosys.extension.com;

import org.slf4j.Logger;

import com.val.util.SHA256;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;
import com.uro.util.FwUtil;

public class LoginProc {
	Logger log = LoggerMg.getInstance().getLogger("fw");
	
	public void execute(DaoService dao) {
		String userId=dao.getStringValue("userId","");
		String userPw=dao.getStringValue("userPw","");
		
		if(userId.equals("") || userPw.equals("")) {
			dao.setError("사용자ID 또는 비밀번호를 입력하세요!");
			return;
		}
		
		
		String encryptUserPw=SHA256.encrypt(userPw);
		log.info("======================================================================================");
		log.info("1.사번[{}], 비번[{}]{}",userId,userPw,encryptUserPw);
		log.info("======================================================================================");
		
		log.info("======================================================================================");
		log.info("2.사용자 정보 조회");
		log.info("======================================================================================");
		
		try {
			dao.setValue("userId", userId);
			if(!userId.startsWith("99999"))
				dao.setValue("userEncryptPw", userPw);
			
			dao.sqlexe("s_selectTrmUserInfo", true);
			ListParam list=dao.getNowListParam();
			
			if(list.rowSize()>0) {
				//사용자 정보가 있다면.......... 세션에 등록한다.
				dao.getRequest().getSession().setAttribute(FwUtil.SESSION_FW_PERMIT_ID, userId);
				dao.getRequest().getSession().setAttribute("USER_ID", list.getValue(0, "userId",""));
				dao.getRequest().getSession().setAttribute("USER_NM", list.getValue(0, "userNm",""));
				dao.getRequest().getSession().setAttribute("USER_ORG_CD", list.getValue(0, "userOrgCd",""));
				dao.getRequest().getSession().setAttribute("USER_ORG_NM", list.getValue(0, "userOrgNm",""));
				dao.getRequest().getSession().setAttribute("USER_GROUP_ID", list.getValue(0, "userGroupId",""));
			}
			else {
				dao.setError("사용자 정보가 없습니다.!");
				return;
			}
		} catch (SQLServiceException e) {
			log.info("사용자 조회시 에러발생!");
			dao.setError("사용자 정보 조회시 에러가 발생하였습니다.!");
			return;
		}
		
		
		/*
		 * dao.getRequest().getSession().setAttribute(FwUtil.SESSION_FW_PERMIT_ID,
		 * userId); dao.getRequest().getSession().setAttribute("USER_ID", "uro");
		 * dao.getRequest().getSession().setAttribute("USER_NM", "유로인스트루먼츠");
		 * dao.getRequest().getSession().setAttribute("USER_ORG_CD", "908");
		 * dao.getRequest().getSession().setAttribute("USER_ORG_NM", "기술지원");
		 * dao.getRequest().getSession().setAttribute("USER_GROUP_ID", "0");
		 */
		
		
	}
}
