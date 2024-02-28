 package com.jurosys.extension.com;

import org.slf4j.Logger;
import com.uro.util.StringUtil;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.service.sql.SQLServiceException;
import com.uro.transfer.ListParam;

public class GetAllSession {
	Logger log = LoggerMg.getInstance().getLogger("fw");
	
	public void execute(DaoService dao) {
		
		//세션에서 값 가져오기
		
		String userId="";
		String userNm="";
		String userOrgCd="";
		String userOrgNm="";
		String userGroupId="";
		
		userId=(String) dao.getRequest().getSession().getAttribute("USER_ID");
		userNm=(String) dao.getRequest().getSession().getAttribute("USER_NM");
		userOrgCd=(String) dao.getRequest().getSession().getAttribute("USER_ORG_CD");
		userOrgNm=(String) dao.getRequest().getSession().getAttribute("USER_ORG_NM");
		userGroupId=(String) dao.getRequest().getSession().getAttribute("USER_GROUP_ID");
		
		if(userId==null) {
			if(dao.isView()) {
				dao.setForceTargetUrl("/com/no_session");
			}
			else {
				dao.setError("권한이 없습니다. 로그인을 다시 시도해 주세요!!");
			}
			
			return;
		}
		
		/* 2022.11.24 add by LGO */		
		dao.setValue("_systemClsCd", "00");
		dao.setValue("_userIp",StringUtil.getClientIp(dao.getRequest()));
		dao.setValue("_userGroupId", userGroupId);
		dao.setValue("_userId", userId);
		dao.setValue("_userNm", userNm);
		dao.setValue("_loginDeptCd", userOrgCd);
		dao.setValue("_loginDeptNm", userOrgNm);
		dao.setValue("_loginEmpNo", userId);
		dao.setValue("_loginEmpNm", userNm);
		
		if(dao.isView()) {
			/*메뉴제목밑 뎁스를 가져온다.*/
			String title="";
			try {
				dao.setValue("reqUrl", dao.getReqUrl());
				dao.sqlexe("s_selectMenuPath2", false);
				ListParam menuList=dao.getNowListParam();
				
				if(menuList.rowSize()>0) {
					dao.setValue("menuPath", menuList.getValue(0, "menuPath", ""));
				};
				
				if(menuList.rowSize()>0) {
					String[] menuPathArray=menuList.getValue(0, "menuPath", "").split(",");
					title="<div class=\"titArea\">\r\n"
							+ "	<h3>"+menuList.getValue(0, "menuNm", "")+"<a href=\"\" class=\"tit_help\">"+menuList.getValue(0, "menuNm", "")+"</a></h3>\r\n"
							+ "	<nav aria-label=\"breadcrumb\">\r\n"
							+ "		<ol class=\"breadcrumb\">\r\n";
							
							int seq=0;
							for(String pathNm:menuPathArray) {
								seq++;
								title+="			<li class=\"breadcrumb-item "+(menuPathArray.length==seq?"active":"")+"\"><span>"+pathNm+"</span></li>\r\n";
							}
							
					title	+= "		</ol>\r\n"
							+ "	</nav>\r\n"
							+ "</div>";
				}
				
			} catch (SQLServiceException e) {
				log.info("제목타이틀, 메뉴뎁스 가져오기 실폐!!!");
			}
			
			dao.setValue("_title", title);
		}
		
	}
}
