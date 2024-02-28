 package com.jurosys.extension.com;

import org.slf4j.Logger;
import com.uro.DaoService;
import com.uro.log.LoggerMg;
import com.uro.util.FwUtil;

public class LogOutProc {
	Logger log = LoggerMg.getInstance().getLogger("fw");
	
	public void execute(DaoService dao) {
		
		dao.getRequest().getSession().removeAttribute(FwUtil.SESSION_FW_PERMIT_ID);
		dao.getRequest().getSession().removeAttribute("USER_ID");
		dao.getRequest().getSession().removeAttribute("USER_NM");
		dao.getRequest().getSession().removeAttribute("USER_ORG_CD");
		dao.getRequest().getSession().removeAttribute("USER_ORG_NM");
		dao.getRequest().getSession().removeAttribute("USER_GROUP_ID");
	}
}
