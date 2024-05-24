package com.jurosys.extension.com;

import com.uro.DaoService;

public class MainApplication {

    public static void main(String[] args) {
        // DaoService 초기화
        DaoService daoService = new DaoService();

        // 특정 값으로 sqlParam을 채운다.
        daoService.getSqlParam().addValue("username", "john_doe");

        // getStringValue를 이용해서 value을 retrieve한다.
        String username = daoService.getStringValue("username");

        // retrieved된 value를 출력한다.
        System.out.println("Username: " + username);
    }
}
