package com.jurosys.extension.com;

import com.uro.DaoService;

public class MainApplication {

    public static void main(String[] args) {
        // Initialize DaoService
        DaoService daoService = new DaoService();

        // Populate sqlparam with some values
        daoService.getSqlParam().addValue("username", "john_doe");

        // Retrieve the value using getStringValue
        String username = daoService.getStringValue("username");

        // Output the retrieved value
        System.out.println("Username: " + username);
    }
}
