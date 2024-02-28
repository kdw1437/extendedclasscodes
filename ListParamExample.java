package com.jurosys.extension.com;

import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class ListParamExample {
    public static void main(String[] args) {
        try {
            // Define columns for the ListParam
            String[] columns = {"name", "id"};
            ListParam employees = new ListParam(columns);

            // Add a row and set values for the first employee
            int rowIndex = employees.createRow();
            employees.setValue(rowIndex, "name", "John Doe");
            employees.setValue(rowIndex, "id", "E1001");

            // Add another row and set values for the second employee
            rowIndex = employees.createRow();
            employees.setValue(rowIndex, "name", "Jane Smith");
            employees.setValue(rowIndex, "id", "E1002");

            // Example of retrieving data
            String name = (String) employees.getValue(0, "name");
            String id = (String) employees.getValue(0, "id");
            System.out.println("Employee 1 - Name: " + name + ", ID: " + id);

            // Iterate over all rows and print values
            for (int i = 0; i < employees.rowSize(); i++) {
                String employeeName = (String) employees.getValue(i, "name");
                String employeeId = (String) employees.getValue(i, "id");
                System.out.println("Employee " + (i + 1) + " - Name: " + employeeName + ", ID: " + employeeId);
            }

        } catch (ParamException e) {
            e.printStackTrace();
        }
    }
}
