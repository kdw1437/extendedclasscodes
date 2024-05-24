package com.jurosys.extension.com;

import com.uro.transfer.ListParam;
import com.uro.transfer.ParamException;

public class ListParamExample {
    public static void main(String[] args) {
        try {
            // ListParam에 대한 column정의
            String[] columns = {"name", "id"};
            ListParam employees = new ListParam(columns);

            // 첫번째 직원에 대한 row 추가 및 값 설정
            int rowIndex = employees.createRow();
            employees.setValue(rowIndex, "name", "John Doe");
            employees.setValue(rowIndex, "id", "E1001");

            // 두번째 직원에 대한 row 추가 미 값 설정
            rowIndex = employees.createRow();
            employees.setValue(rowIndex, "name", "Jane Smith");
            employees.setValue(rowIndex, "id", "E1002");

            // data retrieve 예시
            String name = (String) employees.getValue(0, "name");
            String id = (String) employees.getValue(0, "id");
            System.out.println("Employee 1 - Name: " + name + ", ID: " + id);

            // 모든 열을 반복하고 값 출력
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
