package api.state.broadcast.domain;

/**
 * @author zdp
 * @description SaleValue
 * @email 13221018869@189.cn
 * @date 2021/6/24 20:13
 */
public class SaleValue {
    private String employeeId;
    private String employeeName;
    private String departmentName;
    private Double sale;

    public SaleValue() {
    }

    public SaleValue(String employeeId, String employeeName, String departmentName, Double sale) {
        this.employeeId = employeeId;
        this.employeeName = employeeName;
        this.departmentName = departmentName;
        this.sale = sale;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }

    public String getEmployeeName() {
        return employeeName;
    }

    public void setEmployeeName(String employeeName) {
        this.employeeName = employeeName;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public Double getSale() {
        return sale;
    }

    public void setSale(Double sale) {
        this.sale = sale;
    }
}
