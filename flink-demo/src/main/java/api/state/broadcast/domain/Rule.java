package api.state.broadcast.domain;

/**
 * @author zdp
 * @description Rule
 * @email 13221018869@189.cn
 * @date 2021/6/24 20:12
 */
public class Rule {
    private String departmentName;
    private Double threshold;

    public Rule() {
    }

    public Rule(String departmentName, Double threshold) {
        this.departmentName = departmentName;
        this.threshold = threshold;
    }

    public String getDepartmentName() {
        return departmentName;
    }

    public void setDepartmentName(String departmentName) {
        this.departmentName = departmentName;
    }

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }
}
