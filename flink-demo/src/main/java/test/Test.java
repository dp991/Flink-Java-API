package test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zdp
 * @description test
 * @email 13221018869@189.cn
 * @date 2021/6/29 11:00
 */
public class Test {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<Integer>();

        list.add(10);
        list.add(16);
        list.add(17);
        list.add(20);
        list.add(23);

        for (int i = 0; i < list.size() - 1; i++) {
            if (list.get(i + 1) - list.get(i) > 2) {
                System.out.println(list.get(i+1));
            }
        }

    }
}
