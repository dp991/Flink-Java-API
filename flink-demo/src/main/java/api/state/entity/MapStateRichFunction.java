package api.state.entity;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author zdp
 * @description mapstate
 * @email 13221018869@189.cn
 * @date 2021/6/2 18:40
 */
public class MapStateRichFunction extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    /**
     * MapState[K, V]存储一个Key-Value map，其功能与Java的Map几乎相同。
     * get(key: K)可以获取某个key下的value，
     * put(key: K, value: V)可以对某个key设置value，
     * contains(key: K)判断某个key是否存在，
     * remove(key: K)删除某个key以及对应的value，
     * entries(): java.lang.Iterable[java.util.Map.Entry[K, V]]返回MapState中所有的元素，
     * iterator(): java.util.Iterator[java.util.Map.Entry[K, V]]返回一个迭代器。需要注意的是，MapState中的key和Keyed State的key不是同一个key。
     */

    MapState mapState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建MapStateDescriptor对象
        MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", TypeInformation.of(String.class), TypeInformation.of(Integer.class));
        //获取到RuntimeContext的对象
        RuntimeContext runtimeContext = getRuntimeContext();
        mapState = runtimeContext.getMapState(mapStateDescriptor);
    }

    //统计部门下的员工,以及该员工的工资
    @Override
    public Tuple2<String, String> map(Tuple2<String, String> t) throws Exception {

        //传入数据：部门 姓名:工资
        int salary = Integer.parseInt(t.f1.split(":")[1]);
        String name = t.f1.split(":")[0];
        List stateList = new ArrayList();

        if (!mapState.contains(name)) {
            //判断某个元素是否存在，不存在则加入
            mapState.put(name, salary);

            stateList.add(name + "->" + salary);
        } else {
            //元素存在map中，直接处理业务逻辑
            Iterator iterator = mapState.entries().iterator();

            while (iterator.hasNext()) {
                //统计工资情况
                Map.Entry entry = (Map.Entry) iterator.next();
                String eName = entry.getKey().toString();
                int eSalary = Integer.parseInt(entry.getValue().toString());
                if (eName.equals(name)) {
                    eSalary = eSalary + salary;
                }
                stateList.add(eName + "->" + eSalary);
                //更新状态
                mapState.put(eName, eSalary);
            }
        }
        return new Tuple2<>(t.f0, stateList.toString());
    }
}
