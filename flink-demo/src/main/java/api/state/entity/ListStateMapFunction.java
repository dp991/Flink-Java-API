package api.state.entity;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author zdp
 * @description list state mapFunction
 * @email 13221018869@189.cn
 * @date 2021/5/28 15:33
 */
public class ListStateMapFunction extends RichMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

    /**
     * ListState[T]存储了一个由T类型数据组成的列表。我们可以使用add(value: T)或addAll(values: java.util.List[T])向状态中添加元素，
     * 使用get(): java.lang.Iterable[T]获取整个列表，使用update(values: java.util.List[T])来更新列表，新的列表将替换旧的列表。
     */
    ListState<String> listState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //1.创建ListStateDescriptor对象,状态注册
        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("ListStateDescriptor", TypeInformation.of(new TypeHint<String>() {
        }));
        //2.获取到RuntimeContext的对象
        RuntimeContext runtimeContext = getRuntimeContext();
        //3.通过RuntimeContext对象获取到ListState对象
        listState = runtimeContext.getListState(listStateDescriptor);
    }

    @Override
    public Tuple2<String, String> map(Tuple2<String, String> tuple2) throws Exception {

        //获取到历史数据
        Iterable<String> historyData = listState.get();

        String newString = tuple2.f1;
        //stateList 临时存储value元素
        List<String> stateList = new ArrayList<>();
        stateList.add(newString);

        Iterator<String> iterator = historyData.iterator();
        while (iterator.hasNext()) {
            //迭代器中有元素，装载到临时的list中
            stateList.add(iterator.next());
        }
        Stream<String> distinct = stateList.stream().distinct();
        List<String> collect = distinct.collect(Collectors.toList());

        //更新状态
        listState.update(collect);
        return new Tuple2<String, String>(tuple2.f0, listState.get().toString());
    }
}
