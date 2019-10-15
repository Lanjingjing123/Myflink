package com.mujie.myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


/**
 * 创建FLink 程序读取文件统计单词：
 * 1.创建环境
 *
 *
 * 2. 批处理：FLink处理的数据对象是Dataset
 *    流处理：FLink处理的数据对象是DataStream
 * 3. 代码流程必须符合 source -> transformation -> sink
 *      transformation 都是懒执行，需要最后使用env.execute()触发执行或者使用print(),count(),collect()触发执行
 * 4. Flink编程不是基于K,V格式的编程，通过某些方式来指定虚拟Key
 * 5.Flink 中的Tuple最多支持25个元素，每个元素是从0开始
 * 编写Flink代码的要求
 * 1.source -> transformations -> sink
 */
public class MyFisrstCodeFlink {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = environment.readTextFile("./data/words");
        FlatMapOperator<String, String> words = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    collector.collect(word);
                }
            }
        });

        words.print();
        System.out.println("###################################");
        MapOperator<String, Tuple2<String, Integer>> pairwords = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return new Tuple2<>(word,1);
            }
        });
        pairwords.print();
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = pairwords.groupBy(0);
        System.out.println("========================");
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1).setParallelism(1);

        result.writeAsText("./data/result/r1.txt", FileSystem.WriteMode.OVERWRITE);
        // result 进行排序（局部排序——每一个核去排序,因此所有结果只会是局部有序，全局乱序）
        SortPartitionOperator<Tuple2<String, Integer>> sortResult = result.sortPartition(1, Order.DESCENDING);
        result.print();
//        environment.execute("myFlink");
//        result.collect();
//        result.count();
    }
}
