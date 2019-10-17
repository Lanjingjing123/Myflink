package com.mujie.myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

public class FlinkTest {
    public static void main(String[] args) {
        // 创建env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获得source
        DataStreamSource<String> dataStreamSource = env.socketTextStream("node10", 10001);
        SingleOutputStreamOperator<String> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                String[] words = row.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);
        // 窗口操作
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyBy.timeWindow(Time.seconds(5), Time.seconds(5));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);
        sum.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
