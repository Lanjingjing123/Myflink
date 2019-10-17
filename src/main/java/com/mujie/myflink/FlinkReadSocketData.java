package com.mujie.myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkReadSocketData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String node="";
        Integer port = 0;
        if(parameterTool.has("node")&&parameterTool.has("port")){
            node = parameterTool.get("node");
            port = Integer.valueOf(parameterTool.get("port"));
            System.out.println(node+port);

        }else{
            System.out.println("集群任务需要提交参数");
            System.exit(1);
        }

        DataStreamSource<String> dataStreamSource = env.socketTextStream(node, port);
        SingleOutputStreamOperator<String> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String row, Collector<String> out) throws Exception {
                String[] words = row.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> map = flatMap.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3 map(String value) throws Exception {
                return new Tuple3<>(value, value, 1);
            }
        });

        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyBy = map.keyBy(0, 1);
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> sum = keyBy.sum(2);
        sum.print();
        env.execute("flinkSteam");
    }
}
