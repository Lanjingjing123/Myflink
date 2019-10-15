package com.mujie.myflink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class FlinkAccumuator {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env= ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("./data/360index");
        FilterOperator<String> filter = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("https://");
            }
        });
        MapOperator<String, String> map = filter.map(new RichMapFunction<String, String>() {
            private IntCounter counter = new IntCounter();


            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("myacc",counter);
            }

            @Override
            public String map(String line) throws Exception {
                counter.add(1);
                return line;
            }
        });

        DataSink<String> dataSink = map.writeAsText("./data/result/r4", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult mycounter = env.execute("mycounter");
        Integer myacc = mycounter.getAccumulatorResult("myacc");
        System.out.println("mycounter value="+myacc);
    }
}
