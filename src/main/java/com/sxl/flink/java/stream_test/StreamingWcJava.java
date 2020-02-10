package com.sxl.flink.java.stream_test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用java API开发flink实时处理wordwount
 */
public class StreamingWcJava {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2 读取数据
        DataStreamSource<String> data = env.socketTextStream("localhost",9999);

        //3 transform
        data.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens){
                    if (token.length()>0){
                        out.collect(new Tuple2<>(token,1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);

        //4 执行
        env.execute("StreamingWcJava");
    }
}
