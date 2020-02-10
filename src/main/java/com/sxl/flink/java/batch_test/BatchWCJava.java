package com.sxl.flink.java.batch_test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWCJava {

    public static void main(String[] args) throws Exception {

        String input = "/Users/sxl/Desktop/test/hello.txt";
        //获取上下文
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读数据
        DataSource<String> data =  env.readTextFile(input);
        data.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
               String[] tokens =  value.toLowerCase().split("\t");
               for (String token : tokens){
                   if (token.length() > 0){
                       out.collect(new Tuple2(token,1));
                   }
               }
            }
        }).groupBy(0).sum(1).print();
    }
}
