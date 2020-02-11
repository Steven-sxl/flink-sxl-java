package com.sxl.flink.java.stream_POJO;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用java API开发flink实时处理wordwount
 * 使用POJO
 */
public class StreamingWcJavaPojo {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2 读取数据
        DataStreamSource<String> data = env.socketTextStream("localhost",9999);

        //3 transform
        //3.1匿名类实现flatmap
//        data.flatMap(new FlatMapFunction<String, WC>() {
//            @Override
//            public void flatMap(String value, Collector<WC> out) throws Exception {
//                String[] tokens = value.toLowerCase().split(",");
//                for (String token : tokens){
//                    if (token.length()>0){
//                        out.collect(new WC(token,1));
//                    }
//                }
//            }
//        })
        //3.2指定转换函数方式实现flatmap
//        data.flatMap(new MyFlatMap())

        //3.3 Rich function 实现flatmap
        data.flatMap(new RichFlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> out) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens){
                    if (token.length()>0){
                        out.collect(new WC(token,1));
                    }
                }
            }
        })
                //key选择器来选择key
                .keyBy(new KeySelector<WC, String>() {
                    @Override
                    public String getKey(WC value) throws Exception {
                        return value.word;
                    }
                })
//                .keyBy("word")//field 来选择key
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print()
                .setParallelism(1);

        //4 执行
        env.execute("StreamingWcJava");
    }

    /**
     * 指定转换函数
     */
    public static class MyFlatMap implements FlatMapFunction<String,WC>{
        @Override
        public void flatMap(String value, Collector<WC> out) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            for (String token : tokens){
                if (token.length()>0){
                    out.collect(new WC(token,1));
                }
            }
        }
    }

    /**
     * POJO
     */
    public static class WC{
        private String word;
        private int count;

        public WC() {
        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }
}
