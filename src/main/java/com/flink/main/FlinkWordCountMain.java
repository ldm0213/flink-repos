package com.flink.main;

import com.flink.transformation.LineSplitFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkWordCountMain {
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8823;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream(HOST, PORT);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum =
                stream.flatMap(new LineSplitFlatMapFunction()).
                        keyBy(0).
                        sum(1);
        sum.print();

        env.execute("Flink word-count example");
    }
}
