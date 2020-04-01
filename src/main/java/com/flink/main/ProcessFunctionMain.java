package com.flink.main;

import com.flink.transformation.CountWithTimestampProcessFunction;
import com.flink.transformation.LineSplitMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ProcessFunctionMain {
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8823;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream(HOST, PORT);
        // 输入: key value
        SingleOutputStreamOperator<Tuple2<String, Long>> processResult =
                stream.map(new LineSplitMapFunction()).
                        keyBy(0).
                        process(new CountWithTimestampProcessFunction());

        processResult.print();
        env.execute("Flink word-count-process-function example");
    }
}
