package com.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class LineSplitMapFunction implements MapFunction<String, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> map(String s) throws Exception {
        String[] arr = s.split(" ");
        return new Tuple2<>(arr[0], arr[1]);
    }
}
