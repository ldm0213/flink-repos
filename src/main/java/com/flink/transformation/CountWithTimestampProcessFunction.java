package com.flink.transformation;

import com.flink.bean.CountWithTimestamp;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimestampProcessFunction
        extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

    private ValueState<CountWithTimestamp> countWithTimestampValueState;

    public void open(Configuration parameters) throws Exception {
        // 状态保存，设置状态过期时间,设置24小时,自动清理状态
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(24))
                .cleanupFullSnapshot()
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        // 初始化ValueState
        ValueStateDescriptor<CountWithTimestamp> stateDescriptor =
                new ValueStateDescriptor("userComicReadInfoValueState",
                        TypeInformation.of(new TypeHint<CountWithTimestamp>() {}));
        stateDescriptor.enableTimeToLive(ttlConfig);
        countWithTimestampValueState = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx,
                               Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp current = countWithTimestampValueState.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.setKey(value.f0);
        }

        current.setCount(current.getCount() + 1);
        current.setLastModified(System.currentTimeMillis());

        countWithTimestampValueState.update(current);
        ctx.timerService().registerProcessingTimeTimer(current.getLastModified() + 10000);
    }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp result = countWithTimestampValueState.value();
        if (result.getLastModified() + 10000 == timestamp) {
            out.collect(new Tuple2<>(result.getKey(), result.getCount()));
        }
    }
}
