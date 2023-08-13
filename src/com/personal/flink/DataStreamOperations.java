package com.personal.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamOperations {

    public static void main(String[] args) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> file = env.readTextFile("C:\\razarapersonal\\flink-programming\\src\\main\\resources\\emp.csv");

        //----------------Filter function--------------------------
        SingleOutputStreamOperator<String> empAfterFilter = file.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.contains("php");
            }
        });

        //--------------keyby------------------
        KeyedStream<String, String> keyedStream = empAfterFilter.keyBy(value -> value.split(",")[2]);

        keyedStream.writeAsText("C:\\razarapersonal\\flink-programming\\src\\main\\resources\\grp-by-sink.csv", FileSystem.WriteMode.OVERWRITE);

        //-----------Map function---------------

        /*SingleOutputStreamOperator<Integer> ids = file.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s.split(",")[0]) + 100;
            }
        });

        ids.writeAsText("C:\\razarapersonal\\flink-programming\\src\\main\\resources\\ids-sink.csv", FileSystem.WriteMode.OVERWRITE);
*/
        env.execute("Example Flink DataStream Programming");
    }
}

