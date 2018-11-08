package com.dbapp.etlengine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

import java.util.Map;
import java.util.Properties;


/**
 * 描述:
 * 启动主类
 *
 * @author lenghl
 * @create 2018-11-07 10:19
 */
public class EtlEngineApplicationStartup {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", "test10");
        DataStream<Map<String, String>> stream = env
                .addSource(new FlinkKafkaConsumer010("topicA", new EtlDeserializationSchema(), properties));
        tableEnv.registerDataStream("myTable2", stream, "name, user, timestamp");

        Table sqlResult  = tableEnv.sqlQuery("SELECT name FROM myTable2");
        sqlResult.writeToSink(new CsvTableSink("C:\\Users\\guazi\\Dev\\flink\\flink-1.6.0-bin-hadoop28-scala_2.11\\flink-1.6.0\\dataCsv", ","));
        stream.print();
        env.execute();
    }
}
