package com.nibudon.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class ConsumerUtil {

    public static void main(String[] args) {
		Properties props = new Properties();
		//配置kafka集群信息
		props.put("bootstrap.servers", "10.0.10.35:9092,10.0.10.36:9092,10.0.10.37:9092,10.0.10.38:9092,10.0.10.39:9092");
		//配置消费组名称
		props.put("group.id", "group1");
		//配置从何处消费数据，latest表示消费最新消息，earliest表示从头开始消费,none表示跑出异常，默认为latest
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", StringDeserializer.class);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("test"));
		while(true){
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("生产时间：" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(record.timestamp())));
				System.out.println(record.key() + "\t" + record.value());
			}
		}
	}
    

}
