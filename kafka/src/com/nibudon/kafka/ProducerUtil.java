package com.nibudon.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerUtil {

	public static void main(String[] args) {
		Properties props = new Properties();
		//配置kafka集群信息
		props.put("bootstrap.servers", "10.0.10.35:9092,10.0.10.36:9092,10.0.10.37:9092,10.0.10.38:9092,10.0.10.39:9092");
		props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", StringSerializer.class);
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		//创建生产的记录，第一个参数为指定的topic，第二个参数为key，第三个参数为value
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "name2","nibudon");
		producer.send(record);
		producer.close();
		System.out.println("finished");
	}

}
