package com.nibudon.kafka;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.*;
import java.util.Map.Entry;


public class KafkaOffsetTools {
//	public final static String KAFKA_TOPIC_NAME_ADAPTER = "sample";
	public final static String KAFKA_TOPIC_NAME_ADAPTER = "test";
//	public final static String KAFKA_TOPIC_NAME_EXCEPTION = "exception";
	public final static String KAFKA_TOPIC_NAME_EXCEPTION = "TOPIC20200319";
	public final static String KAFKA_TOPIC_NAME_AUDIT = "audit";
	private static final String rawTopicTotal = "rawTopicTotalRecordCounter";
	private static final String avroTopicTotal = "avroTopicTotalRecordCounter";
	private static final String exceptionTopicTotal = "exceptionTopicTotalRecordCounter";

	public KafkaOffsetTools() {
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.err
					.println("Error fetching data Offset Data the Broker. Reason: "
							+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private TreeMap<Integer, PartitionMetadata> findLeader(List a_seedBrokers,
			String a_topic) {
		TreeMap<Integer, PartitionMetadata> map = new TreeMap<Integer, PartitionMetadata>();
		loop: for (Object seed1 : a_seedBrokers) {
			String seed = String.valueOf(seed1);
			SimpleConsumer consumer = null;
			try {
				String[] hostAndPort;
				hostAndPort = seed.split(":");
				consumer = new SimpleConsumer(hostAndPort[0],
						Integer.valueOf(hostAndPort[1]), 100000, 64 * 1024,
						"leaderLookup" + new Date().getTime());
				List topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List metaData = resp.topicsMetadata();
				for (Object item1 : metaData) {
					TopicMetadata item = (TopicMetadata) item1;
					for (PartitionMetadata part : item.partitionsMetadata()) {
						map.put(part.partitionId(), part);
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", ] Reason: "
						+ e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		return map;
	}
	
	public long getProduceCountByTopic(String topic,String servers) {
		String kafkaBrokerList = servers;
		// init topic,logSize = 0
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(topic, 0);
		// init kafka broker list
		String[] kafkaHosts;
		kafkaHosts = kafkaBrokerList.split(",");
		if (kafkaHosts == null || kafkaHosts.length == 0) {
			System.err
					.println("No config kafka metadata.broker.list,it is null .");
			System.exit(1);
		}
		List seeds = new ArrayList();
		for (int i = 0; i < kafkaHosts.length; i++) {
			seeds.add(kafkaHosts[i]);
		}

		KafkaOffsetTools kot = new KafkaOffsetTools();

		for (String topicName : topics.keySet()) {
			TreeMap<Integer, PartitionMetadata> metadatas = kot.findLeader(
					seeds, topicName);
			int logSize = 0;
			for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
				int partition = entry.getKey();
				String leadBroker = entry.getValue().leader().host();
				String clientName = "Client_" + topicName + "_" + partition;
				SimpleConsumer consumer = new SimpleConsumer(leadBroker, entry
						.getValue().leader().port(), 100000, 64 * 1024,
						clientName);
				long readOffset = getLastOffset(consumer, topicName, partition,
						kafka.api.OffsetRequest.LatestTime(), clientName);
				logSize += readOffset;
				if (consumer != null)
					consumer.close();
			}
			topics.put(topicName, logSize);
		}
		long result =topics.get(topic);
		return result;
	}
	
	public static void main(String[] args) {
		System.out.println("t:" + new KafkaOffsetTools().getProduceCountByTopic("test","10.0.10.35:9092,10.0.10.36:9092,10.0.10.37:9092,10.0.10.38:9092,10.0.10.39:9092"));
		long xwKafkaCount = new KafkaOffsetTools().getProduceCountByTopic("TOPIC20200322","10.0.10.1:9092,10.0.10.2:9092,10.0.10.3:9092,10.0.10.4:9092,10.0.10.5:9092");
		long dsjKafkaCount = new KafkaOffsetTools().getProduceCountByTopic("TOPIC20200322","10.0.10.144:9092,10.0.10.145:9092,10.0.10.146:9092,10.0.10.147:9092,10.0.10.148:9092");
		System.out.println("现               网:" + xwKafkaCount);
		System.out.println("大数据平台:" + dsjKafkaCount);
	}

	public static void main1(String[] args) {
		String kafkaBrokerList = System.getenv("metadata.broker.list");
		if (kafkaBrokerList == null || kafkaBrokerList.length() == 0) {
			System.err
					.println("No config kafka metadata.broker.list,it is null .");
			// for test
//			kafkaBrokerList = "10.0.10.144:9092,10.0.10.145:9092,10.0.10.146:9092,10.0.10.147:9092,10.0.10.148:9092";
			kafkaBrokerList = "10.0.10.35:9092,10.0.10.36:9092,10.0.10.37:9092,10.0.10.38:9092,10.0.10.39:9092";
			System.err
					.println("Use this broker list for test,metadata.broker.list="
							+ kafkaBrokerList);
		}
		// init topic,logSize = 0
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(KAFKA_TOPIC_NAME_ADAPTER, 0);
		topics.put(KAFKA_TOPIC_NAME_EXCEPTION, 0);
		topics.put(KAFKA_TOPIC_NAME_AUDIT, 0);
		// init kafka broker list
		String[] kafkaHosts;
		kafkaHosts = kafkaBrokerList.split(",");
		if (kafkaHosts == null || kafkaHosts.length == 0) {
			System.err
					.println("No config kafka metadata.broker.list,it is null .");
			System.exit(1);
		}
		List seeds = new ArrayList();
		for (int i = 0; i < kafkaHosts.length; i++) {
			seeds.add(kafkaHosts[i]);
		}

		KafkaOffsetTools kot = new KafkaOffsetTools();

		for (String topicName : topics.keySet()) {
			TreeMap<Integer, PartitionMetadata> metadatas = kot.findLeader(
					seeds, topicName);
			int logSize = 0;
			for (Entry<Integer, PartitionMetadata> entry : metadatas.entrySet()) {
				int partition = entry.getKey();
				String leadBroker = entry.getValue().leader().host();
				String clientName = "Client_" + topicName + "_" + partition;
				SimpleConsumer consumer = new SimpleConsumer(leadBroker, entry
						.getValue().leader().port(), 100000, 64 * 1024,
						clientName);
				long readOffset = getLastOffset(consumer, topicName, partition,
						kafka.api.OffsetRequest.LatestTime(), clientName);
				logSize += readOffset;
				if (consumer != null)
					consumer.close();
			}
			topics.put(topicName, logSize);
		}
		System.out.println(topics.toString());
		System.out.println(rawTopicTotal + "="
				+ topics.get(KAFKA_TOPIC_NAME_ADAPTER) + " "
				+ System.currentTimeMillis());
		System.out.println(avroTopicTotal + "="
				+ topics.get(KAFKA_TOPIC_NAME_AUDIT) + " "
				+ System.currentTimeMillis());
		System.out.println(exceptionTopicTotal + "="
				+ topics.get(KAFKA_TOPIC_NAME_EXCEPTION) + " "
				+ System.currentTimeMillis());
	}
}