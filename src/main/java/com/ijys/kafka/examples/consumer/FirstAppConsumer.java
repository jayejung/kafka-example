package com.ijys.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FirstAppConsumer {
	private static String topicName = "first-app";

	public static void main(String[] args) {
		Properties conf = new Properties();
		conf.setProperty("bootstrap.servers", "localhost:9092");
		conf.setProperty("group.id", "FirstAppConsumerGroup");
		conf.setProperty("enable.auto.commit", "false");
		conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// Kafka cluster에서 메세지를 수신
		Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);

		// 구독하는 topic 등록
		consumer.subscribe(Collections.singletonList(topicName));

		Duration pollingInterval = Duration.ofMillis(1);
		for (int count = 0; count < 300; count++) {
			// 메세지를 수신하여 콘솔에 표시
			ConsumerRecords<Integer, String> records = consumer.poll(pollingInterval);

			for (ConsumerRecord<Integer, String> record : records) {
				String msgString = String.format("key: %d, value: %s", record.key(), record.value());
				System.out.println(msgString);

				// 처리가 완료된 메세지의 오프셋을 커밋
				TopicPartition tp = new TopicPartition(record.topic(), record.partition());
				OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1);
				Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
				consumer.commitSync(commitInfo);
			}

			try {
				Thread.sleep(500);
				System.out.println("sleeped: " + count);
			} catch (InterruptedException ex) {
				ex.printStackTrace();
			}
		}

		consumer.close();
	}
}
