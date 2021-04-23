package com.ijys.kafka.examples.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FirstAppProducer {
	private static String topicName = "first-app";

	public static void main(String[] args) {
		Properties conf = new Properties();
		conf.setProperty("bootstrap.servers", "localhost:9092");
		conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<Integer, String> producer = new KafkaProducer<>(conf);

		int key;
		String value;

		for (int i = 1; i <= 100; i++) {
			key = i;
			value = String.valueOf(i);

			// 송신 메세지 생성
			ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);
			// 메세지 송신후 Ack를 받았을 때 실행할 작업(Callback) 등록

//			producer.send(record, new Callback() {
//
//				@Override
//				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//					if (recordMetadata != null) {
//						// 송신에 성공한 경우 처리
//						String infoString = String.format("Success partition: %d, offset: %d",
//								recordMetadata.partition(), recordMetadata.offset());
//						System.out.println(infoString);
//					} else {
//						// 송신에 실패한 경우
//						String infoString = String.format("Failed: %s", e.getMessage());
//						System.out.println(infoString);
//					}
//				}
//			});

			producer.send(record, (recordMetadata, e) -> {
				if (recordMetadata != null) {
					// 송신에 성공한 경우 처리
					String infoString = String.format("Success partition: %d, offset: %d",
							recordMetadata.partition(), recordMetadata.offset());
					System.out.println(infoString);
				} else {
					// 송신에 실패한 경우
					String infoString = String.format("Failed: %s", e.getMessage());
					System.out.println(infoString);
				}
			});

		}
		// KafkaProducer를 닫고 종료
		producer.close();
	}
}
