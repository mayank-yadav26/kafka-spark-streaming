package mayank.yadav.spark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class SparkKafkaConsumer {
	private static final String COMMA = ",";
	private static final String TOPIC_NAME = "demo";

	public static void main(String[] args) throws IOException {

		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		// batchDuration - The time interval at which streaming data will be divided
		// into batches
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from the beginning of the topic
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		Collection<String> topics = Arrays.asList(TOPIC_NAME);
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

		// Specify the offsets you want to start reading from
//        Map<TopicPartition, Long> fromOffsets = new HashMap<>();
//        fromOffsets.put(new TopicPartition(TOPIC_NAME, 0), 10L); // Read from offset 10 in partition 0

//		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(ssc,
//				LocationStrategies.PreferConsistent(), ConsumerStrategies.Assign(fromOffsets.keySet(), kafkaParams, fromOffsets));

		processStream(stream);
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Its not working , checking for serialized object
	 * @param stream
	 */
	public static void processStreamTemp1(JavaInputDStream<ConsumerRecord<String, String>> stream) {
		System.out.println("Processing Start.........");
		try {
			stream.foreachRDD(rdd -> {
				OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				// Print offset ranges
				for (OffsetRange offsetRange : offsetRanges) {
					System.out.println("Topic: " + offsetRange.topic() + ", Partition: " + offsetRange.partition()
							+ ", From Offset: " + offsetRange.fromOffset() + ", Until Offset: "
							+ offsetRange.untilOffset());
				}

				List<String> allRecord = new ArrayList<>();
				List<ConsumerRecord<String, String>> records = rdd.collect();
				for (ConsumerRecord<String, String> record : records) {
					String key = record.key();
					String value = record.value();
					System.out.println("Key: " + key + ", Value: " + value);
					StringTokenizer st = new StringTokenizer(value, ",");

					StringBuilder sb = new StringBuilder();
					while (st.hasMoreTokens()) {
						String step = st.nextToken(); // Maps a unit of time in the real world. In this case 1 step
														// is 1
														// hour of time.
						String type = st.nextToken(); // CASH-IN,CASH-OUT, DEBIT, PAYMENT and TRANSFER
						String amount = st.nextToken(); // amount of the transaction in local currency
						String nameOrig = st.nextToken(); // customerID who started the transaction
						String oldbalanceOrg = st.nextToken(); // initial balance before the transaction
						String newbalanceOrig = st.nextToken(); // customer's balance after the transaction.
						String nameDest = st.nextToken(); // recipient ID of the transaction.
						String oldbalanceDest = st.nextToken(); // initial recipient balance before the transaction.
						String newbalanceDest = st.nextToken(); // recipient's balance after the transaction.
						String isFraud = st.nextToken(); // dentifies a fraudulent transaction (1) and non
															// fraudulent
															// (0)
						String isFlaggedFraud = st.nextToken(); // flags illegal attempts to transfer more than
																// 200.000
																// in a single transaction.
						// Keep only interested columnn in Master Data set.
						sb.append(step).append(COMMA).append(type).append(COMMA).append(amount).append(COMMA)
								.append(oldbalanceOrg).append(COMMA).append(newbalanceOrig).append(COMMA)
								.append(oldbalanceDest).append(COMMA).append(newbalanceDest).append(COMMA)
								.append(isFraud);
						System.out.println("Adding : " + sb.toString());
						allRecord.add(sb.toString());
					}
				}

				System.out.println("All records OUTER MOST :" + allRecord.size());
				FileWriter writer = new FileWriter("Master_dataset.csv", true);
				for (String s : allRecord) {
					System.out.println("s : " + s);
					writer.write(s);
					writer.write("\n");
				}
				System.out.println("Master dataset has been created : ");
				writer.close();
				// Commit offsets after processing
				((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
			});
		} catch (Exception exp) {
			System.out.println("Exception occurred : " + exp);
		}
	}

	public static void processStream(JavaInputDStream<ConsumerRecord<String, String>> stream) {
		System.out.println("Processing Start.........");
		try {
			stream.foreachRDD(rdd -> {
				OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				// Print offset ranges
				for (OffsetRange offsetRange : offsetRanges) {
					System.out.println("Topic: " + offsetRange.topic() + ", Partition: " + offsetRange.partition()
							+ ", From Offset: " + offsetRange.fromOffset() + ", Until Offset: "
							+ offsetRange.untilOffset());
				}
				List<String> allRecord = new ArrayList<String>();
				rdd.foreachPartition(partition -> {
					List<String> partitionRecords = new ArrayList<>();
					partition.forEachRemaining(record -> {
						String key = record.key();
						String value = record.value();
						System.out.println("Key: " + key + ", Value: " + value);
						StringTokenizer st = new StringTokenizer(value, ",");

						StringBuilder sb = new StringBuilder();
						while (st.hasMoreTokens()) {
							String step = st.nextToken(); // Maps a unit of time in the real world. In this case 1 step
															// is 1
															// hour of time.
							String type = st.nextToken(); // CASH-IN,CASH-OUT, DEBIT, PAYMENT and TRANSFER
							String amount = st.nextToken(); // amount of the transaction in local currency
							String nameOrig = st.nextToken(); // customerID who started the transaction
							String oldbalanceOrg = st.nextToken(); // initial balance before the transaction
							String newbalanceOrig = st.nextToken(); // customer's balance after the transaction.
							String nameDest = st.nextToken(); // recipient ID of the transaction.
							String oldbalanceDest = st.nextToken(); // initial recipient balance before the transaction.
							String newbalanceDest = st.nextToken(); // recipient's balance after the transaction.
							String isFraud = st.nextToken(); // dentifies a fraudulent transaction (1) and non
																// fraudulent
																// (0)
							String isFlaggedFraud = st.nextToken(); // flags illegal attempts to transfer more than
																	// 200.000
																	// in a single transaction.
							// Keep only interested columnn in Master Data set.
							sb.append(step).append(COMMA).append(type).append(COMMA).append(amount).append(COMMA)
									.append(oldbalanceOrg).append(COMMA).append(newbalanceOrig).append(COMMA)
									.append(oldbalanceDest).append(COMMA).append(newbalanceDest).append(COMMA)
									.append(isFraud);
							System.out.println("Adding : " + sb.toString());
							try {
								// To write data in file
								FileWriter writer = new FileWriter("Master_dataset.csv", true);
								writer.write(sb.toString());
								writer.write("\n");
								writer.close();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							partitionRecords.add(sb.toString());
						}
					});
//					System.out.println("partitionRecords size() : " + partitionRecords.size());
//					synchronized (allRecord) {
//						allRecord.addAll(partitionRecords);
//					}
				});
				System.out.println("All records OUTER MOST :" + allRecord.size());
//				FileWriter writer = new FileWriter("Master_dataset.csv", true);
//				for (String s : allRecord) {
//					System.out.println("s : " + s);
//					writer.write(s);
//					writer.write("\n");
//				}
				System.out.println("Master dataset has been created.....");
//				writer.close();
				// Commit offsets after processing
				((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
			});
		} catch (Exception exp) {
			System.out.println("Exception occored : " + exp);
		}
	}

	public static void processStramTemp(JavaInputDStream<ConsumerRecord<String, String>> stream) {
		List<String> allRecord = new ArrayList<String>();
		stream.foreachRDD(rdd -> {
			System.out.println(
					"New data arrived  " + rdd.partitions().size() + " Partitions and " + rdd.count() + " Records");
			if (rdd.count() > 0) {
				rdd.collect().forEach(rawRecord -> {

					System.out.println(rawRecord);
					System.out.println("***************************************");
					System.out.println(rawRecord.value());
					String record = rawRecord.value();
					StringTokenizer st = new StringTokenizer(record, ",");

					StringBuilder sb = new StringBuilder();
					while (st.hasMoreTokens()) {
						String step = st.nextToken(); // Maps a unit of time in the real world. In this case 1 step is 1
														// hour of time.
						String type = st.nextToken(); // CASH-IN,CASH-OUT, DEBIT, PAYMENT and TRANSFER
						String amount = st.nextToken(); // amount of the transaction in local currency
						String nameOrig = st.nextToken(); // customerID who started the transaction
						String oldbalanceOrg = st.nextToken(); // initial balance before the transaction
						String newbalanceOrig = st.nextToken(); // customer's balance after the transaction.
						String nameDest = st.nextToken(); // recipient ID of the transaction.
						String oldbalanceDest = st.nextToken(); // initial recipient balance before the transaction.
						String newbalanceDest = st.nextToken(); // recipient's balance after the transaction.
						String isFraud = st.nextToken(); // dentifies a fraudulent transaction (1) and non fraudulent
															// (0)
						String isFlaggedFraud = st.nextToken(); // flags illegal attempts to transfer more than 200.000
																// in a single transaction.
						// Keep only interested columnn in Master Data set.
						sb.append(step).append(COMMA).append(type).append(COMMA).append(amount).append(COMMA)
								.append(oldbalanceOrg).append(COMMA).append(newbalanceOrig).append(COMMA)
								.append(oldbalanceDest).append(COMMA).append(newbalanceDest).append(COMMA)
								.append(isFraud);
						allRecord.add(sb.toString());
					}

				});
				System.out.println("All records OUTER MOST :" + allRecord.size());
				FileWriter writer = new FileWriter("Master_dataset.csv");
				for (String s : allRecord) {
					writer.write(s);
					writer.write("\n");
				}
				System.out.println("Master dataset has been created : ");
				writer.close();
			}
		});
	}

}
