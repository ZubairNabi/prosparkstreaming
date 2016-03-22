package org.apress.prospark;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaDriver extends AbstractDriver {

	private final String topic;
	private Producer<String, String> producer;

	public KafkaDriver(String path, String topic, Properties props) {
		super(path);
		this.topic = topic;
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	@Override
	public void init() throws Exception {
	}

	@Override
	public void close() throws Exception {
		producer.close();
	}

	@Override
	public void sendRecord(String record) throws Exception {
		producer.send(new KeyedMessage<String, String>(topic, record));
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: KafkaDriver <path_to_input_folder> <brokerUrl> <topic>");
			System.exit(-1);
		}

		String path = args[0];
		String brokerUrl = args[1];
		String topic = args[2];

		Properties props = new Properties();
		props.put("metadata.broker.list", brokerUrl);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("request.required.acks", "1");

		KafkaDriver driver = new KafkaDriver(path, topic, props);
		try {
			driver.execute();
		} finally {
			driver.close();
		}
	}

}
