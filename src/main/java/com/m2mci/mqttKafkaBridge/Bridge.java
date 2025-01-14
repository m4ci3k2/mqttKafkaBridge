package com.m2mci.mqttKafkaBridge;

// import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.kohsuke.args4j.CmdLineException;

public class Bridge implements MqttCallback {
	private Logger logger = Logger.getLogger(this.getClass().getName());
	private MqttAsyncClient mqtt;
	private Producer<String, String> kafkaProducer;
	private String format;
	private String kafkaFormat;
	private Pattern filter;

	public Bridge(String format_, String kafkaFormat_, String filter_) {
		format = format_;
		kafkaFormat = kafkaFormat_;
		filter = Pattern.compile(filter_);
	}
	
	private void connect(String serverURI, String clientId, String brokerList, String username, String password) throws MqttException {
		mqtt = new MqttAsyncClient(serverURI, clientId);
		mqtt.setCallback(this);
		
		MqttConnectOptions connOpts = new MqttConnectOptions();
		if (username != null && username != "") {
			connOpts.setUserName(username);
		}
		if (password != null && password != "") {
			connOpts.setPassword(password.toCharArray());
		}

		IMqttToken token = mqtt.connect(connOpts);
		Properties props = new Properties();
                props.put("metadata.broker.list", brokerList);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		kafkaProducer = new Producer<String, String>(config);
		token.waitForCompletion();
		logger.info("Connected to MQTT and Kafka");
	}

	private void reconnect() throws MqttException {
		IMqttToken token = mqtt.connect();
		token.waitForCompletion();
	}
	
	private void subscribe(String[] mqttTopicFilters) throws MqttException {
		int[] qos = new int[mqttTopicFilters.length];
		for (int i = 0; i < qos.length; ++i) {
			qos[i] = 0;
		}
		mqtt.subscribe(mqttTopicFilters, qos);
	}

	@Override
	public void connectionLost(Throwable cause) {
		logger.warn("Lost connection to MQTT server", cause);
		while (true) {
			try {
				logger.info("Attempting to reconnect to MQTT server");
				reconnect();
				logger.info("Reconnected to MQTT server, resuming");
				return;
			} catch (MqttException e) {
				logger.warn("Reconnect failed, retrying in 10 seconds", e);
			}
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
		}
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		logger.debug(String.format("from mqtt: topic %s, message %s", topic, message));
		String payload  = message.toString();
		if (! filter.matcher(payload).lookingAt())
		{
			logger.debug("message does not match filter, ignoring");
			return;
		}
		/* if you know that payload is in utf-8, use
		   String payload = String(message.getPayload(), StandardCharsets.UTF_8);
		*/
		String output = String.format(format, payload, System.currentTimeMillis(), topic);
		topic = String.format(kafkaFormat, topic);
		logger.debug(String.format("to kafka: topic %s, message %s", topic, output));
                kafkaProducer.send(new KeyedMessage<String, String>(topic, new String(output)));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CommandLineParser parser = null;
		try {
			parser = new CommandLineParser();
			parser.parse(args);
			Bridge bridge = new Bridge(parser.getKafkaFormat(), parser.getKafkaTopicFormat(), parser.getFilter());
			bridge.connect(parser.getServerURI(), parser.getClientId(), parser.getBrokerList(), parser.getMqttUser(), parser.getMqttPassword());
			bridge.subscribe(parser.getMqttTopicFilters());
		} catch (MqttException e) {
			e.printStackTrace(System.err);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
		}
	}
}
