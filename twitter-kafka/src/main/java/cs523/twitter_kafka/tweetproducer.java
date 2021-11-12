package cs523.twitter_kafka;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class tweetproducer {
	Logger logger = LoggerFactory.getLogger(tweetproducer.class.getName());
	String consumerKey = "JYH4JVCi5sSiJ25Yc1hX7VSxE";
	String consumerSecret = "Exnwn0HyKCU8VSPPiALzEytXAgkoQ7AsIqiyXFYIbeHgTD901q";// specify the consumerSecret key from the twitter app
	String token = "326551772-DAMMvcFoUxWVlG2V8ldknfvy66aDb98Og8ZSXBtR";// specify the token key from the twitter app
	String secret = "xZ7y0MOYbnmxWoX6dVbwvyFTw028WSaxGtCrOs92YFcLw";// specify the secret key from the twitter app

	public tweetproducer() {
	}// constructor to invoke the producer function

	public static void main(String[] args) {
		new tweetproducer().run();
	}

	public void run() {
		logger.info("Setup");

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);// Specify the size accordingly.
		Client client = createTweetClient(msgQueue);
		client.connect(); // invokes the connection function
		KafkaProducer<String, String> producer = createKafkaProducer();

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);// specify the times to try the connection as client
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<>("twitter-kafka", null, msg),
						new Callback() {
							@Override
							public void onCompletion(
									RecordMetadata recordMetadata, Exception e) {
								if (e != null) {
									logger.error("Something went wrong", e);
								}
							}
						});
			}

		}// Specify the topic name, key value, msg

		logger.info("This is the end");// When the reading is complete, inform logger
	}

	public Client createTweetClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("a","b","c", "d", "e", "f", "g", "h", "i", "k", "j", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");// terms we want to search on twitter
		hosebirdEndpoint.trackTerms(terms);
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,
				token, secret);
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")
				// optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient; // Attempts to establish a connection.
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		// creating kafka producer
		// creating producer properties
		String bootstrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		
		// create safe Producer
	    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
	    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
	    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
	    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

	    // Additional settings for high throughput producer
	    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
	    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
	    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
	    
		KafkaProducer<String, String> first_producer = new KafkaProducer<String, String>(
				properties);
		return first_producer;

	}
}