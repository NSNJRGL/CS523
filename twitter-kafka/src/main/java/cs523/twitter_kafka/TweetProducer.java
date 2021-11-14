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

public class TweetProducer {
	Logger logger = LoggerFactory.getLogger(TweetProducer.class.getName());
	
	public TweetProducer() {}

	public static void main(String[] args) {
		new TweetProducer().run();
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
							public void onCompletion(RecordMetadata recordMetadata, Exception e) {
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
		List<String> terms = Lists.newArrayList("coronavirus");// terms we want to search on twitter
		hosebirdEndpoint.trackTerms(terms);
		Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEY, TwitterConfig.CONSUMER_SECRET, TwitterConfig.TOKEN, TwitterConfig.SECRET);
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client")
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		// creating kafka producer
		// creating producer properties
		Properties prop = new Properties();
	    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAPSERVERS);
	    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

	    // create safe Producer
	    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
	    prop.setProperty(ProducerConfig.ACKS_CONFIG, KafkaConfig.ACKS_CONFIG);
	    prop.setProperty(ProducerConfig.RETRIES_CONFIG, KafkaConfig.RETRIES_CONFIG);
	    prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, KafkaConfig.MAX_IN_FLIGHT_CONN);

	    // Additional settings for high throughput producer
	    prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, KafkaConfig.COMPRESSION_TYPE);
	    prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, KafkaConfig.LINGER_CONFIG);
	    prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, KafkaConfig.BATCH_SIZE);

	    // Create producer
	    return new KafkaProducer<String, String>(prop);

	}
}