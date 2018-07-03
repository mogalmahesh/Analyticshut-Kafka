package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class FirstKafkaProducer {
    public static void main (String args[]){
        String bootstrapServer = "localhost:9092";
        String keySerializer = StringSerializer.class.getName();
        String valueSerializer = StringSerializer.class.getName();
        String ackStatus = "0";
        int retries = 2;
        int lingerMs = 1;
        String topicName = "myTopic";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,valueSerializer);

        properties.put(ProducerConfig.ACKS_CONFIG,ackStatus);
        properties.put(ProducerConfig.RETRIES_CONFIG,retries);
        properties.put(ProducerConfig.LINGER_MS_CONFIG ,lingerMs);

        Producer <String,String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName,"1","this is working2");
        try {
            System.out.println(producer.send(producerRecord).get().partition());
        }catch (Exception e){
            System.out.println(e);
        }
        producer.close();
    }
}
