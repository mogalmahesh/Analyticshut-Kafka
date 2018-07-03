package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class SimpleKafkaConsumer {
    public static void main (String aregs[]) {

        String bootstrapServer = "localhost:9092";
        String keyDeserializer = StringDeserializer.class.getName();
        String valueDeserializer = StringDeserializer.class.getName();

        String groupID = "consumerGroup1";
        String offsetReset  = "earliest";

        String topicName = "myTopic";

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,keyDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,valueDeserializer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetReset);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(1000);
            //System.out.println(consumerRecords.count());
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                System.out.println("Partition: " + consumerRecord.partition() +
                        ", Offset: " + consumerRecord.offset() +
                        ", Key: " + consumerRecord.key() +
                        ", Value: " + consumerRecord.value());

            }
        }




    }
}
