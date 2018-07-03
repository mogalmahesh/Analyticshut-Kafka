package kafkaAdmin;

import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;
import scala.collection.Seq;

import java.util.Properties;

public class kafkaTopicOps {
    public static void main (String args[]) {
        String zookeeperHost = "127.0.0.1:2181";
        Boolean isSucre = false;
        int sessionTimeoutMs = 200000;
        int connectionTimeoutMs = 15000;
        int maxInFlightRequests = 10;
        Time time = Time.SYSTEM;
        String metricGroup = "myGroup";
        String metricType = "myType";
        KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperHost,isSucre,sessionTimeoutMs,
                connectionTimeoutMs,maxInFlightRequests,time,metricGroup,metricType);
        AdminZkClient adminZkClient = new AdminZkClient(zkClient);
        Seq<String> allTopic = zkClient.getAllTopicsInCluster();
        System.out.println("Cluset has " + allTopic.length() + " topics");
        System.out.println(allTopic);

        String topicName1 = "myTopic";
        int partitions = 3;
        int replication = 1;
        Properties topicConfig = new Properties();

        //adminZkClient.createTopic(topicName1,partitions,replication,topicConfig,RackAwareMode.Disabled$.MODULE$);

        System.out.println(zkClient.topicExists(topicName1));
        System.out.println(zkClient.isTopicMarkedForDeletion(topicName1));

        System.out.println(adminZkClient.getAllTopicConfigs());

        topicConfig.put("cleanup.policy","delete");
        adminZkClient.changeTopicConfig(topicName1,topicConfig);

        System.out.println(adminZkClient.getAllTopicConfigs().get(topicName1));

        //adminZkClient.deleteTopic(topicName1);
        //System.out.println(zkClient.isTopicMarkedForDeletion(topicName1));
        System.out.println(adminZkClient.fetchAllEntityConfigs(topicName1));

    }
}
