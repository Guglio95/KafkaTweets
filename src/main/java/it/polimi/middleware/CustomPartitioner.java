package it.polimi.middleware;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.log4j.Logger;

import java.util.Map;

public class CustomPartitioner implements Partitioner {
    private static final Logger logger = Logger.getLogger(CustomPartitioner.class);

    public CustomPartitioner() {
    }


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int availablePartitions = cluster.availablePartitionsForTopic(topic).size();
        return partition(availablePartitions, (String) key);
    }

    static int partition(int availablePartitions, String key) {
        int choosenPartition = ((int) key.toLowerCase().toCharArray()[0]) % availablePartitions;
        logger.info("Key " + key + " is assigned to partition " + choosenPartition + " of " + availablePartitions);
        return choosenPartition;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {

    }
}
