package br.com.estudo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaService<T>  implements Closeable {
    private final KafkaConsumer<String,T> consumer;
    private final ConsumerFunction<T> parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type){
        this(parse, groupId, type);
        consumer.subscribe(Collections.singletonList(topic));

    }
    public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type){
        this(parse, groupId, type);
        consumer.subscribe(topic);
    }
    public KafkaService(ConsumerFunction parse, String groupId, Class<T> type) {
        this.parse = parse;
        this.consumer = new KafkaConsumer(properties(type, groupId));
    }

    public void run() {
        while (true){
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("encontrei registros!" +  records.count());
                records.forEach(record -> {
                    this.parse.consume(record);
                });
            }
        }
    }

    private Properties properties(Class<T> type, String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //host e porta
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName()); // chave/valor
       // properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // me da uma mensagem de cada vez, pequenos commits
        return properties;

    }

    @Override
    public void close() {
        consumer.close();
    }
}
