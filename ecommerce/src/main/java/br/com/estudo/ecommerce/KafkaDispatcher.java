package br.com.estudo.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {
    private KafkaProducer<String, String> producer; //producer

    public KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }
    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> { // um observer para saber quando terminar
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + " partition " + data.partition() + " offset" +
                    data.offset() + " / " + data.timestamp());
        };
        producer.send(record, callback).get(); //enviando mensagem para o message broker
    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    @Override
    public void close(){
        producer.close();
    }
}
