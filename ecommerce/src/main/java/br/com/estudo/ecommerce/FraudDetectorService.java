package br.com.estudo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class FraudDetectorService {
    public static void main(String[] args) {
        var fraud = new FraudDetectorService();
        try(var service = new KafkaService(FraudDetectorService.class.getSimpleName(),"ECOMMERCE_NEW_ORDER", fraud::parse)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("------------------------------");
        System.out.println("Detecting Frauds");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            //ignoring
            e.printStackTrace();
        }
        System.out.println("No fraud!");
    };
}
