package br.com.estudo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        LogService log = new LogService();
        try(KafkaService service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                log::parse,
                String.class)) {
            service.run();
        }
    }
        private void parse(ConsumerRecord<String, String> record) {
            System.out.println("------------------------------");
            System.out.println("LOG: " + record.topic());
            System.out.println(record.key());
            System.out.println(record.value());
            System.out.println(record.partition());
            System.out.println(record.offset());
            try {
                Thread.sleep(1000); // 1 milisegundos
            } catch (InterruptedException e) {
                //ignoring
                e.printStackTrace();
            }
            System.out.println("lAST LOG");
        };
}
