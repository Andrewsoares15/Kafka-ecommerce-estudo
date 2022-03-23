package br.com.estudo.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class EmailService {
    public static void main(String[] args) {
        EmailService email = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL", email::parse,
                Email.class)){
            service.run();
        }
    }
    private void parse(ConsumerRecord<String, Email> record) {
        System.out.println("------------------------------");
        System.out.println("Sending email");
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
        System.out.println("email sent!");
    };
}

