package br.com.estudo.ecommerce;


import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class newOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<Email>()) {
                for (int i = 0; i <= 2; i++) {
                    var userId = UUID.randomUUID().toString(); // ramdom id
                    var orderId = UUID.randomUUID().toString(); // ramdom id
                    var order = new Order(userId, orderId, new BigDecimal(Math.random() * 5000 + 1));
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    var email = new Email("andrew.fialho@iteris.com.br", "kakakaakakakkak");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }

        }
    }
}
