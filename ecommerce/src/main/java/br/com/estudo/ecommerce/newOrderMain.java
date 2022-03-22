package br.com.estudo.ecommerce;


import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class newOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i <= 10;i++){
                var key = UUID.randomUUID().toString(); // ramdom id
                var value = key + "123123,123132,AA";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank for your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, value);
            }
        }

    }
}
