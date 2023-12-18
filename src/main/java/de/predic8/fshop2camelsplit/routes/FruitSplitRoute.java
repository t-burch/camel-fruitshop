package de.predic8.fshop2camelsplit.routes;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import static java.security.MessageDigest.getInstance;
import static org.apache.camel.component.redis.RedisConstants.KEY;

@Component
public class FruitSplitRoute extends RouteBuilder {
    RedisKeyProcessor keyProcessor;

    public FruitSplitRoute(RedisKeyProcessor keyProcessor){this.keyProcessor = keyProcessor;}

    @Override
    public void configure() {
        from("timer:query-products?period=20000")
            .to("rest:get:/shop/v2:/products?start=1&limit=1")
            .to("rest:get:/shop/v2:/products?start=1&limit=${jsonpath:$.meta.count}")
            .split(jsonpath("$.products[*]"))
                .marshal().json()
                .log("Fruit Object: ${body}")
                .setProperty("fruit-object", simple("${body}"))
                .process(keyProcessor)
                .to("spring-redis:127.0.0.1:6379?command=EXISTS")
                .log("Exists in Redis: ${body}")
                .choice()
                    .when().simple("${body} == 'false'")
                        .setBody(simple("${exchangeProperty.fruit-object}"))
                        .to("spring-redis:127.0.0.1:6379?command=SET")
                        .to("activemq:queue:fruits");
    }

    @Component
    public static class RedisKeyProcessor implements Processor {
        @Override
        public void process(Exchange exchange) throws Exception {
            byte[] bodyBytes = exchange.getIn().getBody(String.class).getBytes();
            String bodyHash = bytesToHex(getInstance("SHA-256").digest(bodyBytes));
            exchange.getIn().setHeader(KEY, bodyHash);
        }

        private static String bytesToHex(byte[] hash) {
            StringBuilder hexString = new StringBuilder(2 * hash.length);
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        }
    }
}
