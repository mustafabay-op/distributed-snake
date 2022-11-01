package op.koko.snakeclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import op.koko.snakeclient.model.Dot;
import op.koko.snakeclient.model.Game;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

public class Main {
    public static final String GAME_INPUT = "game-input";
    public static final String GAME_OUTPUT = "game-output";
    private static Consumer<String, String> consumer;

    public static void main(String[] args) {
        // Kafka setup
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(GAME_OUTPUT));

        // Game setup
        ObjectMapper objectMapper = new ObjectMapper();
        Game game = new Game(20, 20, consumer, objectMapper);
        Scanner scanner = new Scanner(System.in);

        char input;
        game.start();
        while (true) {
            game.render();
            switch (input = scanner.nextLine().charAt(0)) {
                case 'w', 'a', 's', 'd' -> emitMovementEvent(input);
                case ' ' -> emitAdministrationEvent(input);
            }
        }
    }

    private static void emitAdministrationEvent(char administrationChar) {
    }

    public static void emitMovementEvent(char movementChar) {

    }

}
