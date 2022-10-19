
package op.kompetensdag.snake;

import op.kompetensdag.snake.GameAdministrationKeyPressed;
import op.kompetensdag.snake.GameMovementKeyPressed;
import op.kompetensdag.kafkasnake.GameStatus;
import op.kompetensdag.kafkasnake.HeadDirection;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CustomSerdes {


    public static Serde<GameMovementKeyPressed> gameMovementKeyPressedValueSerde = enumValueSerde(GameMovementKeyPressed.class);
    public static Serde<GameAdministrationKeyPressed> gameAdministrationKeyPressedSerde = enumValueSerde(GameAdministrationKeyPressed.class);

    public static <T> Serde<T> enumValueSerde(Class<T> type) {
        JsonSerializer<T> serializer = new JsonSerializer<>();
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
