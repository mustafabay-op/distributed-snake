
package op.kompetensdag.snake;

import op.kompetensdag.snake.events.GameAdministrationKeyPressed;
import op.kompetensdag.snake.events.GameMovementKeyPressed;
import op.kompetensdag.snake.events.GameStatusUpdated;
import op.kompetensdag.snake.events.HeadDirectionUpdated;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CustomSerdes {


    public static Serde<GameMovementKeyPressed> gameMovementKeyPressedSerde = enumValueSerde(GameMovementKeyPressed.class);
    public static Serde<GameAdministrationKeyPressed> gameAdministrationKeyPressedSerde = enumValueSerde(GameAdministrationKeyPressed.class);
    public static Serde<GameStatusUpdated> gameStatusUpdateSerde = enumValueSerde(GameStatusUpdated.class);
    public static Serde<HeadDirectionUpdated> headDirectionUpdateSerde = enumValueSerde(HeadDirectionUpdated.class);

    public static <T> Serde<T> enumValueSerde(Class<T> type) {
        JsonSerializer<T> serializer = new JsonSerializer<>();
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
