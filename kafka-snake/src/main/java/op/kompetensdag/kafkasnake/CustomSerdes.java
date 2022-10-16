
package op.kompetensdag.kafkasnake;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CustomSerdes {

    public CustomSerdes() {
    }

    public static Serde<GameMovementKeyPressed> gameMovementKeyPressedValueSerde() {
        JsonSerializer<GameMovementKeyPressed> serializer = new JsonSerializer<>();
        JsonDeserializer<GameMovementKeyPressed> deserializer = new JsonDeserializer<>(GameMovementKeyPressed.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<GameAdministrationKeyPressed> gameAdministrationKeyPressedValueSerde() {
        JsonSerializer<GameAdministrationKeyPressed> serializer = new JsonSerializer<>();
        JsonDeserializer<GameAdministrationKeyPressed> deserializer = new JsonDeserializer<>(GameAdministrationKeyPressed.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<GameStatus> gameStatusSerde() {
        JsonSerializer<GameStatus> serializer = new JsonSerializer<>();
        JsonDeserializer<GameStatus> deserializer = new JsonDeserializer<>(GameStatus.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<HeadDirection> headDirectionSerde() {
        JsonSerializer<HeadDirection> serializer = new JsonSerializer<>();
        JsonDeserializer<HeadDirection> deserializer = new JsonDeserializer<>(HeadDirection.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
