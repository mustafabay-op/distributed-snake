package op.koko.snakeclient.util;

import op.koko.snakeclient.model.Dot;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CustomSerdes {


    public static final Serde<Dot> DOT_SERDE = genericSerdeFactory(Dot.class);

    public static <T> Serde<T> genericSerdeFactory(Class<T> type) {
        JsonSerializer<T> serializer = new JsonSerializer<>();
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
