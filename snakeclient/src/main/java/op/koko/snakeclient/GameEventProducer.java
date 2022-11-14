package op.koko.snakeclient;

import javafx.scene.input.KeyCode;
import op.koko.snakeclient.model.Color;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class GameEventProducer {
    private final Producer<String, String> producer;
    private final String outputTopic;

    public GameEventProducer(final Producer<String, String> producer,
                             final String topic) {
        this.producer = producer;
        this.outputTopic = topic;
    }

    public Future<RecordMetadata> produce(final String gameId, final KeyCode keyCode) {
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outputTopic, gameId, keyCode.name());
        return producer.send(producerRecord);
    }
}
