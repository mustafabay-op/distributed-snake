package op.koko.snakeclient;

import op.koko.snakeclient.model.Color;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class GameOutputProducer {
    private final Producer<String, String> producer;
    private final String outputTopic;

    public GameOutputProducer(final Producer<String, String> producer,
                              final String topic) {
        this.producer = producer;
        this.outputTopic = topic;
    }

    public Future<RecordMetadata> produce(final Color command) {
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outputTopic, "KEY", "12-14-" + command);
        return producer.send(producerRecord);
    }
}
