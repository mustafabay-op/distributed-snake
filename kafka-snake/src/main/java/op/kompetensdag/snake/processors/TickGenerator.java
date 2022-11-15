package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.GameTick;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TickGenerator implements Transformer<String, GameStatusRecord, KeyValue<String, GameTick>> {

    private static final String STATE_STORE_NAME = "GameRunningStatus";
    private static final TransformerSupplier<String, GameStatusRecord, KeyValue<String, GameTick>> tickGeneratorSupplier =
            new TransformerSupplier<>() {
                public Transformer<String, GameStatusRecord, KeyValue<String, GameTick>> get() {
                    return new TickGenerator();
                }

                public Set<StoreBuilder<?>> stores() {
                    final StoreBuilder<KeyValueStore<String, Short>> countsStoreBuilder =
                            Stores
                                    .keyValueStoreBuilder(
                                            Stores.persistentKeyValueStore(STATE_STORE_NAME),
                                            Serdes.String(),
                                            Serdes.Short()
                                    );
                    return Collections.singleton(countsStoreBuilder);
                }
            };
    private ProcessorContext context;
    private KeyValueStore<String, Short> gameRunningStatus;

    public static void define(KTable<String, GameStatusRecord> gameStatusKTable, Map<String, String> schemaRegistryProps) {


        // get a table view of game status
        // stream status updates
        // define a processor
        // for game started/resumed events
        // processor schedule punctuation
        // for game paused/ended events
        // processor stop punctuation
        // on punctuate processor will produce tick event
        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameTick> tickSerde = new SpecificAvroSerde<>();

        gameStatusSerde.configure(schemaRegistryProps, false);
        tickSerde.configure(schemaRegistryProps, false);

        gameStatusKTable
                .toStream()
                .transform(tickGeneratorSupplier)
                .to(Topics.GAME_TICKS, Produced.with(Serdes.String(), tickSerde));

    }


    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.gameRunningStatus = context.getStateStore(STATE_STORE_NAME);
        this.context.schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME, getPunctuator(context));
    }

    private Punctuator getPunctuator(ProcessorContext context) {
        return timestamp -> gameRunningStatus.all().forEachRemaining(gameStatus -> forwardIfRunning(context, timestamp, gameStatus));
    }

    private void forwardIfRunning(ProcessorContext context, long timestamp, KeyValue<String, Short> gameStatus) {
        if (gameStatus.value == 1) {
            context.forward(gameStatus.key, new GameTick(Instant.ofEpochMilli(timestamp)));
        }
    }

    @Override
    public KeyValue<String, GameTick> transform(String key, GameStatusRecord value) {
        switch (value.getType()) {
            case STARTED, RUNNING, INITIALIZING -> gameRunningStatus.putIfAbsent(key, (short) 1);
            case PAUSED, ENDED -> gameRunningStatus.putIfAbsent(key, (short) 0);
        }
        return null;
    }

    @Override
    public void close() {
    }
}
