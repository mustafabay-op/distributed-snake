package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.GameTick;
import op.kompetensdag.snake.processors.TickGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Set;

@Configuration
public class TickGeneratorConfig {

    public static final String STATE_STORE_NAME = "GameRunningStatus";

    @Bean
    public TransformerSupplier<String, GameStatusRecord, KeyValue<String, GameTick>> tickGeneratorSupplier() {
        final TransformerSupplier<String, GameStatusRecord, KeyValue<String, GameTick>> tickGeneratorSupplier =
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
        return tickGeneratorSupplier;
    }

    @Bean
    public Object tickGenerationControlTopology(final KTable<String, GameStatusRecord> gameStatusKTable,
                                              final SpecificAvroSerde<GameTick> tickSerde,
                                              TransformerSupplier<String, GameStatusRecord, KeyValue<String, GameTick>> tickGeneratorSupplier) {
        gameStatusKTable
                .toStream()
                .transform(tickGeneratorSupplier)
                .to(Topics.GAME_TICKS, Produced.with(Serdes.String(), tickSerde));
        return null;
    }
}
