package op.kompetensdag.snake.processors;

import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.GameTick;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

import static op.kompetensdag.snake.config.TickGeneratorConfig.STATE_STORE_NAME;

@Component
public class TickGenerator implements Transformer<String, GameStatusRecord, KeyValue<String, GameTick>> {

    private ProcessorContext context;
    private KeyValueStore<String, Short> gameRunningStatus;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.gameRunningStatus = context.getStateStore(STATE_STORE_NAME);
        this.context.
                schedule(Duration.ofMillis(500), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    gameRunningStatus.all().forEachRemaining(status -> {
                        boolean isGameRunning = status.value == 1;
                        if (isGameRunning) {
                            context.forward(status.key, new GameTick(Instant.ofEpochMilli(timestamp)));
                        }
                    });
                });
    }

    @Override
    public KeyValue<String, GameTick> transform(String key, GameStatusRecord value) {
        switch (value.getType()) {
            case STARTED, RUNNING -> gameRunningStatus.putIfAbsent(key, (short) 1);
            case PAUSED, ENDED -> gameRunningStatus.putIfAbsent(key, (short) 0);
        }
        return null;
    }

    @Override
    public void close() {
    }
}
