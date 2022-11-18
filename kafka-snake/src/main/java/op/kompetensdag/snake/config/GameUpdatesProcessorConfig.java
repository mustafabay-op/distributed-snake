package op.kompetensdag.snake.config;

import op.kompetensdag.snake.Color;
import op.kompetensdag.snake.model.GameStatus;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.GameTableEntryType;
import op.kompetensdag.snake.processors.GameUpdatesProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import static op.kompetensdag.snake.Topics.GAME_OUTPUT;

@Configuration
public class GameUpdatesProcessorConfig {

    @Bean
    public Object tableEntryUpdateOutputStreamTopology(final KStream<String, GameTableEntry> tableEntryLog) {
        tableEntryLog
                .mapValues(GameUpdatesProcessor::getOutputString)
                .to(GAME_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        return null;
    }

    @Bean
    public Object gameStatusUpdateOutputStreamTopology(final KTable<String, GameStatusRecord> gameStatusKTable) {
        gameStatusKTable
                .toStream()
                .filter((k, v) -> v.getType() == GameStatus.ENDED)
                .mapValues(v -> "0-0-RED")
                .to(GAME_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
        return null;
    }
}
