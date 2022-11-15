package op.kompetensdag.snake.processors;

import op.kompetensdag.snake.Color;
import op.kompetensdag.snake.model.GameStatus;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.GameTableEntryType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

import static op.kompetensdag.snake.Topics.GAME_OUTPUT;

@Component
public class GameUpdatesProcessor {
    public static KStream<String, GameTableEntry> tableEntryLog;
    public static KTable<String, GameStatusRecord> gameStatusKTable;

    public static final String DELIMITER = "-";

    public GameUpdatesProcessor(KStream<String, GameTableEntry> tableEntryLog, KTable<String, GameStatusRecord> gameStatusKTable) {
        GameUpdatesProcessor.tableEntryLog = tableEntryLog;
        GameUpdatesProcessor.gameStatusKTable = gameStatusKTable;
    }

    public static void define() {
        tableEntryLog
                .mapValues(GameUpdatesProcessor::getOutputString)
                .to(GAME_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        gameStatusKTable
                .toStream()
                .filter((k, v) -> v.getType() == GameStatus.ENDED)
                .mapValues(v -> "0-0-RED")
                .to(GAME_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static String getOutputString(GameTableEntry entry) {
        if (entry.getBusy() && entry.getType() == GameTableEntryType.SNAKE) {
            return constructOutputString(entry, Color.GREEN);
        } else if (entry.getBusy() && entry.getType() == GameTableEntryType.WALL) {
            return constructOutputString(entry, Color.WHITE);
        } else {
            return constructOutputString(entry, Color.BLACK);
        }
    }

    private static String constructOutputString(GameTableEntry entry, Color color) {
        return entry.getPosition().getX() + DELIMITER + entry.getPosition().getY() + DELIMITER + color;
    }
}
