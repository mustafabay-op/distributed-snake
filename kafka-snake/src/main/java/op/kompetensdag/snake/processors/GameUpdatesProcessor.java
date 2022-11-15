package op.kompetensdag.snake.processors;

import op.kompetensdag.snake.Color;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.GameTableEntryType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class GameUpdatesProcessor {

    public static final String DELIMITER = "-";

    public static void define(final KStream<String, GameTableEntry> tableEntryLog) {
        tableEntryLog
                .mapValues(GameUpdatesProcessor::getOutputString)
                .to(Topics.GAME_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
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
