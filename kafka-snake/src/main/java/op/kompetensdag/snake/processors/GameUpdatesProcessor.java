package op.kompetensdag.snake.processors;

import op.kompetensdag.snake.model.Color;
import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.GameTableEntryType;

public class GameUpdatesProcessor {
    public static final String DELIMITER = "-";

    public static String getOutputString(GameTableEntry entry) {
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
