package op.kompetensdag.snake.processors;

import op.kompetensdag.snake.model.GameAdministrationCommand;
import op.kompetensdag.snake.model.GameMovementKeyPressed;
import org.apache.kafka.streams.kstream.Predicate;

public class GameInputRouter {

    public static Predicate<String, String> isGameMovementKeyPressedEvent() {
        return (k, v) -> {
            try {
                GameMovementKeyPressed.valueOf(v);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    public static Predicate<String, String> isGameAdministrationKeyPressedEvent() {
        return (k, v) -> {
            try {
                GameAdministrationCommand.valueOf(v);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }
}
