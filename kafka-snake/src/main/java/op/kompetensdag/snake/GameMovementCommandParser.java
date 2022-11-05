package op.kompetensdag.snake;

public class GameMovementCommandParser {

        static GameMovementCommand parse(String command) {
            switch (command) {
                case "UP":
                    return new GameMovementCommand("UP");
                case "DOWN":
                    return new GameMovementCommand("DOWN");
                case "LEFT":
                    return new GameMovementCommand("LEFT");
                case "RIGHT":
                    return new GameMovementCommand("RIGHT");
                default:
                    return null;
            }
        }
}
