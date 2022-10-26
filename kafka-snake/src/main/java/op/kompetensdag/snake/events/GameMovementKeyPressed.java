package op.kompetensdag.snake.events;

public enum GameMovementKeyPressed {
    LEFT, RIGHT, UP, DOWN;


    public static boolean isValidValue(String value){
        try {
            valueOf(value);
            return true;
        } catch (IllegalArgumentException iaex){
            return false;
        }
    }
}
