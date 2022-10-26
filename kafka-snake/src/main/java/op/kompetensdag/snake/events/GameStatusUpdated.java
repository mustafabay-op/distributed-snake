package op.kompetensdag.snake.events;

public enum GameStatusUpdated {
    INITIALIZING, RUNNING, PAUSED, ENDED;

    public static boolean isValidValue(String value){
        try {
            valueOf(value);
            return true;
        } catch (IllegalArgumentException iaex){
            return false;
        }
    }
}
