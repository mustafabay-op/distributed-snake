package op.kompetensdag.snake.events;


public enum GameAdministrationKeyPressed {
    SPACE;

    public static boolean isValidValue(String value){
        try {
            valueOf(value);
            return true;
        } catch (IllegalArgumentException iaex){
            return false;
        }
    }

}
