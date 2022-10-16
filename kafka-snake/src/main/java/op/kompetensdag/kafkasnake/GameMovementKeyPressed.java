package op.kompetensdag.kafkasnake;

public enum GameMovementKeyPressed {
    LEFT, RIGHT, UP, DOWN;


    public static GameMovementKeyPressed parse(String key, String value) {
        return GameMovementKeyPressed.valueOf(value);
    }
}
