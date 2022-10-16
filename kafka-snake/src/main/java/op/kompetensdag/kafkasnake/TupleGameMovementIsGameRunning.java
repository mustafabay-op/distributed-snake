package op.kompetensdag.kafkasnake;

public class TupleGameMovementIsGameRunning {
    public final GameMovementKeyPressed gameMovementKeyPressed;
    public final boolean isGameRunning;


    public TupleGameMovementIsGameRunning(GameMovementKeyPressed gameMovementKeyPressed, boolean isGameRunning) {
        this.gameMovementKeyPressed = gameMovementKeyPressed;
        this.isGameRunning = isGameRunning;
    }
}
