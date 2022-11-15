package op.koko.snakeclient;

import javafx.scene.input.KeyCode;

import java.util.UUID;

public class Controller {
    private final GameEventProducer gameEventProducer;

    public static String gameId;

    public Controller(final GameEventProducer gameEventProducer) {
        this.gameEventProducer = gameEventProducer;
    }

    public void sendMovementKeyPressedEvent(final KeyCode keyCode) {
        gameEventProducer.produce(gameId, keyCode);
    }

    public void sendPauseEvent(final KeyCode keyCode) {
        gameEventProducer.produce(gameId, keyCode);
    }

    public void sendStartStopEvent(final KeyCode keyCode) {
        setGameId(UUID.randomUUID().toString());
        gameEventProducer.produce(gameId, keyCode);
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }
}
