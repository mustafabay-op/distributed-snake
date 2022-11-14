package op.koko.snakeclient;

import javafx.scene.input.KeyCode;

import java.util.UUID;

public class Controller {
    private final GameEventProducer gameEventProducer;

    public static String gameId;

    public Controller(final GameEventProducer gameEventProducer) {
        this.gameEventProducer = gameEventProducer;
    }

    public void sendMovementKeyPressedEvent(KeyCode keyCode) {
        gameEventProducer.produce(gameId, keyCode);
    }

    public void sendAdministrationKeyEvent(KeyCode keyCode) {
        if (gameId == null)
            setGameId(UUID.randomUUID().toString());
        gameEventProducer.produce(gameId, keyCode);
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }
}
