package op.koko.snakeclient;

import op.koko.snakeclient.model.Color;

public class Controller {
    private final GameOutputProducer gameOutputProducer;

    private String gameId;

    public Controller(final GameOutputProducer gameOutputProducer) {
        this.gameOutputProducer = gameOutputProducer;
    }

    public void up() {
        if (Screen.isStarted)
            gameOutputProducer.produce(Color.GREEN, gameId);
    }

    public void down() {
        if (Screen.isStarted)
            gameOutputProducer.produce(Color.BLACK, gameId);
    }

    public void left() {
        if (Screen.isStarted)
            gameOutputProducer.produce(Color.WHITE, gameId);
    }

    public void right() {
        if (Screen.isStarted)
            gameOutputProducer.produce(Color.GREEN, gameId);
    }

    public void space() {
        if (Screen.isStarted)
            gameOutputProducer.produce(Color.WHITE, gameId);
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }
}
