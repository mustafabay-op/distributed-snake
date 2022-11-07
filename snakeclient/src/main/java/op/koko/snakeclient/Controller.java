package op.koko.snakeclient;

import op.koko.snakeclient.model.Color;

public class Controller {
    private final GameOutputProducer gameOutputProducer;

    public Controller(final GameOutputProducer gameOutputProducer) {
        this.gameOutputProducer = gameOutputProducer;
    }

    public void up() {
        gameOutputProducer.produce(Color.GREEN);
    }

    public void down() {
        gameOutputProducer.produce(Color.BLACK);
    }

    public void left() {
        gameOutputProducer.produce(Color.WHITE);
    }

    public void right() {
        gameOutputProducer.produce(Color.GREEN);
    }

    public void space() {
        gameOutputProducer.produce(Color.WHITE);
    }
}
