package op.koko.snakeclient;

import javafx.animation.AnimationTimer;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import op.koko.snakeclient.model.Dot;

import java.util.Queue;

public class RectangleUpdater extends AnimationTimer {
    private final Queue<Dot> queue;
    private final Rectangle[][] rectangles;

    public RectangleUpdater(Queue<Dot> queue,
                            Rectangle[][] rectangles) {
        this.queue = queue;
        this.rectangles = rectangles;
    }

    @Override
    public void handle(long now) {
        Dot dot = queue.poll();
        if (dot != null) {
            Rectangle rect = rectangles[dot.x()][dot.y()];
            switch (dot.color()) {
                case GREEN -> rect.setFill(Color.DARKGREEN);
                case WHITE -> rect.setFill(Color.WHITESMOKE);
                case BLACK -> rect.setFill(Color.BLACK);
                case RED -> Screen.showMainMenuScene();
            }
        }
    }
}
