package op.kompetensdag.snake;

import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.GameTableEntryType;
import op.kompetensdag.snake.model.GameTablePosition;

import java.util.ArrayList;
import java.util.List;

public class GameWall {
    private static final int MINIMUM_WIDTH = 2;
    private static final int MINIMUM_HEIGHT = 2;

    public static List<GameTableEntry> getWalls(int width, int height) {
        validate(width, height);

        List<Position> wallPositions = new ArrayList<>();
        getfloorAndCeiling(width, height, wallPositions);
        getSides(width, height, wallPositions);

        return wallPositions.stream().map(GameWall::getWall).toList();
    }

    private static void validate(int width, int height) {
        if (width < MINIMUM_WIDTH || height < MINIMUM_HEIGHT) {
            throw new IllegalArgumentException("Width: " + width + " less than MINIMUM_WIDTH: " + MINIMUM_WIDTH + " or height: " + height + " less than MINIMUM_HEIGHT");
        }
    }

    private static void getfloorAndCeiling(int width, int height, List<Position> wallPositions) {
        for (int x = 0; x < width; x++) {
            wallPositions.add(new Position(x, 0));
            wallPositions.add(new Position(x, height - 1));
        }
    }

    private static void getSides(int width, int height, List<Position> wallPositions) {
        for (int y = 0; y < height; y++) {
            wallPositions.add(new Position(0, y));
            wallPositions.add(new Position(width - 1, y));
        }
    }

    private static GameTableEntry getWall(Position position) {
        GameTablePosition wallPosition = GameTablePosition.newBuilder(new GameTablePosition()).setX(position.getX()).setY(position.getY()).build();
        return GameTableEntry.newBuilder(new GameTableEntry()).setBusy(true).setPosition(wallPosition).setType(GameTableEntryType.WALL).build();
    }
}
