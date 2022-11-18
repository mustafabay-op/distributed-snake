package op.kompetensdag.snake.commands;

import lombok.Builder;
import lombok.Getter;
import op.kompetensdag.snake.model.GameWall;
import op.kompetensdag.snake.model.Position;
import op.kompetensdag.snake.model.*;

import java.util.ArrayList;
import java.util.List;

@Builder
public class ProcessAdminCommand {

    @Getter
    private GameAdministrationCommand gameAdministrationCommand;
    @Getter
    private GameStatus gameStatus;
    @Getter
    private String gameId;

    public Iterable<GameTableEntry> initializeGame() {
        List<GameTableEntry> snake = getInitialSnake(List.of(new Position(10, 2), new Position(10, 3), new Position(10, 4)));
        // GameSnakeEntries gameSnakeEntries = getInitialSnake(List.of(new Position(10, 2), new Position(10, 3), new Position(10, 4)));
        List<GameTableEntry> walls = getInitialWalls(23, 17);
        ArrayList<GameTableEntry> merge = new ArrayList<>();
        merge.addAll(snake);
        merge.addAll(walls);
        return merge;
    }

    public boolean shouldInitializeGame(){
        return getGameStatus() == null || getGameStatus() == GameStatus.ENDED;
    }

    public boolean shouldPauseGame(){
        return getGameStatus() == GameStatus.RUNNING;
    }

    public boolean shouldResumeGame(){
        return getGameStatus() == GameStatus.PAUSED;
    }

    private List<GameTableEntry> getInitialWalls(int width, int height) {
        return GameWall.getWalls(width, height);
    }

    private List<GameTableEntry> getInitialSnake(List<Position> positions) {
        return positions.stream().map(this::getSnakeBodyPart).toList();
    }

    private GameTableEntry getSnakeBodyPart(Position position) {
        TablePosition snakeBodyPartPosition = TablePosition.newBuilder(new TablePosition()).setX(position.getX()).setY(position.getY()).build();
        return GameTableEntry.newBuilder(new GameTableEntry()).setBusy(true).setPosition(snakeBodyPartPosition).setType(GameTableEntryType.SNAKE).build();
    }
}
