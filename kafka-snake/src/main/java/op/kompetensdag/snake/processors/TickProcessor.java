package op.kompetensdag.snake.processors;

import lombok.Builder;
import lombok.Getter;
import op.kompetensdag.snake.model.*;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

public class TickProcessor {
    private GameTick gameTick;
    private HeadDirection headDirection;
    private GameTableEntry snakeHead;
    private GameTableEntry snakeTail;

    @Getter
    private String gameId;

    private GameTableEntry newPositionEntry;

    @Builder
    public TickProcessor(GameTick gameTick, HeadDirection headDirection, GameTableEntry snakeHead, GameTableEntry snakeTail, String gameId, GameTableEntry newPositionEntry) {
        this.gameTick = gameTick;
        this.headDirection = headDirection;
        this.snakeHead = snakeHead;
        this.snakeTail = snakeTail;
        this.gameId = gameId;
        this.newPositionEntry = newPositionEntry;
    }

    public TablePosition getNewHeadPosition() {
        return switch (headDirection) {
            case NORTH ->
                    TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY() + 1).build();
            case SOUTH ->
                    TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY() - 1).build();
            case EAST ->
                    TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX() + 1).build();
            case WEST ->
                    TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX() - 1).build();
            default -> snakeHead.getPosition();
        };
    }

    public boolean isNewHeadPositionTaken() {
        return Optional.ofNullable(newPositionEntry).map(GameTableEntry::getBusy).orElse(false);
    }

    public Iterable<GameTableEntry> moveSnake() {
        GameTableEntry addHead = GameTableEntry.newBuilder(snakeHead).setPosition(getNewHeadPosition()).build();
        GameTableEntry removeTail = GameTableEntry.newBuilder(snakeTail).setBusy(false).build();
        return List.of(addHead, removeTail);
    }

    public ProcessTickCommand toSerializableObject() {
        return new ProcessTickCommand(gameId, gameTick, headDirection, snakeHead, snakeTail);
    }

    public static TickProcessor.TickProcessorBuilder fromProcessTickCommand(ProcessTickCommand cmd, GameTableEntry newPositionEntry) {
        return builder().gameId(cmd.getGameId())
                .gameTick(cmd.getGameTick())
                .headDirection(cmd.getHeadDirection())
                .snakeHead(cmd.getSnakeHead())
                .snakeTail(cmd.getSnakeTail())
                .newPositionEntry(newPositionEntry);
    }
}
