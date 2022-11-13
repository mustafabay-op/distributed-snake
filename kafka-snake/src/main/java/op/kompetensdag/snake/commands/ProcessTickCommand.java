package op.kompetensdag.snake.commands;

import lombok.Builder;
import lombok.Getter;
import op.kompetensdag.snake.model.GameTableEntry;
import op.kompetensdag.snake.model.GameTick;
import op.kompetensdag.snake.model.HeadDirection;
import op.kompetensdag.snake.model.TablePosition;

import java.util.List;
import java.util.Optional;

@Builder
public class ProcessTickCommand {

    private GameTick gameTick;
    private HeadDirection headDirection;
    private GameTableEntry snakeHead;
    private GameTableEntry snakeTail;

    @Getter
    private String gameId;

    private GameTableEntry newPositionEntry;

    public TablePosition getNewHeadPosition(){
        return switch(headDirection){
            case NORTH -> TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY()+1).build();
            case SOUTH -> TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY()-1).build();
            case EAST -> TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX()+1).build();
            case WEST -> TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX()-1).build();
            default -> snakeHead.getPosition();
        };
    }

    public boolean isNewHeadPositionTaken() {
        return Optional.of(newPositionEntry).map( entry -> entry.getBusy() ).orElse(false);
    }

    public Iterable<GameTableEntry> moveSnake(){
        // 1. add new snake head

        GameTableEntry addHead = GameTableEntry.newBuilder(snakeHead).setPosition(getNewHeadPosition()).build();

        // 2. invalidate snake tail

        GameTableEntry removeTail = GameTableEntry.newBuilder(snakeTail).setBusy(false).build();

        return List.of(addHead,removeTail);

    }

}
