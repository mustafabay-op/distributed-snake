package op.kompetensdag.snake.processors;

import lombok.Builder;
import lombok.Getter;
import op.kompetensdag.snake.model.*;

import static op.kompetensdag.snake.model.HeadDirection.*;

@Builder
@Getter
public class MovementProcessor {

    private GameMovementKeyPressedRecord gameMovement;
    private GameStatusRecord gameStatus;
    private HeadDirectionRecord intendedHeadDirection;
    private HeadDirectionRecord currentHeadDirection;

    public static boolean isIntendedMoveValid(final HeadDirectionRecord currentHeadDirection,
                                              final HeadDirectionRecord intendedHeadDirection) {
        return (currentHeadDirection != intendedHeadDirection) && (!isOpposite(currentHeadDirection.getType(), intendedHeadDirection.getType()));
    }
    private static boolean isOpposite(final HeadDirection currentDirection,
                                      final HeadDirection intendedHeadDirection) {
        return switch (currentDirection) {
            case NORTH -> intendedHeadDirection.equals(SOUTH);
            case SOUTH -> intendedHeadDirection.equals(NORTH);
            case EAST -> intendedHeadDirection.equals(WEST);
            case WEST -> intendedHeadDirection.equals(EAST);
            default -> true;
        };
    }

    public static HeadDirectionRecord getIntendedHeadDirection(final GameMovementKeyPressed gameMovementKeyPressed) {
        return switch (gameMovementKeyPressed) {
            case UP -> new HeadDirectionRecord(NORTH);
            case DOWN -> new HeadDirectionRecord(SOUTH);
            case RIGHT -> new HeadDirectionRecord(EAST);
            case LEFT -> new HeadDirectionRecord(WEST);
            default -> null;
        };
    }
}
