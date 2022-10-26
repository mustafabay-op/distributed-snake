package op.kompetensdag.snake;

import op.kompetensdag.snake.events.GameMovementKeyPressed;
import op.kompetensdag.snake.events.GameStatusUpdated;
import op.kompetensdag.snake.events.HeadDirectionUpdated;
import op.kompetensdag.snake.util.Join;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import static op.kompetensdag.snake.Topics.*;

public class MovementProcessor {


    public static void define(final StreamsBuilder builder){

        builder
            .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedSerde))
            .join(builder.table(GAME_STATUS_TOPIC,Consumed.with(Serdes.String(),CustomSerdes.gameStatusUpdateSerde)),
                    (gameMovementKeyPressedValues, gameStatus) ->
                    new Join<GameMovementKeyPressed,GameStatusUpdated>(gameMovementKeyPressedValues,gameStatus)
            )
            .filter((k, movementAndStateJoin) -> movementAndStateJoin.r() == GameStatusUpdated.RUNNING)
            .join(builder.table(HEAD_DIRECTION_TOPIC,Consumed.with(Serdes.String(),CustomSerdes.headDirectionUpdateSerde)),
                    (movementAndStateJoin, headDirection) ->
                    new Join<GameMovementKeyPressed,HeadDirectionUpdated>(movementAndStateJoin.l(),headDirection)
            )
            .mapValues( movementAndStateJoin ->
                 new Join<HeadDirectionUpdated,HeadDirectionUpdated>(
                        switch(movementAndStateJoin.l()){
                            case UP -> HeadDirectionUpdated.NORTH;
                            case DOWN -> HeadDirectionUpdated.SOUTH;
                            case RIGHT -> HeadDirectionUpdated.EAST;
                            case LEFT -> HeadDirectionUpdated.WEST;
                        }
                        ,movementAndStateJoin.r())
            )
            .filter((k, newAndCurrentHeadDirection) -> {

                HeadDirectionUpdated newDirection = newAndCurrentHeadDirection.l();
                HeadDirectionUpdated currentDirection = newAndCurrentHeadDirection.r();

                // return true if current is not the same new and if they are not opposite
                return newDirection != currentDirection &&
                        switch (currentDirection){
                            case NORTH -> newDirection != HeadDirectionUpdated.SOUTH;
                            case SOUTH -> newDirection != HeadDirectionUpdated.NORTH;
                            case EAST -> newDirection != HeadDirectionUpdated.WEST;
                            case WEST -> newDirection != HeadDirectionUpdated.EAST;
                        };

            })
            .mapValues( newAndCurrentHeadDirection -> newAndCurrentHeadDirection.l())
            .to(HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(),CustomSerdes.headDirectionUpdateSerde));

                /*
                HEAD  -- LEFT  -- RIGHT -- UP    -- DOWN
                NORTH -- WEST  -- EAST  -- X     -- X
                SOUTH -- WEST  -- EAST  -- X     -- X
                WEST  -- X     -- X     -- NORTH -- SOUTH

                 */


                /*
                    eventstream up, left, right down
                    filter gamestatus is running
                            filter on valid movement given HeadDirection Table
                    Update HeadDirection Table
                 */

    }

}
