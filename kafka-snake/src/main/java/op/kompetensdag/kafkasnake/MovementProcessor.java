package op.kompetensdag.kafkasnake;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;


import static op.kompetensdag.kafkasnake.Constants.*;

public class MovementProcessor {

    final StreamsBuilder builder = new StreamsBuilder();

    KStream<String, GameAdministrationKeyPressed> gameAdministrationKeyPressedKStream = builder.stream(GAME_ADMINISTRATION_COMMANDS,
            Consumed.with(Serdes.String(), CustomSerdes.gameAdministrationKeyPressedValueSerde()));

    KStream<String, GameStatus> gameStatusKStream =
            builder
                    .stream(GAME_STATUS, Consumed.with(Serdes.String(), CustomSerdes.gameStatusSerde()))
                    .join(gameAdministrationKeyPressedKStream, (gameStatus, adminKey) -> {
                        if (!adminKey.equals(GameAdministrationKeyPressed.SPACE)) {
                            throw new IllegalArgumentException();
                        } else if (gameStatus.equals(GameStatus.ENDED)) {
                            return GameStatus.INITIALIZING;
                        } else if (gameStatus.equals(GameStatus.PAUSED)) {
                            return GameStatus.RUNNING;
                        } else if (gameStatus.equals(GameStatus.RUNNING)) {
                            return GameStatus.PAUSED;
                        } else {
                            throw new IllegalArgumentException();
                        }
                    }, null);

    KTable<String, GameStatus> gameStatusKTable = gameStatusKStream
            .groupByKey()
            .reduce(
                    (value1, value2) -> value2,
                    Materialized.with(Serdes.String(), CustomSerdes.gameStatusSerde())
            );

    KTable<String, HeadDirection> headDirectionKTable = builder
            .stream("headDirection", Consumed.with(Serdes.String(), CustomSerdes.headDirectionSerde()))
            .toTable();

    //.to(GAME_MOVEMENT_COMMANDS, Produced.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedValueSerde()))));
/*    KStream<String, HeadDirection> headDirectionKStream = headDirectionKTable
            .filter((k, v) -> v.equals(GameStatus.RUNNING))*/


    KStream<String, HeadDirection> headDirectionKStream = builder
            .stream(GAME_MOVEMENT_COMMANDS, Consumed.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedValueSerde()))
            .join(gameStatusKTable, (gameMovementKeyPressedValues, gameStatus) -> {
                if (gameStatus.equals(GameStatus.RUNNING)) {
                    return new TupleGameMovementIsGameRunning(gameMovementKeyPressedValues, true);
                }
                return new TupleGameMovementIsGameRunning(gameMovementKeyPressedValues, false);
            })
            .filter((k, v) -> v.isGameRunning)
            .join(headDirectionKTable, (tupleGameMovementIsGameRunning, headDirection) -> {
                GameMovementKeyPressed gameMovementKeyPressed = tupleGameMovementIsGameRunning.gameMovementKeyPressed;
                boolean goNorth = gameMovementKeyPressed.equals(GameMovementKeyPressed.UP);
                boolean goSouth = gameMovementKeyPressed.equals(GameMovementKeyPressed.DOWN);
                boolean goEast = gameMovementKeyPressed.equals(GameMovementKeyPressed.RIGHT);
                boolean goWest = gameMovementKeyPressed.equals(GameMovementKeyPressed.LEFT);

                boolean headingWest = headDirection.equals(HeadDirection.WEST);
                boolean headingEast = headDirection.equals(HeadDirection.EAST);
                boolean headingNorth = headDirection.equals(HeadDirection.NORTH);
                boolean headingSouth = headDirection.equals(HeadDirection.SOUTH);

                boolean headingWestOrEast = headingWest || headingEast;
                boolean headingNorthOrSouth = headingNorth || headingSouth;

                if (goNorth && headingWestOrEast) {
                    return new TupleHeadDirectionIsGameMovementValid(HeadDirection.NORTH, true);
                } else if (goSouth && headingWestOrEast) {
                    return new TupleHeadDirectionIsGameMovementValid(HeadDirection.SOUTH, true);
                } else if (goWest && headingNorthOrSouth) {
                    return new TupleHeadDirectionIsGameMovementValid(HeadDirection.WEST, true);
                } else if (goEast && headingNorthOrSouth) {
                    return new TupleHeadDirectionIsGameMovementValid(HeadDirection.EAST, true);
                } else {
                    return new TupleHeadDirectionIsGameMovementValid(null, false);
                }})
            .filter((k, v) -> v.isGameMovementValid)
            .mapValues(v -> v.headDirection);

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
