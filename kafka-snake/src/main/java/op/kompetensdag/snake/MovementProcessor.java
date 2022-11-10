package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.GameMovementKeyPressed;
import op.kompetensdag.snake.model.GameMovementKeyPressedRecord;
import op.kompetensdag.snake.model.GameStatus;
import op.kompetensdag.snake.model.GameStatusRecord;
import op.kompetensdag.snake.util.Join;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class MovementProcessor {


    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatusRecord> gameStatusKTable) {

        SpecificAvroSerde<GameMovementKeyPressedRecord> gameMovementKeyPressedSerde = new SpecificAvroSerde<>();
        gameMovementKeyPressedSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<HeadDirection> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

        KTable<String, HeadDirection> headDirectionKTable = builder.table(HEAD_DIRECTION_TOPIC, Consumed.with(Serdes.String(), headDirSerde));
        // headDirectionKTable.mapValues(v -> "HeadDir: " + v + ", Dir: " + v.getDIRECTION()).toStream().to(GAME_OUTPUT);


        // ValueJoiner<GameMovementKeyPressedRecord, GameStatus, Join<GameMovementKeyPressedRecord, GameStatus>> valueJoiner = (movementCommand, gameStatus) -> new Join<>(movementCommand, gameStatus);



        builder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementKeyPressedSerde))
                .peek((key, value) -> System.out.println("Before join movementKey with headDirection, key: " + key + " value: " + value))
/*              .join(gameStatusKTable, valueJoiner)
                .filter((k, movementAndStateJoin) -> movementAndStateJoin.r().getSTATUS().equals("RUNNING"))
                .join(builder.table(HEAD_DIRECTION_TOPIC, Consumed.with(Serdes.String(), headDirSerde)), (movementAndStateJoin, headDirection) -> new Join<>(movementAndStateJoin.l(), headDirection))*/
  //              .mapValues(value -> value.toString())
                .to("game_output_2");
/*                .join(headDirectionKTable, (movementCommand, headDirection) -> new Join<>(movementCommand, headDirection)) // Join::new) // , Consumed.with(Serdes.String(), headDirSerde)), (movementCommand, headDirection) -> new Join<>(movementCommand, headDirection))
                .peek((key, value) -> System.out.println("After join movementKey with headDirection, key: " + key + " value: " + value))
                .mapValues(movementAndHeadDirectionJoin -> new Join<>(getIntendedHeadDirection(movementAndHeadDirectionJoin.l().getType()), movementAndHeadDirectionJoin.r()))
                .filter((k, intendedAndCurrentHeadDirection) -> isIntendedMoveValid(intendedAndCurrentHeadDirection))
                .mapValues(Join::l)
                .to(HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));*/

    }

    private static boolean isIntendedMoveValid(Join<HeadDirection, HeadDirection> intendedAndCurrentHeadDirection) {
        return (intendedAndCurrentHeadDirection.l() != intendedAndCurrentHeadDirection.r()) && (!isOpposite(intendedAndCurrentHeadDirection.l(), intendedAndCurrentHeadDirection.r()));
    }

    private static boolean isOpposite(HeadDirection newDirection, HeadDirection currentDirection) {
        return switch (currentDirection.getDIRECTION()) {
            case "NORTH" -> newDirection.getDIRECTION().equals("SOUTH");
            case "SOUTH" -> newDirection.getDIRECTION().equals("NORTH");
            case "EAST" -> newDirection.getDIRECTION().equals("WEST");
            case "WEST" -> newDirection.getDIRECTION().equals("EAST");
            default -> true;
        };
    }

    private static HeadDirection getIntendedHeadDirection(GameMovementKeyPressed gameMovementKeyPressed) {
        return switch (gameMovementKeyPressed) {
            case UP -> new HeadDirection("NORTH");
            case DOWN -> new HeadDirection("SOUTH");
            case RIGHT -> new HeadDirection("EAST");
            case LEFT -> new HeadDirection("WEST");
            default -> null;
        };
    }
}
