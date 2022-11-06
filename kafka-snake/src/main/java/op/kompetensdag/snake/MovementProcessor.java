package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.util.Join;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class MovementProcessor {


    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatus> gameStatusKTable) {

        SpecificAvroSerde<GameMovementCommand> gameMovementSerde = new SpecificAvroSerde<>();
        gameMovementSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<GameStatus> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<HeadDirection> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

        builder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementSerde))
                .join(gameStatusKTable, Join::new)
                .filter((k, movementAndStateJoin) -> movementAndStateJoin.r().getSTATUS().equals("RUNNING"))
                .join(builder.table(HEAD_DIRECTION_TOPIC, Consumed.with(Serdes.String(), headDirSerde)), (movementAndStateJoin, headDirection) -> new Join<>(movementAndStateJoin.l(), headDirection))
                .mapValues(movementAndStateJoin -> new Join<>(getIntendedHeadDirection(movementAndStateJoin.l()), movementAndStateJoin.r()))
                .filter((k, intendedAndCurrentHeadDirection) -> isIntendedMoveValid(intendedAndCurrentHeadDirection))
                .mapValues(Join::l)
                .to(HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));
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

    private static HeadDirection getIntendedHeadDirection(GameMovementCommand gameMovementCommand) {
        return switch (gameMovementCommand.getKEYPRESSED()) {
            case "UP" -> new HeadDirection("NORTH");
            case "DOWN" -> new HeadDirection("SOUTH");
            case "RIGHT" -> new HeadDirection("EAST");
            case "LEFT" -> new HeadDirection("WEST");
            default -> null;
        };
    }
}
