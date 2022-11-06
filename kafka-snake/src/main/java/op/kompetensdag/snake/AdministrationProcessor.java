package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class AdministrationProcessor {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatus> gameStatusKTable) {
        SpecificAvroSerde<GameStatus> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<GameAdministrationCommand> gameAdminSerde = new SpecificAvroSerde<>();
        gameAdminSerde.configure(schemaRegistryProps, false);

     /*   builder
                .stream(GAME_MOVEMENT_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameMovementSerde))
                .join(builder.table(GAME_STATUS_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde)), Join::new)
                .filter((k, movementAndStateJoin) -> movementAndStateJoin.r().getSTATUS().equals("RUNNING"))
                .join(builder.table(HEAD_DIRECTION_TOPIC, Consumed.with(Serdes.String(), headDirSerde)), (movementAndStateJoin, headDirection) -> new Join<>(movementAndStateJoin.l(), headDirection))
                .mapValues(movementAndStateJoin -> new Join<>(getIntendedHeadDirection(movementAndStateJoin.l()), movementAndStateJoin.r()))
                .filter((k, intendedAndCurrentHeadDirection) -> isIntendedMoveValid(intendedAndCurrentHeadDirection))
                .mapValues(Join::l)
                .to(HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));
    }*/

        // SKIPING RECORD DUE TO NULL KEY!!!
        builder
                .stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .peek((k, v) -> System.out.println("23213823821"))
                .mapValues(v -> v.getKEYPRESSED().equals("SPACE") ? new GameStatus("RUNNING") : null)
                .toTable(Named.as("GAME_STATUS_TABLE"));
                //.to(GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde));

/*        builder
                .stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .join(gameStatusKTable, Join::new)
                .mapValues(adminCommandAndGameStatusJoin -> getNewGameStatus())
                .to(GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde));*/

        gameStatusKTable.mapValues(v -> v.STATUS + " :::: " + v.getSTATUS() + " Processed " + v.getClass()).toStream().to(GAME_OUTPUT);

/*        builder.stream(gameStatusKTable.toStream(Named.as("sdd")), Consumed.with(Serdes.String(), gameStatusSerde))
                .mapValues(v -> v.STATUS + " : " + v.getSTATUS() + "processed " + v.getClass())
                .to(GAME_OUTPUT);*/

/*        builder
                .stream(GAME_STATUS_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde))
                .join(builder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde)),
                        (gameStatus, gameAdministrationCommand) -> new Join<GameStatus, GameAdministrationCommand>(gameStatus, gameAdministrationCommand))
                .to("23");*/


/*
        KStream<String, GameAdministrationKeyPressed> gameAdministrationKeyPressedKStream = builder.stream(GAME_ADMINISTRATION_COMMANDS,
                Consumed.with(Serdes.String(), CustomSerdes.gameAdministrationKeyPressedValueSerde()));

        KStream<String, GameStatus> gameStatusKStream =
                builder
                        .stream(GAME_STATUS, Consumed.with(Serdes.String(), op.kompetensdag.kafkasnake.CustomSerdes.gameStatusSerde()))
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
                        Materialized.with(Serdes.String(), op.kompetensdag.kafkasnake.CustomSerdes.gameStatusSerde())
                );

        KTable<String, HeadDirection> headDirectionKTable = builder
                .stream("headDirection", Consumed.with(Serdes.String(), op.kompetensdag.kafkasnake.CustomSerdes.headDirectionSerde()))
                .toTable();

        //.to(GAME_MOVEMENT_COMMANDS, Produced.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedValueSerde()))));
    KStream<String, HeadDirection> headDirectionKStream = headDirectionKTable
            .filter((k, v) -> v.equals(GameStatus.RUNNING))
*/

    }

    private static GameStatus getNewGameStatus() {
        return new GameStatus("INITIALIZING");
    }
}
