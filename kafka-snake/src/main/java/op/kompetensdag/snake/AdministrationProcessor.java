package op.kompetensdag.snake;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class AdministrationProcessor {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatus> gameStatusKTable) {
        SpecificAvroSerde<GameStatus> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);
        SpecificAvroSerde<GameAdministrationCommand> gameAdminSerde = new SpecificAvroSerde<>();
        gameAdminSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<HeadDirection> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

        builder
                .stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .peek((k, v) -> System.out.println("23213823821"))
                .mapValues(v -> v.getKEYPRESSED().equals("SPACE") ? new GameStatus("RUNNING") : new GameStatus("NULL"))
                .to(GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde));

        builder
                .stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .peek((key, value) -> System.out.println("About to set head Direction to north if count equal 1."))
                .groupByKey()
                .count()
                .toStream()
                .peek((key, value) -> System.out.println("About filter on count 1. key/value: " + key + " " + value))
                .filter((key, value) -> value.equals(1L))
                .peek((key, value) -> System.out.println("After filtering on count, setting head Direction to north."))
                .mapValues(value -> new HeadDirection("NORTH"))
                .to(HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));

        gameStatusKTable.toStream().mapValues(v -> v.STATUS + " :::: " + v.getSTATUS() + " Processed " + v.getClass()).to(GAME_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));






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
