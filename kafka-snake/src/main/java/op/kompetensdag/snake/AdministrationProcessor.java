package op.kompetensdag.snake;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import static op.kompetensdag.snake.Topics.*;

public class AdministrationProcessor {

    public static void define(final StreamsBuilder builder){
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

        */


        //.to(GAME_MOVEMENT_COMMANDS, Produced.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedValueSerde()))));
/*    KStream<String, HeadDirection> headDirectionKStream = headDirectionKTable
            .filter((k, v) -> v.equals(GameStatus.RUNNING))*/



    }


}
