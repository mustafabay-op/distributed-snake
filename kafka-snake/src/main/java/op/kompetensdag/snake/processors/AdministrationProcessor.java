package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.commands.ProcessAdminCommand;
import op.kompetensdag.snake.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

import static op.kompetensdag.snake.Topics.*;

public class AdministrationProcessor {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatusRecord> gameStatusKTable, KTable<String, HeadDirectionRecord> headDirectionRecordKTable3) {

        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<GameAdministrationCommandRecord> gameAdminSerde = new SpecificAvroSerde<>();
        gameAdminSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<GameTableEntry> gameTableEntrySerde = new SpecificAvroSerde<>();
        gameTableEntrySerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

        builder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .mapValues((gameId, gameAdminCommand) -> ProcessAdminCommand.builder().gameId(gameId).gameAdministrationCommand(gameAdminCommand.getType()))
                .join(gameStatusKTable, (cmdBuilder, gameStatus) -> cmdBuilder.gameStatus(GameStatus.ENDED)) //gameStatus.getType()))
                .split()
                .branch((gameId, cmdBuilder) -> cmdBuilder.build().getGameStatus().equals(GameStatus.ENDED),
                        Branched.withConsumer(cmdBuilder ->
                                cmdBuilder
                                        .mapValues(value -> new GameStatusRecord(GameStatus.INITIALIZING))
                                        .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde))))
                .branch((gameId, cmdBuilder) -> cmdBuilder.build().getGameStatus().equals(GameStatus.RUNNING),
                        Branched.withConsumer(cmdBuilder ->
                                cmdBuilder
                                        .mapValues(value -> new GameStatusRecord(GameStatus.PAUSED))
                                        .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde))))
                .branch((gameId, cmdBuilder) -> cmdBuilder.build().getGameStatus().equals(GameStatus.PAUSED),
                        Branched.withConsumer(cmdBuilder ->
                                cmdBuilder.mapValues(value -> new GameStatusRecord(GameStatus.RUNNING))
                                        .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde))))
                .defaultBranch(
                        Branched.withConsumer(cmdBuilder ->
                                cmdBuilder.mapValues(value -> "Illegal game administration argument: cmdBuilder=" + value + " build: " + value.build() + " GameAdminCommand: " + value.build().getGameAdministrationCommand())
                                        .to(Topics.ILLEGAL_ARGUMENTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()))));

        gameStatusKTable
                .toStream()
                .filter((key, value) -> value.getType().equals(GameStatus.INITIALIZING))
                .mapValues(value -> new GameStatusRecord(GameStatus.RUNNING))
                .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde));

        gameStatusKTable
                .toStream()
                .filter((key, value) -> value.getType().equals(GameStatus.INITIALIZING))
                .flatMapValues(value -> ProcessAdminCommand.builder().build().initializeGame())
                .to(Topics.GAME_TABLE_ENTRIES, Produced.with(Serdes.String(), gameTableEntrySerde));

        gameStatusKTable
                .toStream()
                .filter((key, value) -> value.getType().equals(GameStatus.INITIALIZING))
                .mapValues(value -> new HeadDirectionRecord(HeadDirection.NORTH))
                .to(HEAD_DIRECTION_TOPIC_3, Produced.with(Serdes.String(), headDirSerde));

        gameStatusKTable
                .toStream()
                .mapValues(v -> "GameStatus: " + v)
                .to(GAME_OUTPUT);

        builder.stream(ILLEGAL_ARGUMENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(v -> "IllegalArguments: " + v)
                .to(GAME_OUTPUT);

        builder.stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde))
                .mapValues(v -> "GameTableEntries: " + v)
                .to(GAME_OUTPUT);
    }
}
