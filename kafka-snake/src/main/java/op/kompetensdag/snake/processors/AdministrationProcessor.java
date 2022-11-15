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
import java.util.Optional;

import static op.kompetensdag.snake.Topics.GAME_ADMINISTRATION_COMMANDS_TOPIC;
import static op.kompetensdag.snake.Topics.HEAD_DIRECTION_TOPIC_3;

public class AdministrationProcessor {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, GameStatusRecord> gameStatusKTable) {

        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameAdministrationCommandRecord> gameAdminSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameTableEntry> gameTableEntrySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();

        gameStatusSerde.configure(schemaRegistryProps, false);
        gameAdminSerde.configure(schemaRegistryProps, false);
        gameTableEntrySerde.configure(schemaRegistryProps, false);
        headDirSerde.configure(schemaRegistryProps, false);

        builder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .mapValues((gameId, gameAdminCommand) -> ProcessAdminCommand.builder().gameId(gameId).gameAdministrationCommand(gameAdminCommand.getType()))
                .leftJoin(gameStatusKTable, (cmdBuilder, gameStatus) -> cmdBuilder.gameStatus(Optional.ofNullable(gameStatus).map(GameStatusRecord::getType).orElse(null)))
                .split()
                .branch((gameId, cmdBuilder) -> cmdBuilder.build().shouldInitializeGame(),
                        Branched.withConsumer(cmdBuilder ->
                                cmdBuilder
                                        .mapValues(value -> new GameStatusRecord(GameStatus.INITIALIZING))
                                        .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde))))
                .branch((gameId, cmdBuilder) -> cmdBuilder.build().shouldPauseGame(),
                        Branched.withConsumer(cmdBuilder ->
                                cmdBuilder
                                        .mapValues(value -> new GameStatusRecord(GameStatus.PAUSED))
                                        .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde))))
                .branch((gameId, cmdBuilder) -> cmdBuilder.build().shouldResumeGame(),
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

/*        gameStatusKTable
                .toStream()
                .mapValues(v -> "GameStatus: " + v)
                .to(GAME_OUTPUT);

        builder.stream(ILLEGAL_ARGUMENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(v -> "IllegalArguments: " + v)
                .to(GAME_OUTPUT);

        builder.stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde))
                .mapValues(v -> "GameTableEntries: " + v)
                .to(GAME_OUTPUT);*/
    }
}
