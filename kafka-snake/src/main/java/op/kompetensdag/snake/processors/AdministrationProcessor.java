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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

import static op.kompetensdag.snake.Topics.*;

@Component
public class AdministrationProcessor {

    private static StreamsBuilder streamsBuilder;

    private static SpecificAvroSerde<GameStatusRecord> gameStatusSerde;
    private static SpecificAvroSerde<GameAdministrationCommandRecord> gameAdminSerde;
    private static SpecificAvroSerde<GameTableEntry> gameTableEntrySerde;
    private static SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();

    private static KTable<String, GameStatusRecord> gameStatusKTable;

    @Autowired
    public AdministrationProcessor(final StreamsBuilder streamsBuilder,
                                   final SpecificAvroSerde<GameAdministrationCommandRecord> gameAdminSerde,
                                   final SpecificAvroSerde<GameStatusRecord> gameStatusSerde,
                                   final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde,
                                   final SpecificAvroSerde<HeadDirectionRecord> headDirSerde,
                                   final KTable<String, GameStatusRecord> gameStatusKTable) {
        AdministrationProcessor.streamsBuilder = streamsBuilder;
        AdministrationProcessor.gameAdminSerde = gameAdminSerde;
        AdministrationProcessor.gameStatusSerde = gameStatusSerde;
        AdministrationProcessor.gameTableEntrySerde = gameTableEntrySerde;
        AdministrationProcessor.headDirSerde = headDirSerde;
        AdministrationProcessor.gameStatusKTable = gameStatusKTable;
    }

    public static void define() {


        streamsBuilder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .mapValues((gameId, gameAdminCommand) -> ProcessAdminCommand.builder().gameId(gameId).gameAdministrationCommand(gameAdminCommand.getType()))
                .leftJoin(gameStatusKTable, (cmdBuilder, gameStatus) -> cmdBuilder.gameStatus(Optional.ofNullable(gameStatus).map(gs -> gs.getType()).orElse(null))) //gameStatus.getType()))
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
    }
}
