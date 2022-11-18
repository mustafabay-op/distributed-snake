package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.commands.ProcessAdminCommand;
import op.kompetensdag.snake.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

import static op.kompetensdag.snake.config.Topics.*;

@Configuration
public class AdministrationProcessorConfig {

    @Bean
    public Object gameStatueProcessingTopology(final StreamsBuilder streamsBuilder,
                                                final SpecificAvroSerde<GameAdministrationCommandRecord> gameAdminSerde,
                                                final SpecificAvroSerde<GameStatusRecord> gameStatusSerde,
                                                final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde,
                                                final SpecificAvroSerde<HeadDirectionRecord> headDirSerde,
                                                final KTable<String, GameStatusRecord> gameStatusKTable) {


        streamsBuilder.stream(GAME_ADMINISTRATION_COMMANDS_TOPIC, Consumed.with(Serdes.String(), gameAdminSerde))
                .mapValues((gameId, gameAdminCommand) -> ProcessAdminCommand.builder().gameId(gameId).gameAdministrationCommand(gameAdminCommand.getType()))
                .leftJoin(gameStatusKTable, (cmdBuilder, gameStatus) -> cmdBuilder.gameStatus(Optional.ofNullable(gameStatus).map(GameStatusRecord::getType).orElse(null))) //gameStatus.getType()))
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
                .to(HEAD_DIRECTION_TOPIC, Produced.with(Serdes.String(), headDirSerde));

        return null;
    }

    @Bean
    KTable<String, GameStatusRecord> gameStatusKTable(final StreamsBuilder streamsBuilder,
                                                      final SpecificAvroSerde<GameStatusRecord> gameStatusSerde) {
        return streamsBuilder.table(GAME_STATUS_TOPIC, Consumed.with(Serdes.String(), gameStatusSerde));
    }
}
