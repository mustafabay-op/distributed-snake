package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.*;
import op.kompetensdag.snake.processors.TickProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static op.kompetensdag.snake.Topics.GAME_TABLE_ENTRIES;

@Configuration
public class TickProcessorConfig {

    @Bean
    public KStream<String, GameTableEntry> tableEntryLog(final StreamsBuilder streamsBuilder,
                                                         final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return streamsBuilder.stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde));
    }

    @Bean
    public KTable<String, GameTableEntry> snakeHead(final KStream<String, GameTableEntry> tableEntryLog,
                                                    final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return tableEntryLog
                .filter((game, tableEntry) -> tableEntry.getType() == GameTableEntryType.SNAKE && tableEntry.getBusy() == true)
                .groupByKey()
                .reduce((current, next) -> next, Named.as("snake-head"), Materialized.with(Serdes.String(), gameTableEntrySerde));
    }

    @Bean
    public KTable<String, GameTableEntry> snakeTail(final KStream<String, GameTableEntry> tableEntryLog,
                                                    final SpecificAvroSerde<GameTablePosition> gameTablePositionSerde,
                                                    final SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde) {
        return tableEntryLog
                .filter((game, tableEntry) -> tableEntry.getType() == GameTableEntryType.SNAKE)
                .groupByKey()
                .aggregate(() -> GameSnakeEntries.newBuilder().setEntries(new ArrayList<>()).build(), (game, newEntry, entries) -> {
                    List<GameTableEntry> list = entries.getEntries();
                    if (newEntry.getBusy()) {
                        list.add(newEntry);
                    } else {
                        list.remove(0);
                    }
                    entries.setEntries(list);
                    return entries;
                }, Materialized.with(Serdes.String(), gameTableEntriesSerde))
                .filter((game, entries) -> !entries.getEntries().isEmpty())
                .mapValues(v -> v.getEntries().get(0));

    }

    @Bean
    public KTable<GameTablePosition, GameTableEntry> positionUsage(final KStream<String, GameTableEntry> tableEntryLog,
                                                                   final SpecificAvroSerde<GameTablePosition> gameTablePositionSerde,
                                                                   final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return tableEntryLog
                .groupBy((gameId, entry) -> new GameTablePosition(entry.getPosition(), gameId),
                        Grouped.with(gameTablePositionSerde, gameTableEntrySerde))
                .reduce((currentValue, next) -> next);

    }

    @Bean
    public Map tickProcessingTopology(final StreamsBuilder streamsBuilder,
                      final SpecificAvroSerde<GameTablePosition> gameTablePositionSerde,
                      final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde,
                      final SpecificAvroSerde<GameTick> tickSerde,
                      final SpecificAvroSerde<GameStatusRecord> gameStatusSerde,
                      final SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde,
                      final KTable<String, HeadDirectionRecord> headDirectionRecordKTable3,
                      final KTable<String, GameTableEntry> snakeTail,
                      final KTable<String, GameTableEntry> snakeHead,
                      final KTable<GameTablePosition, GameTableEntry> positionUsage
    ) {

        return streamsBuilder.stream(Topics.GAME_TICKS, Consumed.with(Serdes.String(), tickSerde))
                .mapValues((game, tick) -> TickProcessor.builder().gameId(game).gameTick(tick))
                .join(headDirectionRecordKTable3, (cmdBuilder, direction) -> cmdBuilder.headDirection(direction.getType()))
                .join(snakeHead, (cmdBuilder, head) -> cmdBuilder.snakeHead(head))
                .join(snakeTail, (cmdBuilder, tail) -> cmdBuilder.snakeTail(tail))
                .selectKey((gameId, cmdBuilder) -> new GameTablePosition(cmdBuilder.build().getNewHeadPosition(), gameId))
                .mapValues((tablePosition, cmdBuilder) -> cmdBuilder.build().toSerializableObject())
                .leftJoin(positionUsage,
                        (readOnlyKey, processTickCommand, gameTableEntry) -> TickProcessor.fromProcessTickCommand(processTickCommand, gameTableEntry),
                        Joined.with(gameTablePositionSerde, processTickCommandSerde, gameTableEntrySerde))
                .selectKey((k, v) -> v.build().getGameId())
                .split()
                .branch((game, cmdBuilder) -> cmdBuilder.build().isNewHeadPositionTaken(),
                        Branched.withConsumer((builderKStream) ->
                                builderKStream
                                        .mapValues((v) -> new GameStatusRecord(GameStatus.ENDED))
                                        .to(Topics.GAME_STATUS_TOPIC, Produced.with(Serdes.String(), gameStatusSerde))))
                .defaultBranch(
                        Branched.withConsumer((builderKStream) ->
                                builderKStream
                                        .flatMapValues(cmdBuilder -> cmdBuilder.build().moveSnake())
                                        .to(GAME_TABLE_ENTRIES, Produced.with(Serdes.String(), gameTableEntrySerde))
                        ));

    }
}
