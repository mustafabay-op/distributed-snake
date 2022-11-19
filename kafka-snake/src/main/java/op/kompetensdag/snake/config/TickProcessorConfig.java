package op.kompetensdag.snake.config;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.model.*;
import op.kompetensdag.snake.processors.TickProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static op.kompetensdag.snake.config.Topics.GAME_TABLE_ENTRIES;

@Configuration
public class TickProcessorConfig {

    public static final String SNAKE_HEAD_REDUCE_NAME = "snake-head";

    @Bean
    public KStream<String, GameTableEntry> tableEntryLog(final StreamsBuilder streamsBuilder,
                                                         final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return streamsBuilder.stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde));
    }

    @Bean
    public KTable<String, GameTableEntry> snakeHead(final KStream<String, GameTableEntry> tableEntryLog,
                                                    final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return tableEntryLog
                // Here we want to calculate the position of game head.
                // 1. Firstly we need to make sure that the game table entry is of the SNAKE type
                // and that the position is occupied/busy.
                // 2. Next up we need to aggregate the stream to a table and emit the latest entry.
                //    For that we can use the reduce function. But before doing an aggregation
                //    we need to group the stream by key to make sure that we are acting on specific games.
                //    For that we use the groupByKey operation.

                // Add your code above this line.
                .groupByKey()
                .reduce(reduce(), Named.as(SNAKE_HEAD_REDUCE_NAME), Materialized.with(Serdes.String(), gameTableEntrySerde));
    }

    private Reducer reduce() {
        //Implement this, remove return null
        return null;
    }

    @Bean
    public KTable<String, GameTableEntry> snakeTail(final KStream<String, GameTableEntry> tableEntryLog,
                                                    final SpecificAvroSerde<GameTablePosition> gameTablePositionSerde,
                                                    final SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde) {
        return tableEntryLog
                // Here we want to calculate the position of game tail.
                // 1. Firstly we need to make sure that the game table entry is of the SNAKE type
                // 2. Now we want to aggregate all the entries and add it to a list of entries if the entry is busy
                //    When we want to aggregate a type but return a different type, we use the .aggregate() operation
                // 3. At this point we want to filter out empty lists and map the list back to a GameTableEntry
                //    Hint: Use the builder on GameTableEntries

                // Add your code above this line.
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
                      final KTable<String, HeadDirectionRecord> headDirectionRecordKTable,
                      final KTable<String, GameTableEntry> snakeTail,
                      final KTable<String, GameTableEntry> snakeHead,
                      final KTable<GameTablePosition, GameTableEntry> positionUsage
    ) {

        return streamsBuilder.stream(Topics.GAME_TICKS, Consumed.with(Serdes.String(), tickSerde))
                .mapValues((game, tick) -> TickProcessor.builder().gameId(game).gameTick(tick))
                .join(headDirectionRecordKTable, (cmdBuilder, direction) -> cmdBuilder.headDirection(direction.getType()))
                .join(snakeHead, TickProcessor.TickProcessorBuilder::snakeHead)
                .join(snakeTail, TickProcessor.TickProcessorBuilder::snakeTail)
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
