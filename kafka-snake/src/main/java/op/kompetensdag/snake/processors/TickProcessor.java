package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.commands.ProcessTickCommand;
import op.kompetensdag.snake.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static op.kompetensdag.snake.Topics.GAME_TABLE_ENTRIES;
import static op.kompetensdag.snake.Topics.HEAD_DIRECTION_TOPIC;

public class TickProcessor {

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps){

        SpecificAvroSerde<GameTablePosition> gameTablePositionSerde = new SpecificAvroSerde<>();
        gameTablePositionSerde.configure(schemaRegistryProps, true);

        SpecificAvroSerde<GameTableEntry> gameTableEntrySerde = new SpecificAvroSerde<>();
        gameTableEntrySerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde = new SpecificAvroSerde<>();
        gameTableEntriesSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<TablePosition> tablePositionSerde = new SpecificAvroSerde<>();
        tablePositionSerde.configure(schemaRegistryProps, true);

        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        headDirSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<GameTick> tickSerde = new SpecificAvroSerde<>();
        tickSerde.configure(schemaRegistryProps, false);

        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        gameStatusSerde.configure(schemaRegistryProps, false);

        KTable<String,HeadDirectionRecord> headDirection =
                builder.
                        table(HEAD_DIRECTION_TOPIC,Consumed.with(Serdes.String(),headDirSerde));


        KStream<String,GameTableEntry> tableEntryLog =
                builder
                        .stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde));

        KTable<String,GameTableEntry> snakeHead =
                tableEntryLog
                        .filter( (game,tableEntry) -> tableEntry.getType() == GameTableEntryType.SNAKE && tableEntry.getBusy() == true )
                        .groupByKey()
                        .reduce( (current,next) -> next, Named.as("snake-head"),Materialized.with(Serdes.String(),gameTableEntrySerde));

        KTable<String,GameTableEntry> snakeTail =
                tableEntryLog
                        .filter((game,tableEntry) -> tableEntry.getType() == GameTableEntryType.SNAKE)
                        .groupByKey()
                        .aggregate(()-> GameSnakeEntries.newBuilder().setEntries(new ArrayList<>()).build(),(game, newEntry, entries) -> {
                            List<GameTableEntry> list = entries.getEntries();
                            if(newEntry.getBusy()){
                                list.add(newEntry);
                            } else {
                                // if(!entries.getEntries().isEmpty()) // it should never be empty if it is not busy snake element
                                /*if(list.stream().anyMatch( listEntry -> listEntry.getPosition().equals(newEntry.getPosition()))) {
                                list = list.stream().
                                        dropWhile( listEntry -> listEntry.getPosition().equals(newEntry.getPosition()))
                                        .collect(Collectors.toList());*/
                                list.remove(0);
                            }
                            entries.setEntries(list);
                            return entries;
                        },Materialized.with(Serdes.String(),gameTableEntriesSerde))
                        .filter( (game,entries) -> entries.getEntries().isEmpty())
                        .mapValues( v -> v.getEntries().get(0));

        KTable<GameTablePosition,GameTableEntry> positionUsage  =
                tableEntryLog
                        .groupBy( (gameId,entry) -> new GameTablePosition(entry.getPosition(),gameId),
                                Grouped.keySerde(gameTablePositionSerde))
                        .reduce( (currentValue, next) -> next);


        /* KStream<String, GameStatus> gameStatusKStream =
                builder.stream("game-status",Consumed.with(Serdes.String(),gameStatusSerde));*/

        // join current game status? - will game status change ensure that ticking is stopped?
        // join current snake direction
        // join current snake head
        // join current snake tail
        // calculate new snake head using current heed and current direction
        // join all positions on the game table
        // check if new snake head position is clashing with current state of the position in table
        // emit a position busy event for head and position empty event for tail


        builder.stream(Topics.GAME_TICKS,Consumed.with(Serdes.String(),tickSerde))
                .mapValues( (game,tick) -> ProcessTickCommand.builder().gameId(game).gameTick(tick))
                .join(headDirection,(cmdBuilder,direction) -> cmdBuilder.headDirection(direction.getType()))
                .join(snakeHead,(cmdBuilder,head) -> cmdBuilder.snakeHead(head))
                .selectKey((gameId,cmdBuilder) -> new GameTablePosition(cmdBuilder.build().getNewHeadPosition(),gameId))
//                .groupByKey(Grouped.keySerde(gameTablePositionSerde))
                .leftJoin(positionUsage,
                            (readOnlyKey, cmdBuilder, gameTableEntry) -> cmdBuilder.newPositionEntry(gameTableEntry),
                            Joined.keySerde(gameTablePositionSerde))
                .selectKey((k,v) -> v.build().getGameId())
                .split()
                .branch((game,cmdBuilder) -> cmdBuilder.build().isNewHeadPositionTaken(),
                        Branched.withConsumer( (builderKStream) ->
                            builderKStream
                                    .mapValues( (v) -> new GameStatusRecord(GameStatus.ENDED))
                                    .to(Topics.GAME_STATUS_TOPIC,Produced.with(Serdes.String(),gameStatusSerde))))
                .defaultBranch(
                        Branched.withConsumer( (builderKStream) ->
                            builderKStream
                                    .join(snakeTail,(cmdBuilder,tail) -> cmdBuilder.snakeTail(tail))
                                    .flatMapValues( cmdBuilder -> cmdBuilder.build().moveSnake())
                                    .to(GAME_TABLE_ENTRIES,Produced.with(Serdes.String(),gameTableEntrySerde))
                        ));

    }


}
