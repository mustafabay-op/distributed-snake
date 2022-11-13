package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Builder;
import lombok.Getter;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static op.kompetensdag.snake.Topics.*;
@Builder
public class TickProcessor {


    private GameTick gameTick;
    private HeadDirection headDirection;
    private GameTableEntry snakeHead;
    private GameTableEntry snakeTail;

    @Getter
    private String gameId;

    private GameTableEntry newPositionEntry;

    public TablePosition getNewHeadPosition(){
        return switch(headDirection){
            case NORTH -> TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY()+1).build();
            case SOUTH -> TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY()-1).build();
            case EAST -> TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX()+1).build();
            case WEST -> TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX()-1).build();
            default -> snakeHead.getPosition();
        };
    }

    public boolean isNewHeadPositionTaken() {
        return Optional.ofNullable(newPositionEntry).map(entry -> entry.getBusy() ).orElse(false);
    }

    public Iterable<GameTableEntry> moveSnake(){
        // 1. add new snake head

        GameTableEntry addHead = GameTableEntry.newBuilder(snakeHead).setPosition(getNewHeadPosition()).build();

        // 2. invalidate snake tail

        GameTableEntry removeTail = GameTableEntry.newBuilder(snakeTail).setBusy(false).build();

        return List.of(addHead,removeTail);

    }

    public ProcessTickCommand toSerializableObject(){
        return new ProcessTickCommand(gameId,gameTick,headDirection,snakeHead,snakeTail);
    }

    public static TickProcessor.TickProcessorBuilder fromProcessTickCommand(ProcessTickCommand cmd,GameTableEntry newPositionEntry){
        return builder().gameId(cmd.getGameId())
                .gameTick(cmd.getGameTick())
                .headDirection(cmd.getHeadDirection())
                .snakeHead(cmd.getSnakeHead())
                .snakeTail(cmd.getSnakeTail())
                .newPositionEntry(newPositionEntry);
    }

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps,KTable<String, HeadDirectionRecord> headDirectionRecordKTable3){

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

        SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde = new SpecificAvroSerde<>();
        processTickCommandSerde.configure(schemaRegistryProps, false);

        KStream<String,GameTableEntry> tableEntryLog =
                builder
                        .stream(GAME_TABLE_ENTRIES, Consumed.with(Serdes.String(), gameTableEntrySerde));

        KTable<String,GameTableEntry> snakeHead =
                tableEntryLog
                        .filter( (game,tableEntry) -> tableEntry.getType() == GameTableEntryType.SNAKE && tableEntry.getBusy() == true )
                        .groupByKey()
                        .reduce( (current,next) -> next, Named.as("snake-head"),Materialized.with(Serdes.String(),gameTableEntrySerde));

        snakeHead
                .toStream()
                .mapValues(v -> "Snake head: " + v)
                .to(GAME_OUTPUT);

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
                        .filter( (game,entries) -> !entries.getEntries().isEmpty())
                        .mapValues( v -> v.getEntries().get(0));

        snakeTail
                .toStream()
                .mapValues(v -> "Snake tail: " + v)
                .to(GAME_OUTPUT);

        KTable<GameTablePosition,GameTableEntry> positionUsage  =
                tableEntryLog
                        .groupBy( (gameId,entry) -> new GameTablePosition(entry.getPosition(),gameId),
                                Grouped.with(gameTablePositionSerde,gameTableEntrySerde))
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
                .mapValues( (game,tick) -> TickProcessor.builder().gameId(game).gameTick(tick))
                .join(headDirectionRecordKTable3,(cmdBuilder,direction) -> cmdBuilder.headDirection(direction.getType()))
                .join(snakeHead,(cmdBuilder,head) -> cmdBuilder.snakeHead(head))
                .join(snakeTail,(cmdBuilder,tail) -> cmdBuilder.snakeTail(tail))
                .selectKey((gameId,cmdBuilder) -> new GameTablePosition(cmdBuilder.build().getNewHeadPosition(),gameId))
                // change value to persistent avro entity
                .mapValues((tablePosition,cmdBuilder) -> cmdBuilder.build().toSerializableObject())
//                .groupByKey(Grouped.keySerde(gameTablePositionSerde))
                .leftJoin(positionUsage,
                            (readOnlyKey, processTickCommand, gameTableEntry) -> fromProcessTickCommand(processTickCommand,gameTableEntry),
                            Joined.with(gameTablePositionSerde,processTickCommandSerde,gameTableEntrySerde))
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
                                    .flatMapValues( cmdBuilder -> cmdBuilder.build().moveSnake())
                                    .to(GAME_TABLE_ENTRIES,Produced.with(Serdes.String(),gameTableEntrySerde))
                        ));

    }


}
