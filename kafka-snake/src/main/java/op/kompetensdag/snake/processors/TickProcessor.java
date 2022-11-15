package op.kompetensdag.snake.processors;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Builder;
import lombok.Getter;
import op.kompetensdag.snake.Topics;
import op.kompetensdag.snake.model.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static op.kompetensdag.snake.Topics.*;

@Component
public class TickProcessor {

    private static StreamsBuilder streamsBuilder;

    private static SpecificAvroSerde<GameTablePosition> gameTablePositionSerde;
    private static SpecificAvroSerde<GameTableEntry> gameTableEntrySerde;
    private static SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde;
    private static SpecificAvroSerde<HeadDirectionRecord> headDirSerde;
    private static SpecificAvroSerde<GameTick> tickSerde;
    private static SpecificAvroSerde<GameStatusRecord> gameStatusSerde;
    private static SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde;

    private static KTable<String, HeadDirectionRecord> headDirectionRecordKTable3;
    private static KStream<String, GameTableEntry> tableEntryLog;


    private GameTick gameTick;
    private HeadDirection headDirection;
    private GameTableEntry snakeHead;
    private GameTableEntry snakeTail;

    @Getter
    private String gameId;

    private GameTableEntry newPositionEntry;

    @Autowired
    public TickProcessor(final StreamsBuilder streamsBuilder,
                         final SpecificAvroSerde<GameTablePosition> gameTablePositionSerde,
                         final SpecificAvroSerde<GameTableEntry> gameTableEntrySerde,
                         final SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde,
                         final SpecificAvroSerde<HeadDirectionRecord> headDirSerde,
                         final SpecificAvroSerde<GameTick> tickSerde,
                         final SpecificAvroSerde<GameStatusRecord> gameStatusSerde,
                         final SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde,
                         final KTable<String, HeadDirectionRecord> headDirectionRecordKTable3,
                         final KStream<String, GameTableEntry> tableEntryLog) {
        TickProcessor.streamsBuilder = streamsBuilder;
        TickProcessor.gameTablePositionSerde = gameTablePositionSerde;
        TickProcessor.gameTableEntrySerde = gameTableEntrySerde;
        TickProcessor.gameTableEntriesSerde = gameTableEntriesSerde;
        TickProcessor.headDirSerde = headDirSerde;
        TickProcessor.tickSerde = tickSerde;
        TickProcessor.gameStatusSerde = gameStatusSerde;
        TickProcessor.processTickCommandSerde = processTickCommandSerde;
        TickProcessor.headDirectionRecordKTable3 = headDirectionRecordKTable3;
        TickProcessor.tableEntryLog = tableEntryLog;
    }

    @Builder
    public TickProcessor(GameTick gameTick, HeadDirection headDirection, GameTableEntry snakeHead, GameTableEntry snakeTail, String gameId, GameTableEntry newPositionEntry) {
        this.gameTick = gameTick;
        this.headDirection = headDirection;
        this.snakeHead = snakeHead;
        this.snakeTail = snakeTail;
        this.gameId = gameId;
        this.newPositionEntry = newPositionEntry;
    }

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
        GameTableEntry addHead = GameTableEntry.newBuilder(snakeHead).setPosition(getNewHeadPosition()).build();
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

    public static void define(){

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
                                list.remove(0);
                            }
                            entries.setEntries(list);
                            return entries;
                        },Materialized.with(Serdes.String(),gameTableEntriesSerde))
                        .filter( (game,entries) -> !entries.getEntries().isEmpty())
                        .mapValues( v -> v.getEntries().get(0));


        KTable<GameTablePosition,GameTableEntry> positionUsage  =
                tableEntryLog
                        .groupBy( (gameId,entry) -> new GameTablePosition(entry.getPosition(),gameId),
                                Grouped.with(gameTablePositionSerde,gameTableEntrySerde))
                        .reduce( (currentValue, next) -> next);


        streamsBuilder.stream(Topics.GAME_TICKS,Consumed.with(Serdes.String(),tickSerde))
                .mapValues( (game,tick) -> TickProcessor.builder().gameId(game).gameTick(tick))
                .join(headDirectionRecordKTable3,(cmdBuilder,direction) -> cmdBuilder.headDirection(direction.getType()))
                .join(snakeHead,(cmdBuilder,head) -> cmdBuilder.snakeHead(head))
                .join(snakeTail,(cmdBuilder,tail) -> cmdBuilder.snakeTail(tail))
                .selectKey((gameId,cmdBuilder) -> new GameTablePosition(cmdBuilder.build().getNewHeadPosition(),gameId))
                .mapValues((tablePosition,cmdBuilder) -> cmdBuilder.build().toSerializableObject())
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
