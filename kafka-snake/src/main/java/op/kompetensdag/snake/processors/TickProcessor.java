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
import java.util.function.Consumer;

@Builder
public class TickProcessor {

    public static final String SNAKE_HEAD_REDUCER_NAME = "snake-head";
    private GameTick gameTick;
    private HeadDirection headDirection;
    private GameTableEntry snakeHead;
    private GameTableEntry snakeTail;

    @Getter
    private String gameId;

    private GameTableEntry newPositionEntry;

    public static TickProcessor.TickProcessorBuilder fromProcessTickCommand(ProcessTickCommand cmd, GameTableEntry newPositionEntry) {
        return builder().gameId(cmd.getGameId())
                .gameTick(cmd.getGameTick())
                .headDirection(cmd.getHeadDirection())
                .snakeHead(cmd.getSnakeHead())
                .snakeTail(cmd.getSnakeTail())
                .newPositionEntry(newPositionEntry);
    }

    public static void define(final StreamsBuilder builder, Map<String, String> schemaRegistryProps, KTable<String, HeadDirectionRecord> headDirectionRecordKTable3,
                              KStream<String, GameTableEntry> tableEntryLog) {

        SpecificAvroSerde<GameTablePosition> gameTablePositionSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameTableEntry> gameTableEntrySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameSnakeEntries> gameTableEntriesSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<TablePosition> tablePositionSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<HeadDirectionRecord> headDirSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameTick> tickSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<GameStatusRecord> gameStatusSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<ProcessTickCommand> processTickCommandSerde = new SpecificAvroSerde<>();

        gameTablePositionSerde.configure(schemaRegistryProps, true);
        gameTableEntrySerde.configure(schemaRegistryProps, false);
        gameTableEntriesSerde.configure(schemaRegistryProps, false);
        tablePositionSerde.configure(schemaRegistryProps, true);
        headDirSerde.configure(schemaRegistryProps, false);
        tickSerde.configure(schemaRegistryProps, false);
        gameStatusSerde.configure(schemaRegistryProps, false);
        processTickCommandSerde.configure(schemaRegistryProps, false);

        KTable<String, GameTableEntry> snakeHead = tableEntryLog
                .filter((game, tableEntry) -> isSnake(tableEntry) && tableEntry.getBusy())
                .groupByKey()
                .reduce((current, next) -> next, Named.as(SNAKE_HEAD_REDUCER_NAME), Materialized.with(Serdes.String(), gameTableEntrySerde));

        /*snakeHead.toStream().mapValues(v -> "Snake head: " + v).to(GAME_OUTPUT);*/
        /*        snakeTail.toStream().mapValues(v -> "Snake tail: " + v).to(GAME_OUTPUT);*/

        KTable<String, GameTableEntry> snakeTail = tableEntryLog
                .filter((game, tableEntry) -> isSnake(tableEntry))
                .groupByKey()
                .aggregate(
                        TickProcessor::getInitialGameSnakeEntries,
                        (game, newEntry, entries) -> getGameSnakeEntries(newEntry, entries),
                        Materialized.with(Serdes.String(), gameTableEntriesSerde))
                .filter((game, entries) -> !entries.getEntries().isEmpty())
                .mapValues(v -> v.getEntries().get(0));

        KTable<GameTablePosition, GameTableEntry> positionUsage = tableEntryLog
                .groupBy((gameId, entry) -> new GameTablePosition(entry.getPosition(), gameId), Grouped.with(gameTablePositionSerde, gameTableEntrySerde))
                .reduce((currentValue, next) -> next);

        // join current game status? - will game status change ensure that ticking is stopped?
        // join current snake direction
        // join current snake head
        // join current snake tail
        // calculate new snake head using current heed and current direction
        // join all positions on the game table
        // check if new snake head position is clashing with current state of the position in table
        // emit a position busy event for head and position empty event for tail

        builder.stream(Topics.GAME_TICKS, Consumed.with(Serdes.String(), tickSerde))
                .mapValues((game, tick) -> TickProcessor.builder().gameId(game).gameTick(tick))
                .join(headDirectionRecordKTable3, (cmdBuilder, direction) -> cmdBuilder.headDirection(direction.getType()))
                .join(snakeHead, TickProcessorBuilder::snakeHead)
                .join(snakeTail, TickProcessorBuilder::snakeTail)
                .selectKey((gameId, cmdBuilder) -> new GameTablePosition(cmdBuilder.build().getNewHeadPosition(), gameId))
                // change value to persistent avro entity
                .mapValues((tablePosition, cmdBuilder) -> cmdBuilder.build().toSerializableObject())
//                .groupByKey(Grouped.keySerde(gameTablePositionSerde))
                .leftJoin(positionUsage, (readOnlyKey, processTickCommand, gameTableEntry) -> fromProcessTickCommand(processTickCommand, gameTableEntry),
                        Joined.with(gameTablePositionSerde, processTickCommandSerde, gameTableEntrySerde))
                .selectKey((k, v) -> v.build().getGameId())
                .split()
                .branch((game, cmdBuilder) -> cmdBuilder.build().isNewHeadPositionTaken(), Branched.withConsumer(getGameOverConsumer(gameStatusSerde)))
                .defaultBranch(Branched.withConsumer(getMoveSnakeConsumer(gameTableEntrySerde)));
    }

    private static Consumer<KStream<String, TickProcessorBuilder>> getMoveSnakeConsumer(SpecificAvroSerde<GameTableEntry> gameTableEntrySerde) {
        return builderKStream -> builderKStream
                .flatMapValues(cmdBuilder -> cmdBuilder.build().moveSnake())
                .to(Topics.GAME_TABLE_ENTRIES, Produced.with(Serdes.String(), gameTableEntrySerde));
    }

    private static Consumer<KStream<String, TickProcessorBuilder>> getGameOverConsumer(SpecificAvroSerde<GameStatusRecord> gameStatusSerde) {
        return (builderKStream) -> builderKStream
                .mapValues((v) -> new GameStatusRecord(GameStatus.ENDED))
                .to(Topics.GAME_STATUS, Produced.with(Serdes.String(), gameStatusSerde));
    }

    private static GameSnakeEntries getInitialGameSnakeEntries() {
        return GameSnakeEntries.newBuilder().setEntries(new ArrayList<>()).build();
    }

    private static GameSnakeEntries getGameSnakeEntries(GameTableEntry newEntry, GameSnakeEntries entries) {
        List<GameTableEntry> list = entries.getEntries();
        if (newEntry.getBusy()) {
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
    }

    private static boolean isSnake(GameTableEntry tableEntry) {
        return tableEntry.getType() == GameTableEntryType.SNAKE;
    }

    public TablePosition getNewHeadPosition() {
        return switch (headDirection) {
            case NORTH -> TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY() + 1).build();
            case SOUTH -> TablePosition.newBuilder(snakeHead.getPosition()).setY(snakeHead.getPosition().getY() - 1).build();
            case EAST -> TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX() + 1).build();
            case WEST -> TablePosition.newBuilder(snakeHead.getPosition()).setX(snakeHead.getPosition().getX() - 1).build();
            default -> snakeHead.getPosition();
        };
    }

    public boolean isNewHeadPositionTaken() {
        return Optional.ofNullable(newPositionEntry).map(GameTableEntry::getBusy).orElse(false);
    }

    public Iterable<GameTableEntry> moveSnake() {
        GameTableEntry addHead = GameTableEntry.newBuilder(snakeHead).setPosition(getNewHeadPosition()).build();
        GameTableEntry removeTail = GameTableEntry.newBuilder(snakeTail).setBusy(false).build();
        return List.of(addHead, removeTail);
    }

    public ProcessTickCommand toSerializableObject() {
        return new ProcessTickCommand(gameId, gameTick, headDirection, snakeHead, snakeTail);
    }
}
