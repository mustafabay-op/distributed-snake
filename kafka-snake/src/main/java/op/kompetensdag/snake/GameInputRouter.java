package op.kompetensdag.snake;

import op.kompetensdag.snake.events.GameAdministrationKeyPressed;
import op.kompetensdag.snake.events.GameMovementKeyPressed;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import static op.kompetensdag.snake.Topics.*;

public class GameInputRouter {

    public static void define(final StreamsBuilder builder){

        BranchedKStream<String, String> gameInputBranched =
                builder.stream(GAME_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())).split();

        gameInputBranched.branch(
                (k, v) -> GameMovementKeyPressed.isValidValue(v),
                Branched.withConsumer(stream -> stream.mapValues((k, v) -> GameMovementKeyPressed.valueOf(v))
                        .to(GAME_MOVEMENT_COMMANDS_TOPIC, Produced.with(Serdes.String(), CustomSerdes.gameMovementKeyPressedSerde))));

        gameInputBranched.branch(
                (k, v) -> GameAdministrationKeyPressed.isValidValue(v),
                Branched.withConsumer(stream -> stream.mapValues((k,v) -> GameAdministrationKeyPressed.valueOf(v))
                        .to(GAME_ADMINISTRATION_COMMANDS_TOPIC, Produced.with(Serdes.String(), CustomSerdes.gameAdministrationKeyPressedSerde))));

        gameInputBranched.defaultBranch(
                Branched.withConsumer(stream -> stream.mapValues(
                        v -> ""
                ).to(CLIENT_RESPONSES_TOPIC, Produced.with(Serdes.String(), Serdes.String()))));


    }


}
