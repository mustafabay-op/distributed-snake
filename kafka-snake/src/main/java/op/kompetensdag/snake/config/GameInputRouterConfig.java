package op.kompetensdag.snake.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static op.kompetensdag.snake.Topics.GAME_INPUT_TOPIC;

@Configuration
public class GameInputRouterConfig {

    @Bean
    public BranchedKStream<String, String> gameInputBranched(StreamsBuilder streamsBuilder) {
        return streamsBuilder
                .stream(GAME_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .split();
    }
}
