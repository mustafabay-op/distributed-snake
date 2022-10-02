package op.kompetensdag.kafkasnake;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

public class Topic {


    @Bean
    NewTopic snakePosition() {
        return TopicBuilder
                .name("snakePositionTopicName")
                .replicas(2)
                .partitions(3)
                .build();
    }
}
