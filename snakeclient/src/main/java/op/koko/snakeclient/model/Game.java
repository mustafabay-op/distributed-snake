package op.koko.snakeclient.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

public class Game {
    private final int width, height;
    private final char[][] grid;
    private GridUpdater gridUpdater;

    public Game(int width, int height, Consumer<String, String> consumer, ObjectMapper objectMapper) {
        this.width = width;
        this.height = height;
        this.grid = new char[this.width][this.height];
        this.gridUpdater = new GridUpdater(grid, consumer, objectMapper);
    }

    public void start() {
        // todo: send start event to topic and start consuming from output topic
        for (int i = 0; i < this.width; i++) {
            for (int j = 0; j < this.height; j++) {
                this.grid[i][j] = 'O';
            }
        }
        new Thread(gridUpdater).start();
    }

    public void render() {
        for (int i = 0; i < this.width; i++) {
            for (int j = 0; j < this.height; j++) {
                System.out.print(this.grid[i][j]);
            }
            System.out.println();
        }
    }

    static class GridUpdater implements Runnable {

        private final char[][] grid;
        private final Consumer<String, String> consumer;
        private final ObjectMapper objectMapper;
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        public GridUpdater(char[][] grid, Consumer<String, String> consumer, ObjectMapper objectMapper) {
            this.grid = grid;
            this.consumer = consumer;
            this.objectMapper = objectMapper;
        }

        @Override
        public void run() {
            isRunning.compareAndSet(false, true);
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                StreamSupport.stream(records.spliterator(), false).forEach(this::updateGridFromRecord);
            }
        }

        private void updateGridFromRecord(ConsumerRecord<String, String> record) {
            try {
                Dot dot = objectMapper.readValue(record.value(), Dot.class);
                grid[dot.x()][dot.y()] = dot.symbol();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }
}
