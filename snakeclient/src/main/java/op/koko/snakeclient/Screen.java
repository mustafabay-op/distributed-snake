package op.koko.snakeclient;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.GridPane;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;
import op.koko.snakeclient.model.Dot;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.LinkedList;
import java.util.Locale;
import java.util.Properties;
import java.util.Queue;

public class Screen extends Application {

    public static final int HEIGHT = 26;
    public static final int WIDTH = 26;
    public static final String GAME_INPUT = "game-input";
    public static final String GAME_OUTPUT = "game-output";

    public final Queue<Dot> queue = new LinkedList<>();
    public final Rectangle[][] rectangles = new Rectangle[HEIGHT][WIDTH];

    @Override
    public void start(Stage stage) {
        GridPane pane = setupGrid();
        final Scene scene = new Scene(pane, 675, 675);

        final Controller controller = setupController();
        setupKeyEvents(scene, controller);

        RectangleUpdater rectangleUpdater = new RectangleUpdater(queue, rectangles);
        rectangleUpdater.start();
        setupConsumer();


        stage.setScene(scene);
        stage.show();
    }

    private void setupConsumer() {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-snake");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();


        KStream<String, String> inputs = builder.stream(GAME_INPUT, Consumed.with(Serdes.String(), Serdes.String()));

        inputs
                .mapValues(value -> {
                    String[] arr = value.split("-");
                    return new Dot(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), op.koko.snakeclient.model.Color.valueOf(arr[2].toUpperCase(Locale.ROOT)));
                })
                .foreach((k, v) -> queue.add(v));
        final Topology topology = builder.build();
        new KafkaStreams(topology, streamProps).start();
    }

    private void setupKeyEvents(Scene scene, Controller controller) {
        scene.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case UP -> controller.up();
                case DOWN -> controller.down();
                case RIGHT -> controller.right();
                case LEFT -> controller.left();
                case SPACE -> controller.space();
            }
        });
    }

    private Controller setupController() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        final Producer<String, String> producer = new KafkaProducer<>(props);
        final GameOutputProducer gameOutputProducer = new GameOutputProducer(producer, GAME_INPUT);
        final Controller controller = new Controller(gameOutputProducer);
        return controller;
    }

    private GridPane setupGrid() {
        GridPane pane = new GridPane();
        for (int x = 0; x < rectangles.length; x++) {
            for (int y = 0; y < rectangles[x].length; y++) {
                Rectangle rectangle = new Rectangle(HEIGHT, WIDTH);
                rectangles[x][y] = rectangle;
                pane.add(rectangle, x, y);
            }
        }
        return pane;
    }

    public static void main(String[] args) {
        launch();
    }
}