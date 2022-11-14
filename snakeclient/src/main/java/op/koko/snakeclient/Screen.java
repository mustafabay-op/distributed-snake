package op.koko.snakeclient;

import javafx.application.Application;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Hyperlink;
import javafx.scene.control.Label;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
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

import java.util.*;

public class Screen extends Application {

    public static final int HEIGHT = 23;
    public static final int WIDTH = 17;
    public static final String GAME_INPUT = "game_input";
    public static final String GAME_OUTPUT = "game_output";
    private static final Queue<Dot> queue = new LinkedList<>();
    public static final String MAIN_MENU_STYLESHEET = "/mainmenu.css";
    public static final int WIDTH_SCENE = 575;
    public static final int HEIGHT_SCENE = 425;
    private final Rectangle[][] rectangles = new Rectangle[HEIGHT][WIDTH];

    private GridPane pane;
    private Scene playScene;
    private Scene mainMenuScene;
    private Controller controller;
    private RectangleUpdater rectangleUpdater;
    private Stage stage;

    public static void main(String[] args) {
        setupConsumer();
        launch();
    }

    @Override
    public void start(Stage stage) {
        this.stage = stage;
        this.controller = setupController();
        this.mainMenuScene = createMainMenuScene();
        showMainMenuScene();
    }

    private void showMainMenuScene() {
        stage.setScene(mainMenuScene);
        stage.show();
    }

    private Scene createMainMenuScene() {
        Hyperlink startGameLink = new Hyperlink("START GAME");
        Label headingLabel = new Label("DISTRIBUTED SNAKE");
        headingLabel.setAlignment(Pos.TOP_CENTER);
        startGameLink.setAlignment(Pos.CENTER);
        startGameLink.setOnAction(e -> {
            controller.sendAdministrationKeyEvent(KeyCode.SPACE);
            showFreshPlayScene();
        });
        HBox startGameHBox = new HBox(1, startGameLink);
        HBox headingHBox = new HBox(1, headingLabel);
        startGameHBox.setAlignment(Pos.CENTER);
        headingHBox.setAlignment(Pos.TOP_CENTER);
        VBox vbox = new VBox(50, headingHBox, startGameHBox);
        vbox.setAlignment(Pos.CENTER);
        vbox.setBackground(Background.fill(Color.BLACK));

        Scene scene = new Scene(vbox, WIDTH_SCENE, HEIGHT_SCENE);
        setSceneStylesheet(scene, MAIN_MENU_STYLESHEET);
        return scene;
    }

    private void setSceneStylesheet(Scene scene, String stylesheet) {
        scene.getStylesheets().add(getClass().getResource(stylesheet).toExternalForm());
    }

    public void showFreshPlayScene() {
        pane = setupGrid();
        playScene = new Scene(pane, WIDTH_SCENE, HEIGHT_SCENE);
        rectangleUpdater = new RectangleUpdater(queue, rectangles);

        setupKeyEvents(playScene, controller);

        stage.setScene(playScene);
        stage.show();
        //controller.setGameId(UUID.randomUUID().toString());
        rectangleUpdater.start();
    }

    private static void setupConsumer() {
        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-snake-client");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();


        builder.stream(GAME_OUTPUT, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> key.equalsIgnoreCase(Controller.gameId))
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
                case UP, DOWN, LEFT, RIGHT -> controller.sendMovementKeyPressedEvent(event.getCode());
                case SPACE -> controller.sendAdministrationKeyEvent(event.getCode());
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
        final GameEventProducer gameEventProducer = new GameEventProducer(producer, GAME_INPUT);
        final Controller controller = new Controller(gameEventProducer);
        return controller;
    }

    private GridPane setupGrid() {
        GridPane pane = new GridPane();
        for (int x = 0; x < rectangles.length; x++) {
            for (int y = 0; y < rectangles[x].length; y++) {
                Rectangle rectangle = new Rectangle(25, 25);
                rectangles[x][y] = rectangle;
                pane.add(rectangle, x, y);
            }
        }
        return pane;
    }
}
