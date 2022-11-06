package op.kompetensdag.snake;

public class GameAdministrationCommandParser {

        static GameAdministrationCommand parse(String command) {
            switch (command) {
                case "SPACE":
                    return new GameAdministrationCommand("SPACE");
                default:
                    return null;
            }
        }
}
