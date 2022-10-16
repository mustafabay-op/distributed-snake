package op.kompetensdag.kafkasnake;


public enum GameAdministrationKeyPressed {
    SPACE;


    public static GameAdministrationKeyPressed parse(String key, String value) {
        return GameAdministrationKeyPressed.valueOf(value);
    }
}
