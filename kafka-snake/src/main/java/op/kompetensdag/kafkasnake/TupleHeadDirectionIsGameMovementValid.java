package op.kompetensdag.kafkasnake;

public class TupleHeadDirectionIsGameMovementValid {
    public final HeadDirection headDirection;
    public final boolean isGameMovementValid;


    public TupleHeadDirectionIsGameMovementValid(HeadDirection headDirection, boolean isGameMovementValid) {
        this.headDirection = headDirection;
        this.isGameMovementValid = isGameMovementValid;
    }
}
