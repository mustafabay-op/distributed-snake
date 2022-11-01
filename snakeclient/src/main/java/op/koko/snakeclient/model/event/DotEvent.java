package op.koko.snakeclient.model.event;

import op.koko.snakeclient.model.Dot;

public record DotEvent(Dot dot, boolean isActive) {
}
