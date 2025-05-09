package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.bb;

import java.util.Arrays;
import java.util.List;

public class RelativeBoundingBox {
    public final float top;
    public final float right;
    public final float bottom;
    public final float left;

    public static RelativeBoundingBox fromString(String value) {
        List<Float> values = Arrays.stream(value.split(",")).map(String::trim).map(Float::parseFloat).toList();
        return switch (values.size()) {
            case 1 -> new RelativeBoundingBox(values.getFirst());
            case 2 -> new RelativeBoundingBox(values.get(0), values.get(1));
            case 3 -> new RelativeBoundingBox(values.get(0), values.get(1), values.get(2));
            case 4 -> new RelativeBoundingBox(values.get(0), values.get(1), values.get(2), values.get(3));
            default -> throw new IllegalArgumentException("Invalid value " + value);
        };
    }

    public RelativeBoundingBox(float value) {
        this.top = value;
        this.right = 100 - value;
        this.bottom = 100 - value;
        this.left = value;
        checkBounds();
    }

    public RelativeBoundingBox(float topBottom, float rightLeft) {
        this.top = topBottom;
        this.right = 100 - rightLeft;
        this.bottom = 100 - topBottom;
        this.left = rightLeft;
        checkBounds();
    }

    public RelativeBoundingBox(float top, float rightLeft, float bottom) {
        this.top = top;
        this.right = 100 - rightLeft;
        this.bottom = 100 - bottom;
        this.left = rightLeft;
        checkBounds();
    }

    public RelativeBoundingBox(float top, float right, float bottom, float left) {
        this.top = top;
        this.right = 100 - right;
        this.bottom = 100 - bottom;
        this.left = left;
        checkBounds();
    }

    private void checkBounds() {
        if (!(0 < top || top < 100 || 0 < right || right < 100 || 0 < bottom || bottom < 100 || 0 < left || left < 100)) {
            throw new IllegalArgumentException("Bounds must be in range [0, 100]");
        }
        if (top > bottom) {
            throw new IllegalArgumentException("top bound must be smaller than bottom bound");
        }
        if (left > right) {
            throw new IllegalArgumentException("left bound must be smaller than right bound");
        }
    }

    public class DocumentChecker {
        private final float width;
        private final float height;

        public DocumentChecker(int width, int height) {
            this.width = (float) width;
            this.height = (float) height;
        }

        public boolean contains(Rect rect) {
            float fTop = rect.top() / height * 100;
            float fRight = rect.right() / width * 100;
            float fBottom = rect.bottom() / height * 100;
            float fLeft = rect.left() / width * 100;
            return (
                    fTop >= top
            ) && (
                    fRight <= right
            ) && (
                    fBottom <= bottom
            ) && (
                    fLeft >= left
            );
        }
    }

    public DocumentChecker checker(int width, int height) {
        return new DocumentChecker(width, height);
    }
}
