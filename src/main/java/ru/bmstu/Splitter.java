package ru.bmstu;

import java.util.List;

@FunctionalInterface
public interface Splitter<I> {
    List<Range> split(I input);

    static Range InputTrange(long left, long right) {
        return new Range(left, right);
    }

    class Range {
        private long left;
        private long right;

        private Range(long left, long right) {
            this.left = left;
            this.right = right;
        }

        public long getLeft() {
            return left;
        }

        public long getRight() {
            return right;
        }
    }
}
