package ru.bmstu.core;

import java.nio.file.Path;
import java.util.List;

@FunctionalInterface
public interface Splitter<I> {

    List<Path> split(I input, int splitRatio);

    static Range range(long left, long right) {
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

        public void plusLeft(long left) {
            this.left += left;
        }

        public void plusRight(long right) {
            this.right += right;
        }

        public long size() {
            return right - left;
        }

        @Override
        public String toString() {
            return "Range{" + left + "," + right + '}';
        }
    }
}
