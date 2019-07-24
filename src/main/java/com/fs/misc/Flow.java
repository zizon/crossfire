package com.fs.misc;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Flow<T> implements Stream<T>, Iterable<T>, AutoCloseable {

    protected final Stream<T> stream;

    protected Flow(Stream<T> stream) {
        this.stream = stream;
    }

    @Override
    public Flow<T> filter(Predicate<? super T> predicate) {
        return new Flow<>(this.stream.filter(predicate));
    }

    @Override
    public <R> Flow<R> map(Function<? super T, ? extends R> mapper) {
        return new Flow<>(this.stream.map(mapper));
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        //TODO
        return null;
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        //TODO
        return null;
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        //TODO
        return null;
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new Flow<>(this.stream.flatMap(mapper));
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return null;
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return null;
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return null;
    }

    @Override
    public Flow<T> distinct() {
        return new Flow<>(this.stream.distinct());
    }

    @Override
    public Flow<T> sorted() {
        return new Flow<>(this.stream.sorted());
    }

    @Override
    public Flow<T> sorted(Comparator<? super T> comparator) {
        return new Flow<>(this.stream.sorted(comparator));
    }

    @Override
    public Flow<T> peek(Consumer<? super T> action) {
        return new Flow<>(this.stream.peek(action));
    }

    @Override
    public Flow<T> limit(long maxSize) {
        return new Flow<>(this.stream.limit(maxSize));
    }

    @Override
    public Flow<T> skip(long n) {
        return new Flow<>(this.stream.skip(n));
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        this.stream.forEach(action);
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        this.stream.forEachOrdered(action);
    }

    @Override
    public Object[] toArray() {
        return this.stream.toArray();
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return this.stream.toArray(generator);
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        return this.stream.reduce(identity, accumulator);
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return this.stream.reduce(accumulator);
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return this.stream.reduce(identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return this.stream.collect(supplier, accumulator, combiner);
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return this.stream.collect(collector);
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        return this.stream.min(comparator);
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        return this.stream.max(comparator);
    }

    @Override
    public long count() {
        return this.stream.count();
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        return this.stream.anyMatch(predicate);
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        return this.stream.anyMatch(predicate);
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        return this.stream.noneMatch(predicate);
    }

    @Override
    public Optional<T> findFirst() {
        return this.stream.findFirst();
    }

    @Override
    public Optional<T> findAny() {
        return this.stream.findAny();
    }

    @Override
    public Iterator<T> iterator() {
        return this.stream.iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return this.stream.spliterator();
    }

    @Override
    public boolean isParallel() {
        return this.stream.isParallel();
    }

    @Override
    public Flow<T> sequential() {
        return new Flow<>(this.stream.sequential());
    }

    @Override
    public Flow<T> parallel() {
        return new Flow<>(this.stream.parallel());
    }

    @Override
    public Flow<T> unordered() {
        return new Flow<>(this.stream.unordered());
    }

    @Override
    public Flow<T> onClose(Runnable closeHandler) {
        return new Flow<>(this.onClose(closeHandler));
    }

    @Override
    public void close() {
        this.stream.close();
    }
}
