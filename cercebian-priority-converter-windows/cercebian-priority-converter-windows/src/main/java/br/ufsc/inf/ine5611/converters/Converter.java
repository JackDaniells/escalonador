package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface Converter extends AutoCloseable {
    void processFor(ConverterTask task, long time, TimeUnit timeUnit)
            throws IOException, InterruptedException;
    boolean cancel(ConverterTask task);
    boolean interrupt();

    void addCompletionListener(Consumer<ConverterTask> listener);
    void addInterruptListener(Consumer<ConverterTask> listener);
    void addCancellationListener(Consumer<ConverterTask> listener);
    void addProcessingListener(Consumer<ConverterTask> listener);

    void removeCompletionListener(Consumer<ConverterTask> listener);
    void removeInterruptListener(Consumer<ConverterTask> listener);
    void removeCancellationListener(Consumer<ConverterTask> listener);
    void removeProcessingListener(Consumer<ConverterTask> listener);

    default void process(ConverterTask task) throws IOException, InterruptedException {
        processFor(task, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
