package br.ufsc.inf.ine5611.converters.impl;

import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;

import java.util.LinkedHashSet;
import java.util.function.Consumer;

public abstract class AbstractConverter  implements Converter {
    protected LinkedHashSet<Consumer<ConverterTask>> completionListeners = new LinkedHashSet<>();
    protected LinkedHashSet<Consumer<ConverterTask>> processingListeners = new LinkedHashSet<>();
    protected LinkedHashSet<Consumer<ConverterTask>> interruptListeners = new LinkedHashSet<>();
    protected LinkedHashSet<Consumer<ConverterTask>> cancellationListeners = new LinkedHashSet<>();
    @Override
    public void addCompletionListener(Consumer<ConverterTask> listener) {
        completionListeners.add(listener);
    }

    @Override
    public void addProcessingListener(Consumer<ConverterTask> listener) {
        processingListeners.add(listener);
    }
    @Override
    public void addInterruptListener(Consumer<ConverterTask> listener) {
        interruptListeners.add(listener);
    }
    @Override
    public void addCancellationListener(Consumer<ConverterTask> listener) {
        cancellationListeners.add(listener);
    }
    @Override
    public void removeCompletionListener(Consumer<ConverterTask> listener) {
        completionListeners.remove(listener);
    }

    @Override
    public void removeProcessingListener(Consumer<ConverterTask> listener) {
        processingListeners.remove(listener);
    }
    @Override
    public void removeInterruptListener(Consumer<ConverterTask> listener) {
        interruptListeners.remove(listener);
    }
    @Override
    public void removeCancellationListener(Consumer<ConverterTask> listener) {
        cancellationListeners.remove(listener);
    }
}
