package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class VisibleConverter implements Converter {
    private Converter delegate;
    private HashSet<ConverterTask> active = new HashSet<>();
    private Map<ConverterEvent, ArrayList<ConverterTask>> eventLists = new HashMap<>();
    private ArrayList<ConverterTaskEvent> events = new ArrayList<>();

    public VisibleConverter(Converter delegate) {
        this.delegate = delegate;
        for (ConverterEvent e : ConverterEvent.values()) eventLists.put(e, new ArrayList<>());
        delegate.addCancellationListener(t -> addEvent(ConverterEvent.CANCEL, t));
        delegate.addCompletionListener(t -> addEvent(ConverterEvent.COMPLETION, t));
        delegate.addInterruptListener(t -> addEvent(ConverterEvent.INTERRUPT, t));
        delegate.addProcessingListener(t -> addEvent(ConverterEvent.PROCESS, t));
    }

    private synchronized void addEvent(ConverterEvent event, ConverterTask task) {
        eventLists.get(event).add(task);
        events.add(new ConverterTaskEvent(event, task));
        if (event == ConverterEvent.PROCESS) active.add(task);
        if (event == ConverterEvent.COMPLETION) active.remove(task);
        System.err.printf("VisibleConverter@%h: %s %s\n", System.identityHashCode(this),
                event.toString(), task);
    }

    public HashSet<ConverterTask> getActive() {
        return active;
    }

    public ArrayList<ConverterTask> getCancellations() {
        return eventLists.get(ConverterEvent.CANCEL);
    }

    public ArrayList<ConverterTask> getCompletions() {
        return eventLists.get(ConverterEvent.COMPLETION);
    }

    public ArrayList<ConverterTask> getProcessed() {
        return eventLists.get(ConverterEvent.PROCESS);
    }

    public ArrayList<ConverterTask> getInterruptions() {
        return eventLists.get(ConverterEvent.INTERRUPT);
    }

    public ArrayList<ConverterTaskEvent> getEvents() {
        return events;
    }


    @Override
    public void processFor(ConverterTask task, long time, TimeUnit timeUnit)
            throws IOException, InterruptedException {
        delegate.processFor(task, time, timeUnit);
    }

    @Override
    public boolean interrupt() {
        return delegate.interrupt();
    }

    @Override
    public boolean cancel(ConverterTask task) {
        return delegate.cancel(task);
    }

    @Override
    public void process(ConverterTask task) throws IOException, InterruptedException {
        delegate.process(task);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public void addCompletionListener(Consumer<ConverterTask> listener) {
        delegate.addCompletionListener(listener);
    }

    @Override
    public void addInterruptListener(Consumer<ConverterTask> listener) {
        delegate.addInterruptListener(listener);
    }

    @Override
    public void addCancellationListener(Consumer<ConverterTask> listener) {
        delegate.addCancellationListener(listener);
    }

    @Override
    public void addProcessingListener(Consumer<ConverterTask> listener) {
        delegate.addProcessingListener(listener);
    }

    @Override
    public void removeCompletionListener(Consumer<ConverterTask> listener) {
        delegate.removeCompletionListener(listener);
    }

    @Override
    public void removeInterruptListener(Consumer<ConverterTask> listener) {
        delegate.removeInterruptListener(listener);
    }

    @Override
    public void removeCancellationListener(Consumer<ConverterTask> listener) {
        delegate.removeCancellationListener(listener);
    }

    @Override
    public void removeProcessingListener(Consumer<ConverterTask> listener) {
        delegate.removeProcessingListener(listener);
    }
}
