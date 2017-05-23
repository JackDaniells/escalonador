package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;

public class ConverterTaskEvent {
    public final ConverterEvent event;
    public final ConverterTask task;

    public ConverterTaskEvent(ConverterEvent event, ConverterTask task) {
        this.event = event;
        this.task = task;
    }

    public ConverterEvent getEvent() {
        return event;
    }

    public ConverterTask getTask() {
        return task;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConverterTaskEvent that = (ConverterTaskEvent) o;

        if (event != that.event) return false;
        return task != null ? task.equals(that.task) : that.task == null;
    }

    @Override
    public int hashCode() {
        int result = event != null ? event.hashCode() : 0;
        result = 31 * result + (task != null ? task.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConverterTaskEvent{" +
                "event=" + event +
                ", task=" + task +
                '}';
    }
}
