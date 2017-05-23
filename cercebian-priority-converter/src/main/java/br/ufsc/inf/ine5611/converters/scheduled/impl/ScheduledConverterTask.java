package br.ufsc.inf.ine5611.converters.scheduled.impl;

import br.ufsc.inf.ine5611.converters.impl.SimpleConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

public class ScheduledConverterTask extends SimpleConverterTask {
    private long inputBytes;
    private Priority priority;
    private long cycles;
    private long epoch;

    public ScheduledConverterTask(InputStream inputStream, OutputStream outputStream,
                                  String mediaType, Consumer<ConverterTask> cancelCallback,
                                  long inputBytes, Priority priority, long epoch) {
        super(inputStream, outputStream, mediaType, cancelCallback);
        this.inputBytes = inputBytes;
        this.priority = priority;
        this.epoch = epoch;
    }

    public long getInputBytes() {
        return inputBytes;
    }
    public Priority getPriority() {
        return priority;
    }
    public long getEpoch() {
        return epoch;
    }
    public long getCycles() {
        return cycles;
    }
    public long incCycles() {
        return  ++cycles;
    }

    @Override
    public String toString() {
        return String.format("ScheduledConverterTask(prio=%s, epoch=%d, cycles=%d, bytes=%d)",
                priority, epoch, cycles, inputBytes);
    }
}
