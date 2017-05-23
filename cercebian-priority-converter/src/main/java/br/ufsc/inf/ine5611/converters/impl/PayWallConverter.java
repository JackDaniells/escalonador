package br.ufsc.inf.ine5611.converters.impl;

import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.input.TeeInputStream;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PayWallConverter extends AbstractConverter implements Converter {
    private TaskData current = null;
    private int processingDelay = 0;
    private double  processingDelayFactor = 0;
    private int workingSetSize = Integer.MAX_VALUE;
    private Map<ConverterTask, TaskData> activeTasks = new HashMap<>();


    protected PayWallConverter(int processingDelay, double processingDelayFactor,
                               int workingSetSize) {
        this.processingDelay = processingDelay;
        this.processingDelayFactor = processingDelayFactor;
        this.workingSetSize = workingSetSize;
    }
    public PayWallConverter() {
    }

    private synchronized TaskData setup(ConverterTask task) throws IOException {
        Preconditions.checkState(activeTasks.size() < workingSetSize);

        /* Para fins didáticos, a etapa de processamento consiste de sleeps.
         * O verdadeiro processamento é rápido e feito já durante o setup. */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TeeInputStream tee = new TeeInputStream(task.getInputStream(), baos);
        TaskData taskData = new TaskData(task, baos.size());
        processImage(tee, task.getOutputStream(), task.getMediaType());
        activeTasks.put(task, taskData);
        return taskData;
    }

    @Override
    public boolean cancel(ConverterTask task) {
        boolean did = activeTasks.remove(task) != null;
        cancellationListeners.forEach(l -> l.accept(task));
        return did;
    }

    @Override
    public synchronized void processFor(ConverterTask task, long time, TimeUnit timeUnit)
            throws IOException, InterruptedException {
        Preconditions.checkNotNull(task);
        Preconditions.checkState(current == null, "Concurrent call to processFor()!");
        Preconditions.checkArgument(!task.isDone(), "Cannot process a done task");
        Preconditions.checkArgument(!task.isCancelled(), "Cannot process a cancelled task");

        processingListeners.forEach(l -> l.accept(task));
        TaskData taskData = !activeTasks.containsKey(task) ? setup(task) : activeTasks.get(task);
        current = taskData;

        long maxMs = TimeUnit.MILLISECONDS.convert(time, timeUnit);
        long taskMs = Math.min(current.processingLeft, maxMs);
        Stopwatch w = Stopwatch.createStarted();
        try {
            while (current != null && current.processingLeft > 0
                    && w.elapsed(TimeUnit.MILLISECONDS) < maxMs) {
                Stopwatch w2 = Stopwatch.createStarted();
                wait(taskMs);
                taskData.processingLeft -= w2.elapsed(TimeUnit.MILLISECONDS);
            }
            if (taskData.processingLeft <= 0) {
                completionListeners.forEach(l -> l.accept(task));
                activeTasks.remove(task);
                current = null;
            }
        } finally {
            current = null;
        }
    }

    @Override
    public synchronized boolean interrupt() {
        if (current == null) return false;
        interruptListeners.forEach(l -> l.accept(current.task));
        current = null;
        notifyAll();
        return true;
    }

    @Override
    public void close()  { /* pass */ }

    private void processImage(InputStream inputStream, OutputStream outputStream, String mediaType) throws IOException {
        BufferedImage img = ImageIO.read(inputStream);
        Graphics graphics = img.getGraphics();
        graphics.setColor(Color.WHITE);
        graphics.fillRect(0, img.getHeight()-40, img.getWidth(), 30);
        graphics.setColor(Color.BLACK);
        graphics.setFont(new Font("Arial", Font.BOLD, 16));
        graphics.drawString("Only available on Pro version!", 10, img.getHeight()-20);

        Iterator<ImageWriter> it = ImageIO.getImageWritersByMIMEType(mediaType);
        if (!it.hasNext()) throw new IOException("Unsupported media type " + mediaType);
        ImageWriter writer = it.next();
        writer.setOutput(ImageIO.createImageOutputStream(outputStream));
        writer.write(img);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int processingDelay = 0;
        private double  processingDelayFactor = 0;
        private int workingSetSize = Integer.MAX_VALUE;

        public Builder withProcessingDelay(int processingDelay) {
            this.processingDelay = processingDelay;
            return this;
        }

        public Builder withProcessingDelayFactor(double processingDelayFactor) {
            this.processingDelayFactor = processingDelayFactor;
            return this;
        }

        public Builder withWorkingSetSize(int workingSetSize) {
            this.workingSetSize = workingSetSize;
            return this;
        }

        public PayWallConverter build() {
            return new PayWallConverter(processingDelay, processingDelayFactor, workingSetSize);
        }
    }

    private class TaskData  {
        ConverterTask task;
        int size;
        long processingLeft;

        TaskData(ConverterTask task, int size) {
            this.task = task;
            processingLeft = processingDelay;
            this.size = size;
            if (size > 0)
                processingLeft += (long)Math.ceil(processingDelayFactor * size);
        }

        @Override
        public String toString() {
            return String.format("PayWallConverter.TaskData(size=%d, left=%d, task=%s)",
                    size, processingLeft, task.toString());
        }
    }
}
