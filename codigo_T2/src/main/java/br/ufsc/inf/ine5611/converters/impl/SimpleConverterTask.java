package br.ufsc.inf.ine5611.converters.impl;

import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class SimpleConverterTask extends CompletableFuture<Void> implements ConverterTask {
    private InputStream inputStream;
    private OutputStream outputStream;
    private String mediaType;
    private Consumer<ConverterTask> cancelCallback;

    public SimpleConverterTask(InputStream inputStream, OutputStream outputStream,
                               String mediaType,
                               Consumer<ConverterTask> cancelCallback) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.mediaType = mediaType;
        this.cancelCallback = cancelCallback;
    }

    public InputStream getInputStream() {
        return inputStream;
    }
    public OutputStream getOutputStream() {
        return outputStream;
    }
    public String getMediaType() {
        return mediaType;
    }

    @Override
    public void close() throws IOException {
        if (!isDone() && isCancelled())
            cancel(false);
        inputStream.close();
        outputStream.close();
    }

    @Override
    public boolean cancel(boolean b) {
        if (isCancelled()) return true;
        boolean did = super.cancel(b);
        if (did && cancelCallback != null) cancelCallback.accept(this);
        return did;
    }
}
