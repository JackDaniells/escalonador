package br.ufsc.inf.ine5611.converters.scheduled;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public interface ConverterTask extends Future<Void>, AutoCloseable {
    InputStream getInputStream();
    OutputStream getOutputStream();
    String getMediaType();
}
