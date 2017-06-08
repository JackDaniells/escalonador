package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import br.ufsc.inf.ine5611.converters.scheduled.ScheduledConverter;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class Utils {
    public static final String IMAGE_JPEG = "image/jpeg";

    public static InputStream alien() {
        return Utils.class.getClassLoader().getResourceAsStream("alien.jpg");
    }
    public static InputStream spock() {
        return Utils.class.getClassLoader().getResourceAsStream("spock.jpg");
    }
    public static int bytes(String resource) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
             InputStream in = Utils.class.getClassLoader().getResourceAsStream(resource)) {
            IOUtils.copy(in, out);
            return out.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static ConverterTask alien(ScheduledConverter converter, Priority priority) {
        return converter.convert(alien(), sink(), IMAGE_JPEG,
                bytes("alien.jpg"), priority);
    }
    public static ConverterTask spock(ScheduledConverter converter, Priority priority) {
        return converter.convert(spock(), sink(), IMAGE_JPEG,
                bytes("spock.jpg"), priority);
    }

    public static ByteArrayOutputStream sink() {
        return new ByteArrayOutputStream();
    }
}
