package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.impl.SimpleConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static br.ufsc.inf.ine5611.converters.Utils.*;

@Test
public abstract class BaseConverterTest {
    protected abstract Converter createConverter();

    protected ConverterTask createTask(Converter converter) {
        return new SimpleConverterTask(alien(), sink(), IMAGE_JPEG, converter::cancel);
    }

    @Test(timeOut = 10000)
    public void testProcessUntilCompletion() throws Exception {
        try (Converter converter = createConverter();
             ConverterTask task = createTask(converter)) {
            converter.processFor(task, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testInterruptEmpty() throws Exception {
        try (Converter converter = createConverter()) {
            Assert.assertFalse(converter.interrupt());
        }
    }
}
