package br.ufsc.inf.ine5611.converters.paywall;

import br.ufsc.inf.ine5611.converters.BaseConverterTest;
import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.impl.PayWallConverter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import com.google.common.base.Stopwatch;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static br.ufsc.inf.ine5611.converters.Utils.*;

public class PayWallInterruptTest extends BaseConverterTest {
    @Override
    protected Converter createConverter() {
        return PayWallConverter.newBuilder().withProcessingDelay(900).build();
    }

    @Test(timeOut = 4*(900+300))
    public void testInterrupt() throws Exception {
        for (int i = 0; i < 4; i++) {
            try (Converter converter = createConverter()) {
                ConverterTask task = createTask(converter);
                Future<Object> future = Executors.newSingleThreadExecutor().submit(() -> {
                    converter.processFor(task, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    return new Object();
                });
                Thread.sleep(300);
                if (converter.interrupt()) {
                    Assert.assertNotNull(future.get(300, TimeUnit.MILLISECONDS));
                } else {
                    future.get(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
                }
            }
        }
    }
}
