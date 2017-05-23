package br.ufsc.inf.ine5611.converters.paywall;

import br.ufsc.inf.ine5611.converters.BaseConverterTest;
import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.Utils;
import br.ufsc.inf.ine5611.converters.impl.PayWallConverter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static br.ufsc.inf.ine5611.converters.Utils.*;

public class PayWallWorkingSetTest extends BaseConverterTest {
    @Override
    protected Converter createConverter() {
        return PayWallConverter.newBuilder().withWorkingSetSize(2).withProcessingDelay(10).build();
    }

    @Test
    public void testFullOccupation() throws Exception {
        try (Converter converter = createConverter();
             ConverterTask t1 = createTask(converter);
             ConverterTask t2 = createTask(converter)) {
            converter.processFor(t1, 1, TimeUnit.MILLISECONDS);
            converter.processFor(t2, 1, TimeUnit.MILLISECONDS);
            converter.process(t1);
            converter.process(t2);
        }
    }

    @Test
    public void testReclaimSlot() throws Exception {
        try (Converter converter = createConverter();
             ConverterTask t1 = createTask(converter);
             ConverterTask t2 = createTask(converter)) {
            converter.processFor(t1, 1, TimeUnit.MILLISECONDS);
            converter.processFor(t2, 1, TimeUnit.MILLISECONDS);
            converter.process(t1);
            try (ConverterTask t3 = createTask(converter)) {
                converter.process(t3);
            }
            converter.process(t2);
        }
    }

    @Test
    public void testCancelReclaimsSlot() throws Exception {
        try (Converter converter = createConverter();
             ConverterTask t1 = createTask(converter);
             ConverterTask t2 = createTask(converter)) {
            converter.processFor(t1, 1, TimeUnit.MILLISECONDS);
            converter.processFor(t2, 1, TimeUnit.MILLISECONDS);
            t1.cancel(false);
            try (ConverterTask t3 = createTask(converter)) {
                converter.process(t3);
            }
            converter.process(t2);
        }
    }

    public void testExceedWorkingSet() throws Exception {
        boolean threw = false;
        try (Converter converter = createConverter();
             ConverterTask t1 = createTask(converter);
             ConverterTask t2 = createTask(converter)) {
            converter.processFor(t1, 1, TimeUnit.MILLISECONDS);
            converter.processFor(t2, 1, TimeUnit.MILLISECONDS);
            try (ConverterTask t3 = createTask(converter)) {
                converter.processFor(t3, 1, TimeUnit.MILLISECONDS);
            } catch (IllegalStateException e) {
                threw = true;
            }
            converter.process(t1);
            try (ConverterTask t3 = createTask(converter)) {
                converter.process(t3);
            } catch (IllegalStateException e) {
                threw = true;
            }
        }
        Assert.assertTrue(threw);
    }
}
