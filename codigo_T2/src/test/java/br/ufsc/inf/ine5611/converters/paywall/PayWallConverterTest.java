package br.ufsc.inf.ine5611.converters.paywall;

import br.ufsc.inf.ine5611.converters.BaseConverterTest;
import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.impl.PayWallConverter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import org.testng.annotations.Test;

import static br.ufsc.inf.ine5611.converters.Utils.*;

@Test
public class PayWallConverterTest extends BaseConverterTest {
    @Override
    protected Converter createConverter() {
        return new PayWallConverter();
    }
}
