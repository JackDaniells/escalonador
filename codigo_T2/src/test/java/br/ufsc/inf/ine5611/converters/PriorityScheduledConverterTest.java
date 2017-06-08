package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import com.google.common.base.Stopwatch;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static br.ufsc.inf.ine5611.converters.ConverterEvent.*;
import static br.ufsc.inf.ine5611.converters.Utils.alien;
import static br.ufsc.inf.ine5611.converters.Utils.spock;
import static br.ufsc.inf.ine5611.converters.scheduled.Priority.*;

public class PriorityScheduledConverterTest extends PriorityScheduledConverterTestBase {
    @Test
    public void testHighPriority() throws Exception {
        setup(1);
        for (int i = 0; i < 4; i++)
            tasks.add(alien(priorityConverter, NORMAL));
        tasks.add(alien(priorityConverter, HIGH)); // index: 4
        run();

        int processIdx = indexOf(PROCESS, 4);
        int completionIdx = indexOf(COMPLETION, 4);
        /* T4 é a primeira tarefa a ser executada */
        Assert.assertEquals(processIdx, 0);
        /* T4 não pode ser interrompido por tarefa menos prioritária */
        Assert.assertTrue(indexOf(PROCESS, -4, processIdx, completionIdx) < 0);
        /* T4 nunca é interrompida */
        Assert.assertTrue(indexOf(INTERRUPT, 4) < 0);
    }

    @Test
    public void testLowPriority() throws Exception {
        setup(1);
        tasks.add(alien(priorityConverter, LOW));
        tasks.add(alien(priorityConverter, HIGH));
        tasks.add(alien(priorityConverter, NORMAL));
        run();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNormalRegime() throws Exception {
        setup(100);
        priorityConverter.setQuantum(NORMAL, 70);
        for (int i = 0; i < 3; i++) tasks.add(alien(priorityConverter, NORMAL));
        run();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(2)));

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testLowRegime() throws Exception {
        setup(100);
        priorityConverter.setQuantum(LOW, 70);
        tasks.add(spock(priorityConverter, LOW));
        tasks.add(spock(priorityConverter, LOW));
        tasks.add(alien(priorityConverter, LOW));
        run();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testHighRegime() throws Exception {
        setup(100);
        priorityConverter.setQuantum(HIGH, 70);
        tasks.add(alien(priorityConverter, HIGH));
        tasks.add(spock(priorityConverter, HIGH));
        tasks.add(alien(priorityConverter, HIGH));
        run();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(2)));

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testMixedPrioritiesRegime() throws Exception {
        setup(100);
        priorityConverter.setQuantum(LOW, 70);
        priorityConverter.setQuantum(NORMAL, 70);
        priorityConverter.setQuantum(HIGH, 70);
        tasks.add(alien(priorityConverter, LOW));
        tasks.add(spock(priorityConverter, NORMAL));
        tasks.add(alien(priorityConverter, HIGH));
        run();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testPreempt() throws Exception {
        setup(200);
        priorityConverter.setQuantum(LOW, 150);
        priorityConverter.setQuantum(NORMAL, 150);
        priorityConverter.setQuantum(HIGH, 150);
        tasks.add(spock(priorityConverter, NORMAL));
        start();
        sleep(100);
        tasks.add(alien(priorityConverter, HIGH));
        waitForDone();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(INTERRUPT, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));

        Assert.assertTrue(actual.size() >= expected.size());
        Assert.assertEquals(actual.subList(0, expected.size()), expected);

        if (actual.size() == expected.size()+3)
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        //expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testQuantum() throws Exception {
        setup(100);
        priorityConverter.setQuantum(LOW, 40);
        priorityConverter.setQuantum(NORMAL, 60);
        priorityConverter.setQuantum(HIGH, 120);
        tasks.add(alien(priorityConverter, LOW));
        tasks.add(alien(priorityConverter, NORMAL));
        tasks.add(alien(priorityConverter, HIGH));
        run();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(2)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(2)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(0)));

        Assert.assertEquals(actual, expected);
    }

    @Test(timeOut = 1000)
    public void testRunWithNoTasks() throws Exception {
        setup(100);
        run();
    }

    @Test
    public void testNoTwoProcessFor() throws Exception {
        setup(2000);
        tasks.add(alien(priorityConverter, HIGH));

        Future<Object> future = Executors.newFixedThreadPool(1).submit(() -> {
            Stopwatch w = Stopwatch.createStarted();
            priorityConverter.processFor(1, TimeUnit.SECONDS);
            System.out.printf("processed for %s", w);
            return null;
        });
        try {
            sleep(100);
            boolean caught = false;
            try {
                priorityConverter.processFor(1000, TimeUnit.MILLISECONDS);
            } catch (IllegalStateException e) {
                caught = true;
            }
            Assert.assertTrue(caught);
        } finally {
            future.get();
        }
    }

    @Test
    public void testCancel() throws Exception {
        setup(500);
        priorityConverter.setQuantum(NORMAL, 600);
        priorityConverter.setQuantum(HIGH, 600);
        tasks.add(alien(priorityConverter, HIGH));
        tasks.add(alien(priorityConverter, NORMAL));
        start();
        sleep(200);
        tasks.get(0).cancel(true);
        waitForDone();

        ArrayList<ConverterTaskEvent> actual = visibleConverter.getEvents();
        ArrayList<ConverterTaskEvent> expected = new ArrayList<>();
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected.add(new ConverterTaskEvent(CANCEL, tasks.get(0)));
        expected.add(new ConverterTaskEvent(INTERRUPT, tasks.get(0)));
        expected.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));

        ArrayList<ConverterTaskEvent> expected2= new ArrayList<>();
        expected2.add(new ConverterTaskEvent(PROCESS, tasks.get(0)));
        expected2.add(new ConverterTaskEvent(INTERRUPT, tasks.get(0)));
        expected2.add(new ConverterTaskEvent(PROCESS, tasks.get(1)));
        expected2.add(new ConverterTaskEvent(COMPLETION, tasks.get(1)));

        //Perdão por esquecer converter.cancel()
        if (actual.equals(expected2))
            Assert.assertEquals(actual, expected2);
        else
            Assert.assertEquals(actual, expected);
    }
}
