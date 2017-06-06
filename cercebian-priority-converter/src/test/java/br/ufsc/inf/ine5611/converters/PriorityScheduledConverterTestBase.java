package br.ufsc.inf.ine5611.converters;

import br.ufsc.inf.ine5611.converters.impl.PayWallConverter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import br.ufsc.inf.ine5611.converters.scheduled.impl.PriorityScheduledConverter;
import com.google.common.base.Stopwatch;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static br.ufsc.inf.ine5611.converters.ConverterEvent.*;
import static br.ufsc.inf.ine5611.converters.Utils.*;
import static br.ufsc.inf.ine5611.converters.scheduled.Priority.*;

@SuppressWarnings("WeakerAccess")
public abstract class PriorityScheduledConverterTestBase {
    protected VisibleConverter visibleConverter;
    protected PriorityScheduledConverter priorityConverter;
    protected Future<Object> runFuture;
    protected List<ConverterTask> tasks;
    protected ExecutorService executorService;
    protected HashMap<Priority, Integer> quantumMs;

    protected void sleep(int milliseconds) throws InterruptedException {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            Stopwatch w = Stopwatch.createStarted();
            while (w.elapsed(TimeUnit.MILLISECONDS) < milliseconds) { }
        } else {
            Thread.sleep(milliseconds);
        }
    }

    protected void setup(int processingDelay) {
        visibleConverter = new VisibleConverter(PayWallConverter.newBuilder()
                .withWorkingSetSize(Integer.MAX_VALUE).withProcessingDelay(processingDelay).build());
        priorityConverter = new PriorityScheduledConverter(visibleConverter);
    }

    protected void start() {
        executorService = Executors.newSingleThreadExecutor();
        runFuture = executorService.submit(() -> {
            priorityConverter.processFor(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    protected void run() throws ExecutionException, InterruptedException {
        start();
        waitForDone();

    }

    protected void waitForDone() throws InterruptedException, ExecutionException {
        boolean allDone = false;
        LinkedList<ConverterTask> list = new LinkedList<>(tasks);
        while (!list.isEmpty()) {
            if (runFuture.isDone())
                runFuture.get(); //throws an ExecutionException if appropriate
            for (Iterator<ConverterTask> it = list.iterator(); it.hasNext(); ) {
                ConverterTask task = it.next();
                if (task.isDone()) {
                    it.remove();
                    continue;
                }
                try {
                    task.get(30, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) { /*pass*/ }
            }
        }
    }

    @BeforeClass
    public void classInit() {
        quantumMs = new HashMap<>();
        quantumMs.put(LOW, PriorityScheduledConverter.DEFAULT_QUANTUM_LOW);
        quantumMs.put(NORMAL, PriorityScheduledConverter.DEFAULT_QUANTUM_NORMAL);
        quantumMs.put(HIGH, PriorityScheduledConverter.DEFAULT_QUANTUM_HIGH);
    }

    @BeforeMethod
    public void init() {
        tasks = new ArrayList<>();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        tasks.forEach(t -> t.cancel(true));

        if (runFuture != null)
            runFuture.cancel(true);
        runFuture = null;
        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            executorService = null;
        }

        for (ConverterTask task : tasks) task.close();

        if (priorityConverter != null)
            priorityConverter.close();
        priorityConverter = null;
        if (visibleConverter != null)
            visibleConverter.close();
        visibleConverter = null;
    }

    protected int indexOf(ConverterEvent event, int taskIndex) {
        return indexOf(event, taskIndex, 0, -1);
    }
    protected int indexOf(ConverterEvent event, int taskIndex, int begin, int end) {
        ArrayList<ConverterTaskEvent> list = visibleConverter.getEvents();
        end = end < 0 ? list.size() : end;
        Predicate<ConverterTask> predicate = taskIndex >= 0 ?
                  t ->  Objects.equals(t, tasks.get(taskIndex))
                : t -> !Objects.equals(t, tasks.get(-1*taskIndex));
        for (int i = begin; i < end; i++) {
            if (list.get(i).event == event && predicate.test(list.get(i).task))
                return i;
        }
        return -1;
    }

    protected List<ConverterTaskEvent> filter(ConverterEvent event, int taskIndex, int begin, int end) {
        ArrayList<ConverterTaskEvent> l = visibleConverter.getEvents();
        end = end < 0 ? l.size() : end;
        Predicate<ConverterTask> predicate = taskIndex >= 0 ?
                t ->  Objects.equals(t, tasks.get(taskIndex))
                : t -> !Objects.equals(t, tasks.get(-1*taskIndex));
        List<ConverterTaskEvent> result = new ArrayList<>();
        for (int i = begin; i < end; i++) {
            if ((event == null || l.get(i).event == event) && predicate.test(l.get(i).task))
                result.add(l.get(i));
        }
        return result;
    }
    protected int count(ConverterEvent event, int taskIndex, int begin, int end) {
        return filter(event, taskIndex, begin, end).size();
    }

}
