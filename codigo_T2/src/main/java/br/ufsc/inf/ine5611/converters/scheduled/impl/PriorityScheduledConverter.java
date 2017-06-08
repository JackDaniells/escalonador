package br.ufsc.inf.ine5611.converters.scheduled.impl;

import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import br.ufsc.inf.ine5611.converters.scheduled.ScheduledConverter;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;

public class PriorityScheduledConverter implements ScheduledConverter {
    public static final int DEFAULT_QUANTUM_LOW = 50;
    public static final int DEFAULT_QUANTUM_NORMAL = 100;
    public static final int DEFAULT_QUANTUM_HIGH = 200;
    
    //relogio
    private final Stopwatch stopwatch;
    
    //hashmap para os quantum
    private final HashMap<Priority, Integer> priorityQuantum;
    
    //fila de prioridades
    private PriorityBlockingQueue<ScheduledConverterTask> priorityQueue;
    
    //task atual
    private ScheduledConverterTask currentTask = null;
    
    //converter
    private final Converter converter;
    
    //selecionador de tasks
    private final ChangeTask changeTask;
    
    //idade da tarefa
    private long epoch = 0;
    
    /***************************************************************************/
    public PriorityScheduledConverter(Converter converter) {

        //converter salvo
        this.converter = converter;
        
        //inicializa o relogio
        this.stopwatch = Stopwatch.createStarted();
        
        //inicializa o hashmap
        this.priorityQuantum = new HashMap<>();
        
        //salva os quantum default no hashmap
        setQuantum(Priority.LOW, DEFAULT_QUANTUM_LOW);
        setQuantum(Priority.NORMAL, DEFAULT_QUANTUM_NORMAL);
        setQuantum(Priority.HIGH, DEFAULT_QUANTUM_HIGH);
        
        //listener de tarefa terminada
        this.converter.addCompletionListener((t) -> {
            priorityQueue.remove(t);
            ScheduledConverterTask task = (ScheduledConverterTask) t;
            task.complete(null);
        });
        
        //inicializa o ChangeTask
        this.changeTask = new ChangeTask();
        
        //inicializa a fila de prioridades
        this.priorityQueue = new PriorityBlockingQueue<>(1024, changeTask);
             
    }
    

    /***************************************************************************/
    @Override
    public void setQuantum(Priority priority, int milliseconds) {
 
        priorityQuantum.put(priority, milliseconds);
      
    }

    /***************************************************************************/
    @Override
    public int getQuantum(Priority priority) {
    
        return priorityQuantum.get(priority);
    }

    /***************************************************************************/
    @Override
    public Collection<ConverterTask> getAllTasks() {
       
        Collection<ConverterTask> collection = new ArrayList<>();
        collection.addAll(priorityQueue);
        
        return collection;
    }

    /***************************************************************************/
    @Override
    public synchronized ConverterTask convert(InputStream inputStream, OutputStream outputStream,
                                String mediaType, long inputBytes, Priority priority) {

        
        //define o epoch como o tempo atual no relogio
        //long epoch = stopwatch.elapsed(MILLISECONDS);
        
        //cria a nova task
        ScheduledConverterTask newTask = new ScheduledConverterTask(
                inputStream, 
                outputStream, 
                mediaType, 
                (t) -> { cancel(t); },
                inputBytes, 
                priority, 
                epoch++
        );
        
        //adiciona a task na fila
        priorityQueue.add(newTask);
        
        //se for mais prioritaria que a task rodando atualmente, cancela
        if(currentTask != null && priority.compareTo(currentTask.getPriority()) == 1) interrupt();
        
        return newTask;     
    }

    /***************************************************************************/
    @Override
    public void processFor(long interval, TimeUnit timeUnit) throws InterruptedException {
    
        long timeBase = stopwatch.elapsed(timeUnit);
        while(stopwatch.elapsed(timeUnit) - timeBase < interval ) {
            
             ScheduledConverterTask task = priorityQueue.take();
             currentTask = task;
             task.incCycles();
             priorityQueue.add(task);
             
            try {
                if (!task.isDone()) 
                    this.converter.processFor(task, getQuantum(task.getPriority()), timeUnit);
                
            } catch (IOException ex) {
                
                task.completeExceptionally(ex);
                priorityQueue.remove(task);
                
            }
         }
    }

    /***************************************************************************/
    @Override
    public synchronized void close() throws Exception {
        
        for (ConverterTask task : priorityQueue) {
            if(task.isDone()){
                task.close();
            }
            cancel(task);
        }
    }
       
    /***************************************************************************/
    public void cancel(ConverterTask task) {
        
        //cancelar task especifica
        priorityQueue.remove(task);
        converter.cancel(task); 
        if (task == currentTask) converter.interrupt();
    }
    
    /***************************************************************************/
    public synchronized void interrupt() {
        
        //interromper operaçao
        currentTask = null;
        this.converter.interrupt();
       
    }
   
}
