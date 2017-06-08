package br.ufsc.inf.ine5611.converters.scheduled.impl;

import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import br.ufsc.inf.ine5611.converters.scheduled.ScheduledConverter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.PriorityQueue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PriorityScheduledConverter implements ScheduledConverter {
    public static final int DEFAULT_QUANTUM_LOW = 50;
    public static final int DEFAULT_QUANTUM_NORMAL = 100;
    public static final int DEFAULT_QUANTUM_HIGH = 200;
    
    
    //relogio
    private Stopwatch stopwatch;
    
    //hashmap para os quantum
    private HashMap<Priority, Integer> priorityQuantum;
    
    //fila de prioridades
    private PriorityBlockingQueue<ScheduledConverterTask> priorityQueue;
    
    //task atual
    private ScheduledConverterTask currentTask = null;
    
    //converter
    private final Converter converter;
    
    //selecionador de tasks
    private ChangeTask changeTask;
    
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
            ScheduledConverterTask sct = (ScheduledConverterTask) t;
            sct.complete(null);
            priorityQueue.remove(sct);
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
       // collection.addAll(priorityQueue);
       for (ConverterTask task: priorityQueue) {
                collection.add(task);
        }
        
        return collection;
    }

    /***************************************************************************/
    @Override
    public synchronized ConverterTask convert(InputStream inputStream, OutputStream outputStream,
                                String mediaType, long inputBytes, Priority priority) {

        
        //define o epoch como o tempo atual no relogio
        long epoch = stopwatch.elapsed(MILLISECONDS);
        
        //cria a nova task
        ScheduledConverterTask newTask = new ScheduledConverterTask(
                inputStream, 
                outputStream, 
                mediaType, 
                (t) -> { cancelTask(t); },
                inputBytes, 
                priority, 
                epoch
        );
        
        //adiciona a task na fila
        priorityQueue.add(newTask);
        
        //se for mais prioritaria que a task rodando atualmente, cancela
        if(currentTask != null) {
            if (priority.compareTo(currentTask.getPriority()) == 1)
                interrupt();
        } 
        
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
            cancelTask(task);
        }
    }
       
    /***************************************************************************/
    public boolean cancelTask(ConverterTask task) {
        
        //cancelar task especifica
        if (task == currentTask) converter.interrupt();
        converter.cancel(task); 
        return priorityQueue.remove(task);
    }
    
    /***************************************************************************/
    public synchronized boolean interrupt() {
        
        //interromper operaçao
        if (currentTask == null) return false;
        currentTask = null;
        this.converter.interrupt();
        return true;
    }
   
}
