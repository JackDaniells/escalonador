package br.ufsc.inf.ine5611.converters.scheduled.impl;

import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import br.ufsc.inf.ine5611.converters.scheduled.ScheduledConverter;
import br.ufsc.inf.ine5611.converters.scheduled.impl.ScheduledConverterTask;
import com.google.common.base.Stopwatch;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PriorityScheduledConverter implements ScheduledConverter {
    public static final int DEFAULT_QUANTUM_LOW = 50;
    public static final int DEFAULT_QUANTUM_NORMAL = 100;
    public static final int DEFAULT_QUANTUM_HIGH = 200;
    
    //fila de tarefas
     private PriorityBlockingQueue<ScheduledConverterTask> priorityQueue;
     
     private ChangeTask changeTasks;
    
    private Stopwatch stopwatch = Stopwatch.createStarted();
    
    //hashmap para os quantum
    private Map<Priority,Integer> priorityQuantum;
    
    //converter
    private Converter converter;
    
    //processo executando no atual momento
    private ScheduledConverterTask currentTask = null;
  
    public PriorityScheduledConverter(Converter converter) {
        //TODO implementar
        /* - Salve converter como um field, para uso posterior
           - Registre um listener em converter.addCompletionListener() para que você saiba
         *   quando uma tarefa terminou */
        
        //converter salvo
        this.converter = converter;
        
        this.setQuantum(Priority.HIGH, DEFAULT_QUANTUM_HIGH);
        this.setQuantum(Priority.NORMAL, DEFAULT_QUANTUM_NORMAL);
        this.setQuantum(Priority.LOW, DEFAULT_QUANTUM_LOW);
        
        //listener de tarefa terminada
        this.converter.addCompletionListener((ConverterTask t) -> {
            priorityQueue.remove(t);
            System.out.println("task removida");
        });
        
        changeTasks = new ChangeTask();
        
        priorityQuantum = new HashMap<>();
        
        this.priorityQueue = new PriorityBlockingQueue<>(1024, changeTasks);
        
    }

    @Override
    public void setQuantum(Priority priority, int milliseconds) {
        /* Dica: use um HasMap<Priority, Integer> para manter os quanta configurados para
         * cada prioridade */
        this.priorityQuantum.put(priority, milliseconds);
    }

    @Override
    public int getQuantum(Priority priority) {
        /* Veja setQuantum */
        return this.priorityQuantum.get(priority);
    }

    @Override
    public Collection<ConverterTask> getAllTasks() {
        /* Junte todas as tarefas não completas em um Collection */
        Collection<ConverterTask> allTasks = null;
        allTasks.addAll(priorityQueue);
        return allTasks;
    }

    @Override
    public synchronized ConverterTask convert(InputStream inputStream, OutputStream outputStream,
                                String mediaType, long inputBytes, Priority priority) {
        
        // - Crie um objeto ScheduledConverterTask utilizando os parâmetros dessa chamada
        
         // - Adicione o objeto em alguma fila (é possível implementar com uma ou várias filas)
         // - Se a nova tarefa for mais prioritária que a atualmente executando, interrompa
         
        long epoch = stopwatch.elapsed(MILLISECONDS);
        
        ScheduledConverterTask task = new ScheduledConverterTask(
            inputStream, 
            outputStream, 
            mediaType, 
            (t) -> { cancelTask(t); }, 
            inputBytes, 
            priority, 
            epoch 
        );
    
        if(currentTask != null) {
            if (priority.compareTo(currentTask.getPriority()) == 1)
                currentTask = null;
                this.converter.interrupt();
        }

        return task;
    }

    @Override
    public void processFor(long interval, TimeUnit timeUnit) throws InterruptedException {
        /* Pseudocódigo:
         * while (!tempo_estourado) {
         *   t = escolha_tarefa();
         *   t.incCycles();
         *   this.converter.processFor(getQuantum(t.getPriority(), MILLISECONDS);
         * }
         */
        long timeBase = stopwatch.elapsed(timeUnit);
        
        while(stopwatch.elapsed(timeUnit) - timeBase < interval){
            
            ScheduledConverterTask task = priorityQueue.take();
            currentTask = task;
            task.incCycles();
            priorityQueue.add(task);
            try {
                
                this.converter.processFor(task, getQuantum(task.getPriority()) , timeUnit);
            
            } catch (IOException ex) {
                task.completeExceptionally(ex);
                priorityQueue.remove(task);            
            }
            
        }
        
    }

    @Override
    public synchronized void close() throws Exception {
        /* - Libere quaisquer recursos alocados
         * - Cancele as tarefas não concluídas
         */
        for (ConverterTask task : priorityQueue) {
            if(task.isDone()){
                task.close();
            }
            cancelTask(task);
        }
        this.stopwatch.stop();
        this.priorityQuantum.clear();
    }
    
    public boolean cancelTask(ConverterTask task) {
        if (task == currentTask) this.converter.interrupt();
        this.converter.cancel(task); 
        return priorityQueue.remove(task);
    }
    
    
}