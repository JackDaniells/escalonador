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
    private List<ScheduledConverterTask> queueTasksHighPriority;
    private List<ScheduledConverterTask> queueTasksNormalPriority;
    private List<ScheduledConverterTask> queueTasksLowPriority;
    
    private Stopwatch stopwatch = Stopwatch.createStarted();
    
    //hashmap para os quantum
    private Map<Priority,Integer> priorityQuantum = new HashMap<>();
    
    //converter
    private Converter converter;
    
    //posicao da ultima task de prioridade media executada
    private int lastMediumPriorityTaskExecuted = -1;
    
    private Priority priorityTaskExecuting; 
  
    public PriorityScheduledConverter(Converter converter) {
        //TODO implementar
        /* - Salve converter como um field, para uso posterior
           - Registre um listener em converter.addCompletionListener() para que você saiba
         *   quando uma tarefa terminou */
        
        //converter salvo
        this.converter = converter;
        
        setQuantum(Priority.HIGH, DEFAULT_QUANTUM_HIGH);
        setQuantum(Priority.NORMAL, DEFAULT_QUANTUM_NORMAL);
        setQuantum(Priority.LOW, DEFAULT_QUANTUM_LOW);
        
        //listener de tarefa terminada
        converter.addCompletionListener((ConverterTask t) -> {
             if(this.queueTasksHighPriority.remove(t)||this.queueTasksNormalPriority.remove(t)||this.queueTasksLowPriority.remove(t)){
                System.out.println("task removida");
            }
        });
        
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
        Collection<ConverterTask> collection = null;
        collection.addAll(queueTasksHighPriority);
        collection.addAll(queueTasksNormalPriority);
        collection.addAll(queueTasksLowPriority);
        return collection;
    }

    @Override
    public synchronized ConverterTask convert(InputStream inputStream, OutputStream outputStream,
                                String mediaType, long inputBytes, Priority priority) {
        
        
        long epoch = stopwatch.elapsed(MILLISECONDS);
        // - Crie um objeto ScheduledConverterTask utilizando os parâmetros dessa chamada
        ScheduledConverterTask task = 
            new ScheduledConverterTask(inputStream, outputStream, mediaType, null, inputBytes, priority, epoch );
    
         // - Adicione o objeto em alguma fila (é possível implementar com uma ou várias filas)
         // - Se a nova tarefa for mais prioritária que a atualmente executando, interrompa
           
          switch(priority){     
            case HIGH:
                queueTasksHighPriority.add(task);      
            break;   
            case NORMAL:
                queueTasksNormalPriority.add(task);     
            break;
            case LOW:
                queueTasksLowPriority.add(task);   
            break;          
        } 
        
         /*for(ScheduledConverterTask item : queueTasksHighPriority ){
            if(task.getPriority().compareTo(item.getPriority()) > 0){
                this.converter.interrupt();
            }
        }
  
        for(ScheduledConverterTask item : queueTasksNormalPriority ){
            if(task.getPriority().compareTo(item.getPriority()) > 0){
                this.converter.interrupt();
            }
        }
        
        for(ScheduledConverterTask item : queueTasksLowPriority ){
            if(task.getPriority().compareTo(item.getPriority()) > 0){
                this.converter.interrupt();
            }
        }*/
         
         if(task.getPriority().compareTo(priorityTaskExecuting)> 0){
             
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
            
            ScheduledConverterTask t = changeTask();
            t.incCycles();
            try {
                
                this.converter.processFor(t, getQuantum(t.getPriority()) , timeUnit);
            
            } catch (IOException ex) {
                Logger.getLogger(PriorityScheduledConverter.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
        
    }

    @Override
    public synchronized void close() throws Exception {
        /* - Libere quaisquer recursos alocados
         * - Cancele as tarefas não concluídas
         */
        this.converter.interrupt();
        this.queueTasksHighPriority.removeAll(queueTasksLowPriority);
        this.queueTasksNormalPriority.removeAll(queueTasksNormalPriority);
        this.queueTasksLowPriority.removeAll(queueTasksLowPriority);
        this. stopwatch.stop();
        this.priorityQuantum.clear();
    }
    
    
    private ScheduledConverterTask changeTask(){
        
        if(!queueTasksHighPriority.isEmpty()){
            //executa sempre o primeiro da lista quando tem task na lista hight priority
            return changeTaskHighPriority();
            
        } else if (!queueTasksNormalPriority.isEmpty()){
           //executa todas as task, com um quantum de cada task na lista
           return changeTaskNormalPriority();
            
        } else if (!queueTasksLowPriority.isEmpty()){
            //executa a task com menor tamanho
            return changeTaskLowPriority();
            
        } else {
            return null;
        }
    }
    
    
    
    
    private ScheduledConverterTask changeTaskHighPriority(){
        priorityTaskExecuting = Priority.HIGH;
        return queueTasksHighPriority.get(0);
    }
    
    
    
    
    private ScheduledConverterTask changeTaskNormalPriority(){
        
        if(lastMediumPriorityTaskExecuted < 0){
            
            if(lastMediumPriorityTaskExecuted < queueTasksNormalPriority.size()){
                
                priorityTaskExecuting = Priority.NORMAL;
                return queueTasksNormalPriority.get(lastMediumPriorityTaskExecuted);
                
            }else {
                
                priorityTaskExecuting = Priority.NORMAL;
                return queueTasksNormalPriority.get(0); 
               
            }
            
        }else{
            
            priorityTaskExecuting = Priority.NORMAL;
            return queueTasksNormalPriority.get(0); 
        }
    }
    
    
    
    
    private ScheduledConverterTask changeTaskLowPriority(){
        
        int lowerSizePosition = 0;
        for(int i = 0; i < queueTasksLowPriority.size(); i++){
            if(queueTasksLowPriority.get(i).getInputBytes() < queueTasksLowPriority.get(lowerSizePosition).getInputBytes()){
                lowerSizePosition = i;
            }
        }
        
        priorityTaskExecuting = Priority.LOW;
        return queueTasksLowPriority.get(lowerSizePosition);
    }
}
