package br.ufsc.inf.ine5611.converters.scheduled.impl;

import br.ufsc.inf.ine5611.converters.Converter;
import br.ufsc.inf.ine5611.converters.scheduled.ConverterTask;
import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import br.ufsc.inf.ine5611.converters.scheduled.ScheduledConverter;
import br.ufsc.inf.ine5611.converters.scheduled.impl.ScheduledConverterTask;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PriorityScheduledConverter implements ScheduledConverter {
    public static final int DEFAULT_QUANTUM_LOW = 50;
    public static final int DEFAULT_QUANTUM_NORMAL = 100;
    public static final int DEFAULT_QUANTUM_HIGH = 200;
    
    //fila de tarefas
    List<ScheduledConverterTask> queueTasksHighPriority;
    List<ScheduledConverterTask> queueTasksNormalPriority;
    List<ScheduledConverterTask> queueTasksLowPriority;
    
    //hashmap para os quantum
    Map<Priority,Integer> priorityQuantum = new HashMap<>();
    
    //converter
    Converter converter;
    
    //

    public PriorityScheduledConverter(Converter converter) {
        //TODO implementar
        /* - Salve converter como um field, para uso posterior
           - Registre um listener em converter.addCompletionListener() para que você saiba
         *   quando uma tarefa terminou */
        
        //converter salvo
        this.converter = converter;
        
        //listener de tarefa terminada
        converter.addCompletionListener((t) -> {
        });
        
    }

    @Override
    public void setQuantum(Priority priority, int milliseconds) {
        /* Dica: use um HasMap<Priority, Integer> para manter os quanta configurados para
         * cada prioridade */
       
        priorityQuantum.put(priority, milliseconds);
    }

    @Override
    public int getQuantum(Priority priority) {
        /* Veja setQuantum */

        return priorityQuantum.get(priority);
    }

    @Override
    public Collection<ConverterTask> getAllTasks() {
        /* Junte todas as tarefas não completas em um Collection */
        //TODO implementar
        return null;
    }

    @Override
    public synchronized ConverterTask convert(InputStream inputStream, OutputStream outputStream,
                                String mediaType, long inputBytes, Priority priority) {
        
        
        long epoch = 0;
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
         
         
        //TODO implementar
        return null;
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
        //TODO implementar
     
    }

    @Override
    public synchronized void close() throws Exception {
        /* - Libere quaisquer recursos alocados
         * - Cancele as tarefas não concluídas
         */
        converter.interrupt();
    }
}
