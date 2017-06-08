/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufsc.inf.ine5611.converters.scheduled.impl;

import java.util.Comparator;

/**
 *
 * @author ASUS-DEV
 */
public class ChangeTask implements Comparator<ScheduledConverterTask> {
    
    @Override
    public int compare(ScheduledConverterTask current, ScheduledConverterTask newTask) {
        
        
        int compareValue = newTask.getPriority().compareTo(current.getPriority());
        if (compareValue != 0) return compareValue;
        
        //seleciona tarefa com base na sua prioridade
        switch(current.getPriority()) {
            
            case LOW:   
                //SJF
                compareValue = Long.compare(current.getInputBytes(), newTask.getInputBytes());
                if (compareValue == 0) compareValue = Long.compare(current.getCycles(), newTask.getCycles());
                return compareValue;
        
            case NORMAL:
                //RR
                compareValue = Long.compare(current.getCycles(), newTask.getCycles());
                if (compareValue == 0) return Long.compare(current.getEpoch(), newTask.getEpoch());
                return compareValue;
            
            case HIGH:
                //FIFO
                compareValue = Long.compare(current.getEpoch(), newTask.getEpoch());
                return compareValue;
                
            default:
                
                return 0;
                
                
        }
    }   
}
