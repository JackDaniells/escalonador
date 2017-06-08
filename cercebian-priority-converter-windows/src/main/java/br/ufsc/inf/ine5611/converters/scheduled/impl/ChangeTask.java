/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.ufsc.inf.ine5611.converters.scheduled.impl;

import br.ufsc.inf.ine5611.converters.scheduled.Priority;
import java.util.Comparator;

/**
 *
 * @author JackDaniels
 */
public class ChangeTask implements Comparator<ScheduledConverterTask> {

    @Override
    public int compare(ScheduledConverterTask current, ScheduledConverterTask newTask) {
        int compare = newTask.getPriority().compareTo(current.getPriority());
        if (compare != 0) return compare;
        
        switch(current.getPriority()) {
            
            case HIGH:
                compare = Long.compare(current.getInputBytes(), newTask.getInputBytes());
                if (compare == 0) {
                    compare = Long.compare(current.getCycles(), newTask.getCycles());
                } else {
                    return compare;
                }
            break;
            
            case NORMAL:
      
                compare = Long.compare(current.getCycles(), newTask.getCycles());
                if(compare == 0){
                    return Long.compare(current.getEpoch(), newTask.getEpoch());
                }
                return compare;
            
            
            case LOW:
            
                compare = Long.compare(current.getEpoch(), newTask.getEpoch());
                return compare;
        }
        
        return 0; 
    } 
}
   
