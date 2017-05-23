package br.ufsc.inf.ine5611.converters.scheduled;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface ScheduledConverter extends AutoCloseable {
    /**
     * Cria uma tarefa de conversão e submete para escalonamento.
     *
     * @param inputStream Stream de entrada da imagem a ser convertida
     * @param outputStream Saída da imagem a ser convertida
     * @param mediaType Internet Media Type da entrada, usado para escolher o decodificador.
     *                  Ex: image/jpeg
     * @param inputBytes Total de bytes no stream de entrada
     * @param priority Prioridade da tarefa
     * @return Objeto representando a tarefa que permite consultar seu estado, aguardar por
     * seu término ou ainda cancelar a tarefa.
     */
    ConverterTask convert(InputStream inputStream, OutputStream outputStream, String mediaType,
                          long inputBytes, Priority priority);

    /**
     * Processa tarefas de acordo com a política de escalonamento da implementação pelo tempo
     * especificado.
     *
     * @param interval Intervalo de tempo para processar
     * @param timeUnit Unidade de tempo do intervalo
     * @throws InterruptedException Caso esse método seja interrompido durante uma operaçõa de
     * wait por um Thread.interrupt()
     */
    void processFor(long interval, TimeUnit timeUnit) throws InterruptedException;

    /**
     * Configura o quantum a ser usado para processos com o nível de prioridade dado.
     * @param priority Nível de prioridade a ser configurado.
     * @param milliseconds Milisegundos a serem usados como quantum
     */
    void setQuantum(Priority priority, int milliseconds);

    /**
     * Consulta o valor configurado por setQuantum()
     *
     * @param priority Nível de prioridade a ser consultado.
     * @return o quantum, em milisegundos.
     */
    int getQuantum(Priority priority);

    /**
     * Obtem todas as tarefas não completadas sob gerencia desse {@link ScheduledConverter}
     * @return Uma coleção não-modificável de {@link ConverterTask}s
     */
    Collection<ConverterTask> getAllTasks();
}
