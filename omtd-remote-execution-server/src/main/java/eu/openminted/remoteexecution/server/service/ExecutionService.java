package eu.openminted.remoteexecution.server.service;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.uima.cas.CAS;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import eu.openminted.remoteexecution.server.processor.Processor;
import eu.openminted.remoteexecution.server.processor.ProcessorInput;
import eu.openminted.remoteexecution.server.processor.ProcessorOutput;
import eu.openminted.remoteexecution.server.service.model.Batch;
import eu.openminted.remoteexecution.server.service.model.Execution;

public class ExecutionService {

	//
	private final Map<UUID, Batch> batchMap = new ConcurrentHashMap<>();
	private final Map<UUID, Execution> executionMap = new ConcurrentHashMap<>();

	// TODO jcasgen java classes?
	// private Set<TypeSystemDescription> typeSystems = new
	// HashSet<TypeSystemDescription>();

	private final Processor processor;
	
	private final BlockingQueue<ProcessorInput> processingQueue = new LinkedBlockingQueue<>();
	
	public ExecutionService(Processor processor) {
		this.processor = processor;
		new Thread(processor).start();
	}

	// TODO clean up batches/type periodically

	// TODO return batch object?
	public Batch createBatch(TypeSystemDescription typeSystem) {
		// TODO check if type system has already been cached?
		// System.out.println("contains " + typeSystems.contains(typeSystem));
		// typeSystems.add(typeSystem);
		// TODO create merged type system
		Batch batch = new Batch(typeSystem);
		batchMap.put(batch.id, batch);
		return batch;
	}

	public void deleteBatch(UUID id) {
		batchMap.remove(id);
	}

	public Execution process(InputStream cas, UUID batchId) {
		TypeSystemDescription tsd = batchMap.get(batchId).typeSystemDescription;
		return process(cas, tsd);
	}

	public Execution process(InputStream cas, Optional<TypeSystemDescription> typeSystemDescription) {
		// TODO merge typesystem
		return process(cas, (TypeSystemDescription) null);
	}
	

	private Execution process(InputStream casIs, TypeSystemDescription tsd) {
		ProcessorInput input = new ProcessorInput(casIs);
		Future<ProcessorOutput> result = processor.process(input);
		Execution execution = new Execution(result);
		executionMap.put(execution.id, execution);
		return execution;
	}

	public void deleteProcess(UUID id) {
		Execution execution = executionMap.remove(id);
		execution.close();
	}
	
	public boolean doesProcessExist(UUID id) {
		return executionMap.containsKey(id);
	}

	public ProcessorOutput getResult(UUID id) throws InterruptedException, ExecutionException {
		if ( executionMap.get(id).result.isDone()) {
			return executionMap.get(id).result.get();
		}
		return null;
	}

	/*
	// TODO will move processor to a long-running thread
	class Job implements Callable<ProcessorOutput> {

		final Processor processor;
		final ProcessorInput input;

		public Job(ProcessorInput input, Processor processor) {
			this.input = input;
			this.processor = processor;
		}

		@Override
		public ProcessorOutput call() throws Exception {
			return processor.process(input);
		}

	} */

}
