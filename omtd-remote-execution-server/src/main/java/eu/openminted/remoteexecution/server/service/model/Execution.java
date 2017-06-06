package eu.openminted.remoteexecution.server.service.model;

import java.util.UUID;
import java.util.concurrent.Future;

import eu.openminted.remoteexecution.server.processor.ProcessorOutput;

public class Execution {
	public final UUID id;
	public Future<ProcessorOutput> result;
	
	public Execution(Future<ProcessorOutput> result) {
		id = UUID.randomUUID();
		this.result = result;
	}
	
	public void close() {
		result.cancel(true);
	}
}
