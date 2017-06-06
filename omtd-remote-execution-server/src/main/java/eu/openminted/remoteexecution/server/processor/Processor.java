package eu.openminted.remoteexecution.server.processor;

import java.util.concurrent.Future;

public interface Processor extends Runnable {
	Future<ProcessorOutput> process(ProcessorInput input);
}
