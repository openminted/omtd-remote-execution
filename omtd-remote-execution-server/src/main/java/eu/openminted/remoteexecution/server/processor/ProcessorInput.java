package eu.openminted.remoteexecution.server.processor;

import java.io.InputStream;


public class ProcessorInput {
	public final InputStream casIs;
	
	public ProcessorInput(InputStream casIs) {
		this.casIs = casIs;
	}
}
