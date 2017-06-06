package eu.openminted.remoteexecution.server.processor;

public class ProcessorOutput {
	public final byte[] typeSystemDescription;
	public final byte[] cas;
	
	public ProcessorOutput(byte[] cas, byte[] tsd) {
		this.cas = cas;
		this.typeSystemDescription = tsd;
	}
}
