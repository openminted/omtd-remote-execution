package eu.openminted.remoteexecution.server.service.model;

import java.util.UUID;

import org.apache.uima.resource.metadata.TypeSystemDescription;

public class Batch {
	public final UUID id;
	public final TypeSystemDescription typeSystemDescription;
	
	public Batch(TypeSystemDescription tsd) {
		id = UUID.randomUUID();
		this.typeSystemDescription = tsd;
	}
	
}
