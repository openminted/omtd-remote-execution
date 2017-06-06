package eu.openminted.remoteexecution.server.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BatchResponse {
	public final String id;

	@JsonCreator
	public BatchResponse(@JsonProperty("id") String id) {
		this.id = id;
	}
}
