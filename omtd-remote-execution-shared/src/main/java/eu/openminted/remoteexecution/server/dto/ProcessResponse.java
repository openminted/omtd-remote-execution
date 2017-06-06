package eu.openminted.remoteexecution.server.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ProcessResponse {
	public static final String STATUS_RUNNING = "running";
	public static final String STATUS_FINISHED = "finished";
	public static final String STATUS_ERROR = "error";
	
	public final String url;
	public final String status;
	public final String casUrl;
	public final String typeSystemUrl;
	public final String deletionUrl;

	@JsonCreator
	public ProcessResponse(@JsonProperty("url") String url, @JsonProperty("casUrl") String casUrl, @JsonProperty("typeSystemUrl") String typeSystemUrl,
			@JsonProperty("deletionUrl") String deletionUrl, @JsonProperty("status") String status) {
		this.url = url;
		this.status = status;
		this.casUrl = casUrl;
		this.typeSystemUrl = typeSystemUrl;
		this.deletionUrl = deletionUrl;
	}
	
	public ProcessResponse(String url, String status) {
		this.url = url;
		this.status = status;
		this.casUrl = null;
		this.typeSystemUrl = null;
		this.deletionUrl = null;
	}
}
