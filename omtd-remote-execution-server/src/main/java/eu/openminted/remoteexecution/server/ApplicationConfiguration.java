package eu.openminted.remoteexecution.server;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {
	@Value("${omtd.processor.class}")
	private String processorClass;

	
	public String getProcessorClass() {
		return processorClass;
	}
}
