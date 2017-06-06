package eu.openminted.remoteexecution.server.processor.uima;

import java.util.List;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;

public interface UimaPipeline {
	List<AnalysisEngineDescription> analysisEngines() throws Exception;
}
