package eu.openminted.remoteexecution.server.processor.uima;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.uima.UIMAException;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.fit.component.Resource_ImplBase;
import org.apache.uima.fit.descriptor.ExternalResource;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.internal.ResourceManagerFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.Progress;
import org.apache.uima.util.TypeSystemUtil;
import org.xml.sax.SAXException;

import eu.openminted.remoteexecution.server.processor.Processor;
import eu.openminted.remoteexecution.server.processor.ProcessorInput;
import eu.openminted.remoteexecution.server.processor.ProcessorOutput;

public class UimaPipelineProcessor implements Processor {

	private final UimaPipeline pipeline;
	private BlockingQueue<ProcessorInput> queue = new LinkedBlockingQueue<ProcessorInput>();
	private Map<ProcessorInput, CompletableFuture<ProcessorOutput>> inputOutputMap = new ConcurrentHashMap<>();

	public UimaPipelineProcessor(UimaPipeline pipeline) {
		this.pipeline = pipeline;
	}

	@Override
	public Future<ProcessorOutput> process(ProcessorInput input) {
		CompletableFuture<ProcessorOutput> futureOutput = new CompletableFuture<>();
		inputOutputMap.put(input, futureOutput);
		// TODO what if queue is full and next call is false? Throw error?
		queue.offer(input);
		return futureOutput;
	}

	@Override
	public void run() {
		while (!Thread.interrupted()) {
			try {
				/*
				 * ExternalResourceDescription sharedModel =
				 * ExternalResourceFactory.createExternalResourceDescription(
				 * SharedModel.class, SharedModel.QUEUE_KEY, queue,
				 * SharedModel.INPUT_OUTPUT_MAP_KEY, inputOutputMap);
				 * 
				 * CollectionReaderDescription reader =
				 * CollectionReaderFactory.createReaderDescription(CasReader.
				 * class, CasReader.SHARED_MODEL, sharedModel);
				 * 
				 * AnalysisEngineDescription writer =
				 * AnalysisEngineFactory.createEngineDescription(CasWriter.
				 * class, CasReader.SHARED_MODEL, sharedModel);
				 */

				List<AnalysisEngineDescription> pipelineEngines = pipeline.analysisEngines();
				AnalysisEngineDescription[] engines = new AnalysisEngineDescription[pipelineEngines.size()];
				pipelineEngines.toArray(engines);
				// engines[engines.length - 1] = writer;
				runPipeline(engines);
			} catch (Exception e) {
				if (!Thread.interrupted()) {
					System.out.println("Processing failed. Pipeline stopped.");
					e.printStackTrace();
					System.out.println("Attempting to restart pipeline");
				}
			}
		}
	}

	void runPipeline(AnalysisEngineDescription[] descs)
			throws UIMAException, IOException, SAXException, InterruptedException {
		ResourceManager resMgr = ResourceManagerFactory.newResourceManager();
		// CollectionReader reader =
		// UIMAFramework.produceCollectionReader(readerDesc, resMgr, null);
		AnalysisEngineDescription aaeDesc = AnalysisEngineFactory.createEngineDescription(descs);
		AnalysisEngine aae = UIMAFramework.produceAnalysisEngine(aaeDesc, resMgr, null);
		CAS cas = CasCreationUtils.createCas(Arrays.asList(new ResourceMetaData[] { aae.getMetaData() }), null, resMgr);
		// reader.typeSystemInit(cas.getTypeSystem());
		try {

			byte[] serialisedCas;
			byte[] serialisedTypeSystem;
			while (true) {

				ProcessorInput input = queue.take();
				CompletableFuture<ProcessorOutput> output = inputOutputMap.get(input);

				XmiCasDeserializer.deserialize(input.casIs, cas, true);
			
				aae.process(cas);

				// TODO complete output when error occurs
				try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
					XmiCasSerializer.serialize(cas, os);
					serialisedCas = os.toByteArray();

					os.reset();
					TypeSystemUtil.typeSystem2TypeSystemDescription(cas.getTypeSystem())
							.toXML(os);
					serialisedTypeSystem = os.toByteArray();
				}

				ProcessorOutput result = new ProcessorOutput(serialisedCas, serialisedTypeSystem);
				output.complete(result);

				cas.reset();
			}
		} finally {
			aae.destroy();
		}
	}

	static class SharedModel extends Resource_ImplBase {
		final static String QUEUE_KEY = "queue";
		@ExternalResource(key = QUEUE_KEY)
		private BlockingQueue<ProcessorInput> queue;

		final static String INPUT_OUTPUT_MAP_KEY = "inputOutputMap";
		@ExternalResource(key = INPUT_OUTPUT_MAP_KEY)
		private Map<ProcessorInput, CompletableFuture<ProcessorOutput>> inputOutputMap;

		final static String INPUT_NEXT_OUTPUT_KEY = "nextOutput";
		@ExternalResource(key = INPUT_NEXT_OUTPUT_KEY)
		private CompletableFuture<ProcessorOutput> nextOutput;

	}

	static class CasReader extends JCasCollectionReader_ImplBase {

		final static String SHARED_MODEL = "sharedModel";
		@ExternalResource(key = SHARED_MODEL)
		private SharedModel sharedModel;

		@Override
		public Progress[] getProgress() {
			return null;
		}

		@Override
		public boolean hasNext() throws IOException, CollectionException {
			return true;
		}

		@Override
		public void getNext(JCas jcas) throws IOException, CollectionException {
			ProcessorInput input;
			try {
				input = sharedModel.queue.take();
				sharedModel.nextOutput = sharedModel.inputOutputMap.get(input);

				XmiCasDeserializer.deserialize(input.casIs, jcas.getCas(), true);

			} catch (InterruptedException | SAXException e) {
				throw new CollectionException(e);
			}
		}

	}

	static class CasWriter extends JCasAnnotator_ImplBase {
		final static String SHARED_MODEL = "sharedModel";
		@ExternalResource(key = SHARED_MODEL)
		private SharedModel sharedModel;

		@Override
		public void process(JCas jcas) throws AnalysisEngineProcessException {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			try {
				XmiCasSerializer.serialize(jcas.getCas(), os);
				ProcessorOutput result = new ProcessorOutput(os.toByteArray(), null);
				sharedModel.nextOutput.complete(result);
				sharedModel.nextOutput = null;
			} catch (SAXException e) {
				throw new AnalysisEngineProcessException(e);
			}
		}
	}

}
