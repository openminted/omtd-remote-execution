package eu.openminted.remoteexecution.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.uima.UIMAException;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.xml.sax.SAXException;

import com.gc.iotools.stream.os.OutputStreamToInputStream;

import eu.openminted.remoteexecution.server.dto.ProcessResponse;

public class RemoteComponent extends JCasAnnotator_ImplBase {

	public static final String URL_PARAM = "url";
	@ConfigurationParameter(mandatory = true)
	private String url;

	public static final String POLLING_INTERVAL_PARAM = "pollingInterval";
	@ConfigurationParameter(mandatory = false)
	private long pollingInterval = 1000;

	private String processUrl;

	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		processUrl = UriComponentsBuilder.fromHttpUrl(url).path("/process").toUriString();
	}

	@Override
	public void process(JCas jcas) throws AnalysisEngineProcessException {
		final RestTemplate restTemplate = buildRestTemplate();

		ProcessResponse response;
		try {
			// TODO upload Type System alongside CAS
			response = upload(jcas.getCas(), restTemplate);
		} catch (Exception e) {
			throw new AnalysisEngineProcessException(e);
		}
		response = waitForResult(response, restTemplate);
		downloadCas(response, jcas.getCas(), restTemplate);
		// TODO download Type System



		// Delete processing results from the remote server
		restTemplate.delete(response.deletionUrl);
	}

	RestTemplate buildRestTemplate() {
		final RestTemplate restTemplate = new RestTemplate();
		final SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
		requestFactory.setBufferRequestBody(false);
		restTemplate.setRequestFactory(requestFactory);
		return restTemplate;
	}
	
	void downloadTypeSystem() {
		// TODO
	}

	void downloadCas(ProcessResponse response, CAS cas, RestTemplate restTemplate) {
		restTemplate.execute(response.casUrl, HttpMethod.GET, null, new ResponseExtractor<Void>() {
			@Override
			public Void extractData(ClientHttpResponse response) throws IOException {
				try {
					XmiCasDeserializer.deserialize(response.getBody(), cas, true);
				} catch (SAXException e) {
					throw new IOException(e);
				}
				return null;
			}
		});
	}

	ProcessResponse waitForResult(ProcessResponse response, RestTemplate restTemplate) {
		String url = response.url;
		response = null;
		do {
			if (response != null) {
				try {
					Thread.sleep(pollingInterval);
				} catch (InterruptedException e) {
				}
			}
			response = restTemplate.getForObject(url, ProcessResponse.class);
		} while (ProcessResponse.STATUS_RUNNING.equals(response.status));

		return response;
	}

	// TODO add type system
	ProcessResponse upload(CAS cas, RestTemplate restTemplate) throws Exception {
		try (final OutputStreamToInputStream<ProcessResponse> os = new OutputStreamToInputStream<ProcessResponse>() {
			@Override
			protected ProcessResponse doRead(final InputStream is) throws Exception {
				final MultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
				map.add("cas", new MultipartInputStreamFileResource(is, "cas.xml"));

				HttpEntity<MultiValueMap<String, Object>> resource = new HttpEntity<MultiValueMap<String, Object>>(map);
				return restTemplate.postForObject(processUrl, resource, ProcessResponse.class);
			}
		}) {
			XmiCasSerializer.serialize(cas, os);
			return os.getResult();
		}
	}

	private class MultipartInputStreamFileResource extends InputStreamResource {

		private final String filename;

		public MultipartInputStreamFileResource(InputStream inputStream, String filename) {
			super(inputStream);
			this.filename = filename;
		}

		@Override
		public String getFilename() {
			return this.filename;
		}

		@Override
		public long contentLength() throws IOException {
			return -1; // we do not want to generally read the whole stream into
						// memory ...
		}
	}

	public static void main(String[] args) throws UIMAException, SAXException, IOException {
		AnalysisEngineDescription ae = AnalysisEngineFactory.createEngineDescription(RemoteComponent.class,
				RemoteComponent.URL_PARAM, "http://localhost:8080");
		File casFile = new File("/Users/mbassjc5/OMTD/corpus/test.txt.xmi");
		FileInputStream fis = new FileInputStream(casFile);
		JCas jcas = JCasFactory.createJCas();
		CAS cas = jcas.getCas();
		// XmiCasDeserializer.deserialize(fis, cas);
		cas.setDocumentText(
				"Aurasperone F- a new member of the naphtho-gamma-pyrone class isolated from a cultured microfungus, Aspergillus niger C-433. The mobile phase comprised methanol and 20 mm potassium dihydrogen phosphate (adjusted to a pH of 3.19 with o-phosphoric acid), and gradient elution mode was applied.");
		
		SimplePipeline.runPipeline(cas, ae);
	}

}
