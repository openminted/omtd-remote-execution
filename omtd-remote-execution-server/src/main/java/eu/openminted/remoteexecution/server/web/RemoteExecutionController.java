package eu.openminted.remoteexecution.server.web;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.http.HttpServletRequest;

import org.apache.uima.UIMAFramework;
import org.apache.uima.internal.util.UIMAClassLoader;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import eu.openminted.remoteexecution.server.ApplicationConfiguration;
import eu.openminted.remoteexecution.server.dto.BatchResponse;
import eu.openminted.remoteexecution.server.dto.ProcessResponse;
import eu.openminted.remoteexecution.server.processor.Processor;
import eu.openminted.remoteexecution.server.processor.ProcessorOutput;
import eu.openminted.remoteexecution.server.processor.uima.UimaPipeline;
import eu.openminted.remoteexecution.server.processor.uima.UimaPipelineProcessor;
import eu.openminted.remoteexecution.server.service.ExecutionService;
import eu.openminted.remoteexecution.server.service.model.Batch;
import eu.openminted.remoteexecution.server.service.model.Execution;

@RestController
public class RemoteExecutionController {


	private ExecutionService executionService;

	@Autowired
	public RemoteExecutionController(ApplicationConfiguration config)
			throws InstantiationException, IllegalAccessException {
		// UimaPipeline pipeline = new TestPipeline();
		// UimaPipeline pipeline = new ChebiPipeline();
		// this.executionService = new ExecutionService(new
		// UimaPipelineProcessor(pipeline));

		// ThreadPoolExecutor
		

		String processorClassName = config.getProcessorClass();
		Class<?> processorClass;
		try {
			processorClass = Class.forName(processorClassName);
		} catch (ClassNotFoundException e) {
		   throw new RuntimeException("Processor class '" + processorClassName + "' cannot be found");
		}
		
		if ( UimaPipeline.class.isAssignableFrom(processorClass)) {
			UimaPipeline pipeline = (UimaPipeline) processorClass.newInstance();
			executionService = new ExecutionService(new UimaPipelineProcessor(pipeline));
		} else {
			throw new RuntimeException("Processor class '" + processorClassName + "' does not implement a known component/pipeline interface");		
		}
	}

	// curl -F "cas=@test.txt.xmi" -F "typeSystem=@TypeSystem.xml"
	// http://localhost/foo/process
	// https://github.com/galanisd/omtd-simple-workflows/blob/master/omtd-simple-workflows.dockerfile
	// https://github.com/galanisd/omtd-simple-workflows/blob/master/omtd-simple-workflows-createDockerImg.sh
	// https://github.com/galanisd/omtd-simple-workflows/
	// http://snf-754063.vm.okeanos.grnet.gr
	// https://github.com/galanisd/omtd-simple-workflows/blob/master/omtd-simple-workflows-dkpro/src/main/java/eu/openminted/simplewokflows/dkpro/PipelinePDFToXMI.java

	// TODO supports merging?
	// TODO delta cas xmi
	// TODO lifecycle of results
	// TODO concurrent jobs
	// TODO support multiple services
	// TODO support long polling?
	// TODO improve error handling

	@PostMapping("/batch")
	BatchResponse createBatch(@RequestPart("file") MultipartFile typeSystemFile)
			throws InvalidXMLException, IOException {
		TypeSystemDescription typeSystemDescription = readTypeSystem(typeSystemFile);
		Batch batch = executionService.createBatch(typeSystemDescription);
		return new BatchResponse(String.valueOf(batch.id));
	}

	@DeleteMapping("/batch/{id}")
	void deleteBatch(@PathVariable String id) {
		executionService.deleteBatch(UUID.fromString(id));
	}

	@PostMapping("/process")
	// https://stackoverflow.com/questions/37870989/spring-how-to-stream-large-multipart-file-uploads-to-database-without-storing
	ProcessResponse process(@RequestPart("cas") MultipartFile casFile,
			@RequestPart("typeSystem") Optional<MultipartFile> typeSystemFile, @RequestParam Optional<String> batchId,
			@RequestParam Optional<String> name) throws IOException, InvalidXMLException, NoSuchAlgorithmException {

		
		
		
		// TODO handle incoming type systems
		final Execution execution;
		if (batchId.isPresent()) {
			UUID uuid = UUID.fromString(batchId.get());
			execution = executionService.process(casFile.getInputStream(), uuid);
		} else {
			final Optional<TypeSystemDescription> typeSystem;
			if (typeSystemFile.isPresent()) {
				typeSystem = Optional.of(readTypeSystem(typeSystemFile.get()));
			} else {
				typeSystem = Optional.empty();
			}
			execution = executionService.process(casFile.getInputStream(), typeSystem);
		}

		String url = urlBuilder().path("/process/" + execution.id).toUriString();
		return new ProcessResponse(url, ProcessResponse.STATUS_RUNNING);

	}

	// Remember to set X-Forwarded-Prefix in Apache
	// <Location "/foo">
	// RequestHeader set X-Forwarded-Prefix "foo"
	// ProxyPass "http://localhost:8080"
	// ProxyPassReverse "http://localhost:8080"
	// </Location>
	private ServletUriComponentsBuilder urlBuilder() {
		return ServletUriComponentsBuilder.fromCurrentContextPath();
	}

	// TODO maybe return zip file instead?
	@GetMapping("/process/{id}")
	ResponseEntity<ProcessResponse> getProcessStatus(HttpServletRequest request, @PathVariable String id)
			throws InterruptedException, ExecutionException {

		UUID uuid = UUID.fromString(id);

		if (!executionService.doesProcessExist(uuid)) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}

		ProcessorOutput result = executionService.getResult(uuid);
		String url = urlBuilder().path("/process/" + id).toUriString();

		ProcessResponse response;
		if (result == null) {
			response = new ProcessResponse(url, ProcessResponse.STATUS_RUNNING);
		} else {
			String casUrl = urlBuilder().path("/cas/" + id).toUriString();
			String typeSystemUrl = urlBuilder().path("/typeSystem/" + id).toUriString();
			String deletionUrl = urlBuilder().path("/process/" + id).toUriString();
			response = new ProcessResponse(url, casUrl, typeSystemUrl, deletionUrl, ProcessResponse.STATUS_FINISHED);
		}

		return new ResponseEntity<ProcessResponse>(response, HttpStatus.OK);
	}

	@DeleteMapping("/process/{id}")
	void deleteProcess(@PathVariable String id) {
		executionService.deleteProcess(UUID.fromString(id));
	}

	@GetMapping(value = "/cas/{id}", produces = MimeTypeUtils.APPLICATION_XML_VALUE)
	Resource getProcessedCas(@PathVariable String id) throws InterruptedException, ExecutionException {
		return new ByteArrayResource(executionService.getResult(UUID.fromString(id)).cas);
	}

	@GetMapping(value = "/typeSystem/{id}", produces = MimeTypeUtils.APPLICATION_XML_VALUE)
	Resource getProcessedTypeSystem(@PathVariable String id) throws InterruptedException, ExecutionException {
		return new ByteArrayResource(executionService.getResult(UUID.fromString(id)).typeSystemDescription);
	}

	// TODO list services
	// TODO metadata - input/output types?
	@GetMapping("/info")
	String info(HttpServletRequest req) {
		return "OK";
	}

	private TypeSystemDescription readTypeSystem(MultipartFile file) throws InvalidXMLException, IOException {
		XMLParser parser = UIMAFramework.getXMLParser();
		TypeSystemDescription tsd = parser.parseTypeSystemDescription(new XMLInputSource(file.getInputStream(), null));
		// System.out.println(tsd.getTypes()[0].getName());
		return tsd;
	}

}
