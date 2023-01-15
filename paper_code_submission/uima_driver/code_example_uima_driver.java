
public class PaperExampleUIMA {
    public static void main(String[] args) throws Exception {
        // A new CAS document is defined.
        // load content into jc ...
        // Defining LUA-Context for communication
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("performance_tracker.db")
                .withConnectionPoolSize(1);

        DUUILuaContext ctx = LuaConsts.getJSON();
        // The composer is defined and initialized with a standard Lua context.
        DUUIComposer composer = new DUUIComposer().withLuaContext(ctx)
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withWorkers(2);

        // Please note in the following only the UIMADriver is used since any statement of a docker container registry or
        // a url can be traced back with ease to us. Therefore, in order to comply with the requirements only local containers
        // will be used. Please find other examples attached with a whole test container using state of the art containers
        // from huggingface implemented in the programming language rust.

        // Instantiate drivers with options
        DUUIDockerDriver docker_driver = new DUUIDockerDriver()
                .withTimeout(10000);
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver(10000);
        // Definition of the UIMA driver with the option of debugging output in the log.
        DUUIUIMADriver uima_driver = new DUUIUIMADriver().withDebug(true);
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        // A driver must be added before components can be added for it in the composer.
        composer.addDriver(docker_driver, remote_driver, uima_driver,
                swarm_driver);
        // Now the composer is able to use the individual drivers.
        // A new component for the composer is added
        composer.add(new DUUIUimaDriver
                // The component is based on a Docker image stored in a remote repository.
                .Component(SpacyMultiTagger.class)
                // The image is reloaded and fetched, regardless of whether it already exists locally (optional)
                .withImageFetching()
                // The scaling parameter is set
                .withScale(1));

        // Adding a UIMA annotator for writing the result of the pipeline as XMI files in compressed form.
        composer.add(new DUUIUIMADriver.Component(
                createEngineDescription(XmiWriter.class,
                        XmiWriter.PARAM_TARGET_LOCATION, "output_location",
                        XmiWriter.PARAM_COMPRESSION, "GZIP"
                )).withScale(1));
        // The document is processed through the pipeline.
        composer.run(CollectionReaderFactory.createReaderDescription(XmiReader.class,
                XmiReader.PARAM_LANGUAGE, "de",
                XmiReader.PARAM_ADD_DOCUMENT_METADATA, false,
                XmiReader.PARAM_OVERRIDE_DOCUMENT_METADATA, false,
                XmiReader.PARAM_LENIENT, true,
                XmiReader.PARAM_SOURCE_LOCATION, "/input_location/*.xmi"), "run_python_token_annotator");
    }
}