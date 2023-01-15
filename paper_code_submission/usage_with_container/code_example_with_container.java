/**
 * This file contains the code to use an container built in a different language from java. It incorporates the results
 * derived from a huggingface transformer into the UIMA Cas Document while enabling easy scaling and reproducible results.
 *
 * There are other examples which provide other use cases.
 *  **uima_driver**             -   This example derives an analysis engine from multiple java internal analysis engines to enable the
 *                                  usage of the most common usage.
 *  **usage_with_container**    -   This example shows the usage of the UI
 */
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
        composer.add(new DUUIDockerDriver
                // Specify the name which was selected for the docker container built from the test container directory!
                .Component("rust-container-name:0.1")
                // The scaling parameter is set to determine how many replicas will be spawned
                // Please note that the hugginface container takes a lot of memory so scale this down according to your
                // own computer memory!
                .withScale(2));

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