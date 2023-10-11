package org.texttechnologylab.DockerUnifiedUIMAInterface;

import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.dkpro.core.io.xmi.XmiReader;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestDoNotTrackErrorDocs {
    @Test
    void testDoNotTrackErrorDocs() throws Exception {
        int workers = 1;
        boolean trackErrorDocs = true;

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("test_do_not_track_errors.db", trackErrorDocs)
                .withConnectionPoolSize(workers);

        DUUIComposer composer = new DUUIComposer()
                .withWorkers(workers)
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withStorageBackend(sqlite);

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        composer.addDriver(uimaDriver);

        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        composer.addDriver(remoteDriver);

        CollectionReaderDescription reader = CollectionReaderFactory.createReaderDescription(XmiReader.class
                , XmiReader.PARAM_LANGUAGE, "de"
                , XmiReader.PARAM_LENIENT, true
                , XmiReader.PARAM_SOURCE_LOCATION, "test_corpora_xmi/*.xmi"
        );

        /*DUUIUIMADriver.Component component = new DUUIUIMADriver.Component(createEngineDescription(DoNotTrackErrorDocsComponent.class
        ));
        composer.add(component);*/

        DUUIRemoteDriver.Component component = new DUUIRemoteDriver.Component("http://127.0.0.1:9714");
        composer.add(component);

        composer.run(reader, "test_do_not_track_errors");

        composer.shutdown();
    }
}
