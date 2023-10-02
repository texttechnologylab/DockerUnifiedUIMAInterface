
import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatb;

import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy.AdaptiveStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy.FixedStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

public class TestDUUIParallel {

    static void SemiParallel(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer.withSemiParallelPipeline(cas_pool_size, max_pool_size);
        composer.run(sample, name + "_semiparallel");
    }

    static void ParallelScaledNarrow(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
            true,
            1);
            

        composer.run(sample, name + "_width_" + 1 + "_scaled");
    }

    static void ParallelUnScaledNarrow(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, max_pool_size, max_pool_size),
            true,
            1);
            

        composer.run(sample, name + "_width_" + 1 + "_unscaled");
    }

    static void ParallelScaledWide(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
            true,
            4);
            

        composer.run(sample, name + "_width_" + 4 + "_scaled");
    }

    static void ParallelUnScaledWide(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, max_pool_size, max_pool_size),
            true,
            4);
            

        composer.run(sample, name + "_width_" + 4 + "_scaled");
    }

    @Test
    static
    void TestInitializer() throws Exception {
        final String root = "duui_parallel_benchmarks/";
        final String samples = root + "samples/";
        final int sample_size = 100;
        final String sample_variant = "smallest"; // "largest" "random" "smallest"
        final int sample_skip_size = 10*1024*1024;
        final String gerparcor_sample = samples + "gerparcor_" + sample_variant + "_" + sample_size + "_" + formatb(sample_skip_size);
        final int cas_pool_size = 30; // 100 
        final int max_pool_size = 10;
        final String run_key = gerparcor_sample.replace(samples, "") + "_pool_" + max_pool_size + "_cas_" + cas_pool_size;

        AsyncCollectionReader sample = new AsyncCollectionReader(
            "C:\\Users\\davet\\projects\\gerparcor", 
            ".xmi.gz",
            1,
            sample_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.SMALLEST,
            gerparcor_sample + ".txt",
            false,
            "de",
            sample_skip_size);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(gerparcor_sample + ".db")
            .withConnectionPoolSize(max_pool_size);

        DUUIComposer composer = new DUUIComposer()
            .withLuaContext(new DUUILuaContext().withJsonLibrary())    
            .withSkipVerification(true)
            .withStorageBackend(sqlite)
            .withCasPoolMemoryThreshhold(1024*1024*1024); 

        composer.addDriver(new DUUIDockerDriver().withContainerPause());

        composer.add(  
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-tokenizer:1.0").withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-sentencizer:1.0").withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-parser:1.0").withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-ner:1.0").withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-lemmatizer:1.0").withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-morphologizer:1.0").withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-tagger:1.0").withScale(5)
                .withImageFetching()
        );

        ParallelScaledNarrow(sample, composer, run_key, cas_pool_size, max_pool_size);

        // FINISHED ANALYSIS: 3567 s scaled, 6, sync
        // URL WAIT 14min 46s 594ms
        // SERIALIZE WAIT 2min 37s 558ms
        // ANNOTATOR WAIT 227min 47s 226ms
        // DESERIALIZE WAIT 98min 17s 274ms
        // SCALING WAIT 0ns
        // AFTER WORKER WAIT 2s 403ms
        // READ WAIT 58min 58s 955ms
        // RESOURCE MANAGER TOTAL 36min 11s 24ms

        // SemiParallel(sample, composer, run_key, cas_pool_size, max_pool_size);

        // FINISHED ANALYSIS: 4127 s
        // URL WAIT 37s 627ms
        // SERIALIZE WAIT 2min 31s 413ms
        // ANNOTATOR WAIT 308min 18s 809ms
        // DESERIALIZE WAIT 99min 12s 352ms
        // SCALING WAIT 0ns
        // AFTER WORKER WAIT 10ms
        // READ WAIT 67min 7s 260ms
        // RESOURCE MANAGER TOTAL 44min 23s 146ms
        
        // ParallelScaledNarrow(sample, composer, run_key, cas_pool_size, max_pool_size);
        // ParallelScaledWide(sample, composer, run_key, cas_pool_size, max_pool_size);
        // ParallelUnScaledNarrow(sample, composer, run_key, cas_pool_size, max_pool_size);
        // ParallelUnScaledWide(sample, composer, run_key, cas_pool_size, max_pool_size);
        
        composer.shutdown();
    }

    public static void main(String[] args) throws Exception {
        TestInitializer();
    }

}
