
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
        composer.run(sample, name + "_semiparallel_");
    }

    static void ParallelScaledNarrow(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
            true,
            1);
            

        composer.run(sample, name + "_width_" + 1 + "_scaled_");
    }

    static void ParallelUnScaledNarrow(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, max_pool_size, max_pool_size),
            true,
            1);
            

        composer.run(sample, name + "_width_" + 1 + "_unscaled_");
    }

    static void ParallelScaledWide(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
            true,
            Integer.MAX_VALUE);
            

        composer.run(sample, name + "_width_" + Integer.MAX_VALUE + "_scaled_");
    }

    static void ParallelUnScaledWide(AsyncCollectionReader sample, DUUIComposer composer, String name, int cas_pool_size, int max_pool_size) throws Exception {
        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, max_pool_size, max_pool_size),
            true,
            Integer.MAX_VALUE);
            

        composer.run(sample, name + "_width_" + Integer.MAX_VALUE + "_scaled_");
    }

    @Test
    static
    void TestInitializer() throws Exception {
        final String root = "duui_parallel_benchmarks/";
        final String samples = root + "samples/";
        final int sample_size = 40;
        final String sample_variant = "smallest"; // "largest" "random" 
        final int sample_skip_size = 1*1024*1024;
        final String gerparcor_sample = samples + "gerparcor_" + sample_variant + "_" + sample_size + "_" + formatb(sample_skip_size);
        final int cas_pool_size = 40; // 100 
        final int max_pool_size = 6;
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
            new DUUIDockerDriver.Component("tokenizer:latest") //.withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("sentencizer:latest") //.withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("parser:latest") //.withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("ner:latest") //.withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("lemmatizer:latest") //.withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("morphologizer:latest") //.withScale(5)
                .withImageFetching(),
            new DUUIDockerDriver.Component("tagger:latest") //.withScale(5)
                .withImageFetching()
        );

        composer
        .withParallelPipeline(
            new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
            true,
            1);
           
        composer.run(sample, run_key + "_width_" + 1 + "_scaled_");
        // FINISHED ANALYSIS: 635 s
        // URL WAIT 25s 738ms
        // SERIALIZE WAIT 13s 493ms
        // ANNOTATOR WAIT 22min 3s 915ms
        // DESERIALIZE WAIT 8min 37s 239ms
        // SCALING WAIT 0ns
        // AFTER WORKER WAIT 863ms
        // READ WAIT 25s 263ms
        // RESOURCE MANAGER TOTAL 7min 7s 741ms

        // SemiParallel(sample, composer, run_key, cas_pool_size, max_pool_size);
        
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
