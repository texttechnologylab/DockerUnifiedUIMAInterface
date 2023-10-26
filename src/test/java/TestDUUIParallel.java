
import static org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation.DUUIPipelineVisualizer.formatb;

import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.executors.strategy.AdaptiveStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.executors.strategy.DefaultStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;

public class TestDUUIParallel {

    static void OldParallel(String sample_name, int size, int skip_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode, DUUIComposer composer, String name, int cas_pool_size,
            int max_pool_size) throws Exception {
        AsyncCollectionReader sample = getReader(sample_name, size, mode, sample_name, skip_size);
        composer.runOld(sample, name + "_old", max_pool_size);
        composer.shutdown();
    }

    static void SemiParallel(String sample_name, int size, int skip_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode, DUUIComposer composer, String name, int cas_pool_size,
            int max_pool_size) throws Exception {
        AsyncCollectionReader sample = getReader(sample_name, size, mode, sample_name, skip_size);
        composer.withDocumentParallelPipeline(cas_pool_size, max_pool_size);
        composer.run(sample, name + "_semiparallel");
        composer.shutdown();
    }

    static void ParallelScaledNarrow(String sample_name, int size, int skip_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode, DUUIComposer composer, String name, int cas_pool_size,
            int max_pool_size) throws Exception {
        AsyncCollectionReader sample = getReader(sample_name, size, mode, sample_name, skip_size);
        composer
                .withComponentParallelPipeline(
                        new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
                        true,
                        1);

        composer.run(sample, name + "_width_" + 1 + "_scaled_kill");
        composer.shutdown();
    }

    static void ParallelUnScaledNarrow(String sample_name, int size, int skip_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode, DUUIComposer composer, String name, int cas_pool_size,
            int max_pool_size) throws Exception {
        AsyncCollectionReader sample = getReader(sample_name, size, mode, sample_name, skip_size);
        composer
                .withComponentParallelPipeline(
                        new AdaptiveStrategy(cas_pool_size, max_pool_size, max_pool_size),
                        false,
                        1);

        composer.run(sample, name + "_width_" + 1 + "_unscaled");
        composer.shutdown();
    }

    static void ParallelScaledWide(String sample_name, int size, int skip_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode, DUUIComposer composer, String name, int cas_pool_size,
            int max_pool_size) throws Exception {
        AsyncCollectionReader sample = getReader(sample_name, size, mode, sample_name, skip_size);
        composer
                .withComponentParallelPipeline(
                        new AdaptiveStrategy(cas_pool_size, 1, max_pool_size),
                        true,
                        4);

        composer.run(sample, name + "_width_" + 4 + "_scaled_kill");
        composer.shutdown();
    }

    static void ParallelUnScaledWide(String sample_name, int size, int skip_size,
            DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode, DUUIComposer composer, String name, int cas_pool_size,
            int max_pool_size) throws Exception {
        AsyncCollectionReader sample = getReader(sample_name, size, mode, sample_name, skip_size);
        composer
                .withComponentParallelPipeline(
                        new AdaptiveStrategy(cas_pool_size, max_pool_size, max_pool_size),
                        false,
                        4);

        composer.run(sample, name + "_width_" + 4 + "_unscaled");
        composer.shutdown();
    }

    static AsyncCollectionReader getReader(String path, int sample_size, DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode,
            String name, int skip_size) {
        return new AsyncCollectionReader(
                path,
                ".xmi.gz",
                1,
                sample_size,
                mode,
                name,
                false,
                "de",
                skip_size);
    }

    @Test
    static void TestInitializer(int pool, String variant) throws Exception {
        final String root = "./duui_parallel_benchmarks/";
        final String samples = root + "samples/";
        final int sample_size = 1000;
        final String sample_variant = variant; // "random" "smallest"
        final DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode = sample_variant.equals("smallest")
                ? DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.SMALLEST
                : DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.RANDOM;
        final int sample_skip_size = sample_variant.equals("smallest") ? 
                1 * 1024 * 1024 : 500 * 1024;
        final String gerparcor_sample = samples + "gerparcor_" + sample_variant + "_" + sample_size + "_" + formatb(1 * 1024 * 1024);
        final int cas_pool_size = (int) (pool * 3);
        final int max_pool_size = pool; // 1, 5, 10, 15
        final String run_key = gerparcor_sample.replace(samples, "") + "_pool_" + max_pool_size;

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend(gerparcor_sample + ".db")
                .withConnectionPoolSize(max_pool_size + 1);

        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withSkipVerification(true)
                .withStorageBackend(sqlite);

        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-tokenizer:1.0").withImageFetching());
        if (mode != DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.RANDOM) {
            composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-ner:1.0").withImageFetching());
            composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-lemmatizer:1.0").withImageFetching());
        }
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-tagger:1.0").withImageFetching());
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-sentencizer:1.0").withImageFetching());
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-morphologizer:1.0").withImageFetching());
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/terefe-ba-textimager-spacy-parser:1.0").withImageFetching());

        ParallelScaledNarrow(gerparcor_sample + ".txt",sample_size, sample_skip_size,mode, composer, run_key,sample_variant.equals("smallest") ? 100 : 50, max_pool_size);
        ParallelScaledWide(gerparcor_sample + ".txt", sample_size,sample_skip_size, mode, composer, run_key, sample_variant.equals("smallest") ? 100 : 50,max_pool_size);
        ParallelUnScaledNarrow(gerparcor_sample + ".txt", sample_size,sample_skip_size, mode, composer, run_key, cas_pool_size,max_pool_size);
        ParallelUnScaledWide(gerparcor_sample + ".txt", sample_size,sample_skip_size, mode, composer, run_key, cas_pool_size,max_pool_size);
        SemiParallel(gerparcor_sample + ".txt",sample_size, sample_skip_size,mode, composer, run_key,cas_pool_size, max_pool_size);
        OldParallel(gerparcor_sample + ".txt",sample_size, sample_skip_size,mode, composer, run_key,cas_pool_size, max_pool_size);
    }

    public static void main(String[] args) throws Exception {
    }

}
