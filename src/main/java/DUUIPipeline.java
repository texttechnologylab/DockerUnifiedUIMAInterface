import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class DUUIPipeline {
    private Vector<DUUIPipelineComponent> _images;

    public DUUIPipeline() {
        _images = new Vector<DUUIPipelineComponent>();
    }

    public DUUIPipeline add(String image) {
        _images.add(new DUUIPipelineComponent(image,false,false));
        return this;
    }

    public DUUIPipeline addLocal(String image) {
        _images.add(new DUUIPipelineComponent(image,false,true));
        return this;
    }

    public DUUIPipeline addRemote(String image) {
        _images.add(new DUUIPipelineComponent(image,true,false));
        return this;
    }

    public List<String> getImages() {
        return _images.stream().map((DUUIPipelineComponent e) -> e.getTarget()).collect(Collectors.toList());
    }
}
