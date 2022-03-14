public class DUUIPipelineComponent {
    private String _target_name;
    private boolean _is_remote;
    private boolean _is_local;

    DUUIPipelineComponent(String target, boolean remote, boolean local) {
        _target_name = target;
        _is_remote = remote;
        _is_local = local;
    }

    public String getTarget() {
        return _target_name;
    }

    public boolean getRemote() {
        return _is_remote;
    }

    public boolean getLocal() {
        return _is_local;
    }
}
