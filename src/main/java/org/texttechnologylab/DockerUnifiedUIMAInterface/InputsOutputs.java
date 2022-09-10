package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.util.List;

public class InputsOutputs {
    private final List<String> inputs;
    private final List<String> outputs;

    public InputsOutputs(List<String> inputs, List<String> outputs) {
        this.inputs = inputs;
        this.outputs = outputs;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public List<String> getOutputs() {
        return outputs;
    }

    @Override
    public String toString() {
        return "InputsOutputs{" +
                "inputs=" + inputs +
                ", outputs=" + outputs +
                '}';
    }
}
