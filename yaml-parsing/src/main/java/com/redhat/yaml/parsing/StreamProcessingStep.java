
package com.redhat.yaml.parsing;

/**
 *
 * @author hhiden
 */
public class StreamProcessingStep {
    private String name;
    private String label;
    private String[] inputStreams;
    private String[] outputStreams;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String[] getInputStreams() {
        return inputStreams;
    }

    public void setInputStreams(String[] inputStreams) {
        this.inputStreams = inputStreams;
    }

    public String[] getOutputStreams() {
        return outputStreams;
    }

    public void setOutputStreams(String[] outputStreams) {
        this.outputStreams = outputStreams;
    }
    
    
}
