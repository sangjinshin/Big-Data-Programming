package com.refactorlabs.cs378.sessions;

/**
 * Enumeration for different session types.
 */
public enum SessionType {
    SUBMITTER("submitter"),
    CLICKER("clicker"),
    SHOWER("shower"),
    VISITOR("visitor"),
    OTHER("other");

    private String text;

    private SessionType(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
