package com.synload.nucleo.examples;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class SessionData {
    String name;
    UUID id;
    Set<Character> flags = new HashSet<>();

    public SessionData(String name, UUID id, Character... flags) {
        this.name = name;
        this.id = id;
        this.flags.addAll(Arrays.asList(flags));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Set<Character> getFlags() {
        return flags;
    }

    public void setFlags(Set<Character> flags) {
        this.flags = flags;
    }
}
