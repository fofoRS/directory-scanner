package org.quarkus.reactive;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain(name = "Main")
public class Main {

    public static void main(String[] args) {
        Quarkus.run(args);
    }
}
