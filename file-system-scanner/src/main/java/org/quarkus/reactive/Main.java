package org.quarkus.reactive;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.vertx.mutiny.core.Vertx;

import javax.inject.Inject;
import java.nio.file.*;

@QuarkusMain(name = "Main")
public class Main  implements QuarkusApplication{

    @Inject VerticleLauncher launcher;
    @Inject Vertx vertx;

    public static void main(String[] args) {
        Quarkus.run(Main.class,args);
    }


    @Override
    public int run(String... args) throws Exception {
        if(args.length > 0) {
            Path dirPath = null;
            try {
             dirPath = Paths.get(args[0]);
            } catch (InvalidPathException e) {
                System.err.println("Invalid Directory Path");
                return -1;
            }
            if(!Files.isDirectory(dirPath, LinkOption.NOFOLLOW_LINKS)) {
                System.err.println("The target path is not a directory");
                return -1;
            }
            launcher.launch(args[0]);
        } else {
            System.err.println("Missing target directory argument!!");
            return -1;
        }
        Quarkus.waitForExit();
        return 0;
    }
}
