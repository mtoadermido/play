/*
 * Copyright 2012 Midokura Europe SARL
 */
package com.midokura.tests.zookeeper;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import sun.security.util.Resources_sv;
import static com.google.common.collect.Iterables.transform;

/**
 * // TODO: Explain yourself
 *
 * @author Mihai Claudiu Toader <mtoader@midokura.com>
 *         Date: 3/13/12
 */
public class ZookeeperLauncher {

    final String INSTALL_DIR = "/usr/share/zookeeper";

    final String JAVA_OPTS = "";

    final String ZOO_KEEPER_ROOT_LOGGER = "DEBUG,ROLLINGFILE";

    private final static org.slf4j.Logger log =
        org.slf4j.LoggerFactory.getLogger(ZookeeperLauncher.class);

    private Process zooKeeperProcess;

    public static ZookeeperLauncher launch() throws IOException {
        try {
            new ProcessBuilder("rm", "-rf", "target/zookeeper/data").start().waitFor();
        } catch (InterruptedException e) {
            log.error("While removing the old data", e);
        }
        return new ZookeeperLauncher().start();
    }

    private ZookeeperLauncher start() throws IOException {

        String zooKeeperLogDir = ".";
        String zooKeeperConfig = "src/main/resources/zookeeper.cfg";

        String command =
            String.format("java -cp %s %s " +
                              "-Dzookeeper.log.dir=%s " +
                              "-Dzookeeper.root.logger=%s " +
                              "org.apache.zookeeper.server.quorum.QuorumPeerMain " +
                              "%s",
                          buildClassPath(),
                          JAVA_OPTS,
                          zooKeeperLogDir,
                          ZOO_KEEPER_ROOT_LOGGER,
                          zooKeeperConfig);

        log.debug("Command:\n\t{}", command);

        zooKeeperProcess =
            new ProcessBuilder()
                .command(Lists.newArrayList(
                    Splitter.onPattern("[ \t]+").split(command)))
                .redirectErrorStream(true)
                .start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    InputSupplier<InputStreamReader> threadInputSupplier = new InputSupplier<InputStreamReader>() {
                        @Override
                        public InputStreamReader getInput()
                            throws IOException {
                            return new InputStreamReader(
                                zooKeeperProcess.getInputStream());
                        }
                    };

                    LineProcessor<Object> logDumper = new LineProcessor<Object>() {
                        @Override
                        public boolean processLine(String line)
                            throws IOException {
                            log.debug("<zookeeper>: {}", line);
                            return true;
                        }

                        @Override
                        public Object getResult() {
                            return null;
                        }
                    };


                    CharStreams.readLines(threadInputSupplier, logDumper);
                } catch (IOException e) {
                    log.error("Exception while reading zookeeper stdout.", e);
                }
            }
        }).start();

        return this;

    }

    private String buildClassPath() {
        List<File> entries = new ArrayList<File>();

        entries.add(new File("/etc/zookeeper"));

        entries.addAll(
            Arrays.asList(new File(INSTALL_DIR).listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return
                        file.isFile() && file.getName().endsWith(".jar");
                }
            })));

        return
            Joiner
                .on(':')
                .join(transform(entries,
                                new Function<File, String>() {
                                    @Override
                                    public String apply(
                                        @Nullable File input) {
                                        if (input == null)
                                            return null;

                                        try {
                                            return input.getCanonicalPath();
                                        } catch (IOException e) {
                                            return input.getAbsolutePath();
                                        }
                                    }
                                }));
    }

    public void destroy() throws InterruptedException {
        log.debug("Sent destroy");
        zooKeeperProcess.destroy();
        log.debug("Waiting for exit");
        zooKeeperProcess.waitFor();
    }
}
