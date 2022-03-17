/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.loader;

import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.loader.constant.Constants;
import com.baidu.hugegraph.loader.exception.LoadException;
import com.baidu.hugegraph.loader.executor.GroovyExecutor;
import com.baidu.hugegraph.loader.executor.LoadContext;
import com.baidu.hugegraph.loader.executor.LoadOptions;
import com.baidu.hugegraph.loader.mapping.LoadMapping;
import com.baidu.hugegraph.loader.task.TaskManager;
import com.baidu.hugegraph.loader.util.HugeClientHolder;
import com.baidu.hugegraph.loader.util.LoadUtil;
import com.baidu.hugegraph.loader.util.Printer;
import com.baidu.hugegraph.util.Log;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

public final class LoaderSchema {

    public static final Logger LOG = Log.logger(LoaderSchema.class);

    private final LoadContext context;
    private final LoadMapping mapping;
    private final TaskManager manager;

    public static void main(String[] args) {
        LoaderSchema loader;
        try {
            loader = new LoaderSchema(args);
        } catch (Throwable e) {
            Printer.printError("Failed to start loading base...", e);
            return;
        }
        loader.loadSchema();
    }

    public LoaderSchema(String[] args) {
        this(LoadOptions.parseOptions(args,"schema"));
    }

    public LoaderSchema(LoadOptions options) {
        this(options, null);
    }

    public LoaderSchema(LoadOptions options, LoadMapping mapping) {
        this.context = new LoadContext(options);
        this.mapping = mapping;
        this.manager = new TaskManager(this.context);
        this.addShutdownHook();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook was triggered");
            this.stopThenShutdown();
        }));
    }

    public LoadContext context() {
        return this.context;
    }

    public boolean loadSchema() {
        try {
            // Switch to loading mode
            this.context.setLoadingMode();
            // Clear schema if needed
            this.clearAllDataIfNeeded();
            // Create schema
            this.createSchema();
//            this.loadInputs();
            // Print load summary
//            Printer.printSummary(this.context);
        } catch (Throwable t) {
            RuntimeException e = LoadUtil.targetRuntimeException(t);
            Printer.printError("Failed to load chema", e);
            if (this.context.options().testMode) {
                throw e;
            }
        } finally {
            this.stopThenShutdown();
        }
        return this.context.noError();
    }

    private void clearAllDataIfNeeded() {
        LoadOptions options = this.context.options();
        if (!options.clearAllData) {
            return;
        }

        int requestTimeout = options.timeout;
        options.timeout = options.clearTimeout;
        HugeClient client = HugeClientHolder.create(options);
        String message = "I'm sure to delete all data";

        LOG.info("Prepare to clear the data of graph '{}'", options.graph);
        client.graphs().clearGraph(options.graph, message);
        LOG.info("The graph '{}' has been cleared successfully", options.graph);

        options.timeout = requestTimeout;
        client.close();
    }

    private void createSchema() {
        LoadOptions options = this.context.options();
        if (!StringUtils.isEmpty(options.schema)) {
            File file = FileUtils.getFile(options.schema);
            HugeClient client = this.context.client();
            GroovyExecutor groovyExecutor = new GroovyExecutor();
            groovyExecutor.bind(Constants.GROOVY_SCHEMA, client.schema());
            String script;
            try {
                script = FileUtils.readFileToString(file, Constants.CHARSET);
            } catch (IOException e) {
                throw new LoadException("Failed to read schema file '%s'", e,
                                        options.schema);
            }
            System.out.println("==========script======info=================");
            System.out.println(script);
            groovyExecutor.execute(script, client);
        }
        this.context.updateSchemaCache();
    }
    /**
     * TODO: How to distinguish load task finished normally or abnormally
     */
    private synchronized void stopThenShutdown() {
        if (this.context.closed()) {
            return;
        }
        LOG.info("Stop loading then shutdown LoaderSchema");
        try {
            this.context.stopLoading();
            if (this.manager != null) {
                // Wait all insert tasks stopped before exit
                this.manager.waitFinished();
                this.manager.shutdown();
            }
        } finally {
            try {
                this.context.unsetLoadingMode();
            } finally {
                //loader schema use
                this.context.close("schema");
            }
        }
    }
}
