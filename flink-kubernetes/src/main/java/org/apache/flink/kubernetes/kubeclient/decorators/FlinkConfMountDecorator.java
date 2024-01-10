/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import com.qlangtech.tis.config.BasicConfig;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_MAP_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mounts the log4j.properties, logback.xml, and flink-conf.yaml configuration on the JobManager or
 * TaskManager pod.
 * baisui modfiy 2024/01/10
 */
public class FlinkConfMountDecorator extends AbstractKubernetesStepDecorator {

    // public static final String TIS_CONF_VOLUME = "tis-config-volume";

    private final AbstractKubernetesParameters kubernetesComponentConf;

    public FlinkConfMountDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Pod mountedPod = decoratePod(flinkPod.getPodWithoutMainContainer());
        EnvVar tisConfigPathEvn = new EnvVar();
        tisConfigPathEvn.setName(BasicConfig.KEY_ENV_TIS_CFG_BUNDLE_PATH);
        tisConfigPathEvn.setValue("conf/" + BasicConfig.KEY_DEFAULT_TIS_CFG_BUNDLE_PATH);
        final Container mountedMainContainer =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addNewVolumeMount()
                        .withName(FLINK_CONF_VOLUME)
                        .withMountPath(kubernetesComponentConf.getFlinkConfDirInPod())
                        .endVolumeMount()
                        .addToEnv(tisConfigPathEvn)
                        // baisui add
//                        .addNewVolumeMount()
//                        .withName(TIS_CONF_VOLUME)
//                        .withMountPath("/opt/flink")
//                        .endVolumeMount()
                        .build();

        return new FlinkPod.Builder(flinkPod)
                .withPod(mountedPod)
                .withMainContainer(mountedMainContainer)
                .build();
    }

    private Pod decoratePod(Pod pod) {

        // baisui add 2021/11/5 for inject configMap from client
        final List<KeyToPath> keyToPaths = (flinkConfigMapData == null)
                ? getLocalLogConfFiles().stream()
                .map(file -> new KeyToPathBuilder()
                        .withKey(file.getName())
                        .withPath(file.getName())
                        .build())
                .collect(Collectors.toList())
                : flinkConfigMapData.entrySet().stream().map((entry) -> new KeyToPathBuilder()
                .withKey(entry.getKey())
                .withPath(entry.getValue().getPodPath())
                .build()).collect(Collectors.toList());


        keyToPaths.add(
                new KeyToPathBuilder()
                        .withKey(FLINK_CONF_FILENAME)
                        .withPath(FLINK_CONF_FILENAME)
                        .build());

        final Volume flinkConfVolume =
                new VolumeBuilder()
                        .withName(FLINK_CONF_VOLUME)
                        .withNewConfigMap()
                        .withName(getFlinkConfConfigMapName(kubernetesComponentConf.getClusterId()))
                        .withItems(keyToPaths)
                        .endConfigMap()
                        .build();

//        if (MapUtils.isEmpty(tisConfigMapData)) {
//            throw new IllegalStateException("tisConfigMapData can not be null");
//        }

//        final Volume tisConfVolume =
//                new VolumeBuilder()
//                        .withName(TIS_CONF_VOLUME)
//                        .withNewConfigMap()
//                        .withName(getTISConfConfigMapName(kubernetesComponentConf.getClusterId()))
//                        .withItems(tisConfigMapData
//                                .entrySet()
//                                .stream()
//                                .map((entry) -> new KeyToPathBuilder()
//                                        .withKey(entry.getKey())
//                                        .withPath(entry.getValue().getPodPath())
//                                        .build())
//                                .collect(Collectors.toList()))
//                        .endConfigMap()
//                        .build();

        return new PodBuilder(pod)
                .editSpec()
                .addNewVolumeLike(flinkConfVolume)
                .endVolume()
                //  .addNewVolumeLike(tisConfVolume).endVolume()
                .endSpec()
                .build();
    }

    // baisui add 2021/11/5 for inject configMap from client
    public static Map<String, ConfigMapData> flinkConfigMapData;
    //   public static Map<String, ConfigMapData> tisConfigMapData;

    // baisui add 2024/01/10 for inject conf tis-web-config/config.properties
    public static class ConfigMapData {
        private String podPath;
        private final String content;

        public ConfigMapData(String podPath, String content) {
            this.podPath = podPath;
            this.content = content;
        }

        public String getPodPath() {
            return podPath;
        }

        public ConfigMapData setPodPath(String podPath) {
            this.podPath = podPath;
            return this;
        }

        public String getContent() {
            return content;
        }
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        final String clusterId = kubernetesComponentConf.getClusterId();

        final Map<String, String> data = new HashMap<>();
        // baisui add 2021/11/5 for inject configMap from client
        if (flinkConfigMapData != null) {
            data.putAll(flinkConfigMapData
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue().content)));
        } else {
            final List<File> localLogFiles = getLocalLogConfFiles();
            for (File file : localLogFiles) {
                data.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
            }
        }


        final Map<String, String> propertiesMap =
                getClusterSidePropertiesMap(kubernetesComponentConf.getFlinkConfiguration());
        data.put(FLINK_CONF_FILENAME, getFlinkConfData(propertiesMap));

        final ConfigMap flinkConfConfigMap =
                new ConfigMapBuilder()
                        .withApiVersion(Constants.API_VERSION)
                        .withNewMetadata()
                        .withName(getFlinkConfConfigMapName(clusterId))
                        .withLabels(kubernetesComponentConf.getCommonLabels())
                        .endMetadata()
                        .addToData(data)
                        .build();

//        if (MapUtils.isEmpty(tisConfigMapData)) {
//            throw new IllegalStateException("tisConfigMapData can not be empty");
//        }

//        final ConfigMap tisConfConfigMap =
//                new ConfigMapBuilder()
//                        .withApiVersion(Constants.API_VERSION)
//                        .withNewMetadata()
//                        .withName(getTISConfConfigMapName(clusterId))
//                        .withLabels(kubernetesComponentConf.getCommonLabels())
//                        .endMetadata()
//                        .addToData(tisConfigMapData
//                                .entrySet()
//                                .stream()
//                                .collect(Collectors.toMap(
//                                        (e) -> e.getKey(),
//                                        (e) -> e.getValue().content)))
//                        .build();
        return Lists.newArrayList(flinkConfConfigMap);

        //   return Collections.singletonList(flinkConfConfigMap);
    }

    /** Get properties map for the cluster-side after removal of some keys. */
    private Map<String, String> getClusterSidePropertiesMap(Configuration flinkConfig) {
        final Configuration clusterSideConfig = flinkConfig.clone();
        // Remove some configuration options that should not be taken to cluster side.
        clusterSideConfig.removeConfig(KubernetesConfigOptions.KUBE_CONFIG_FILE);
        clusterSideConfig.removeConfig(DeploymentOptionsInternal.CONF_DIR);
        return clusterSideConfig.toMap();
    }

    @VisibleForTesting
    String getFlinkConfData(Map<String, String> propertiesMap) throws IOException {
        try (StringWriter sw = new StringWriter();
             PrintWriter out = new PrintWriter(sw)) {
            propertiesMap.forEach(
                    (k, v) -> {
                        out.print(k);
                        out.print(": ");
                        out.println(v);
                    });

            return sw.toString();
        }
    }

    private List<File> getLocalLogConfFiles() {
        final String confDir = kubernetesComponentConf.getConfigDirectory();
        final File logbackFile = new File(confDir, CONFIG_FILE_LOGBACK_NAME);
        final File log4jFile = new File(confDir, CONFIG_FILE_LOG4J_NAME);

        List<File> localLogConfFiles = new ArrayList<>();
        if (logbackFile.exists()) {
            localLogConfFiles.add(logbackFile);
        }
        if (log4jFile.exists()) {
            localLogConfFiles.add(log4jFile);
        }

        return localLogConfFiles;
    }

    @VisibleForTesting
    public static String getFlinkConfConfigMapName(String clusterId) {
        return CONFIG_MAP_PREFIX + clusterId;
    }

//    public static String getTISConfConfigMapName(String clusterId) {
//        return "tis-" + getFlinkConfConfigMapName(clusterId);
//    }
}
