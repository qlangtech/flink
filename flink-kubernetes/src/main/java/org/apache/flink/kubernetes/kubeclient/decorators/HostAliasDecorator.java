package org.apache.flink.kubernetes.kubeclient.decorators;

import io.fabric8.kubernetes.api.model.HostAlias;
import io.fabric8.kubernetes.api.model.PodBuilder;

import io.fabric8.kubernetes.api.model.PodFluent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-08-13 10:18
 **/
public class HostAliasDecorator extends AbstractKubernetesStepDecorator {
    public static final String PREFIX = "hostalias.";
    public static final String PREFIX_COUNT = PREFIX + "count";
    public static final String IP_HOST_PREFIX = PREFIX + "%d.";
    public static final String HOST_NAMES_COUNT = "hostnames.count";
    public static final String HOST_NAMES = "hostnames.";
    public static final String HOST_IP = "ip";
    private final AbstractKubernetesParameters kubernetesComponentConf;
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    public static void addHostAliasToFlinkConfig(
            Configuration flinkCfg, Map<String, List<String>> hostAliasList) {
        flinkCfg.setString(PREFIX_COUNT, String.valueOf(hostAliasList.size()));
        int hostAliasIndex = 0;
        for (Map.Entry<String, List<String>> hostAlias : hostAliasList.entrySet()) {
            if (CollectionUtil.isEmptyOrAllElementsNull(hostAlias.getValue())) {
                throw new IllegalStateException(
                        "key:" + hostAlias.getKey() + " relevant val can not be empty");
            }
            final String ipHostPrefix = String.format(IP_HOST_PREFIX, hostAliasIndex++);
            flinkCfg.setString(
                    ipHostPrefix + HOST_NAMES_COUNT,
                    String.valueOf(hostAlias.getValue().size()));
            int hostIdx = 0;
            for (String hostName : hostAlias.getValue()) {
                flinkCfg.setString(ipHostPrefix + HOST_NAMES + hostIdx++, hostName);
            }
            flinkCfg.setString(ipHostPrefix + HOST_IP, hostAlias.getKey());
        }
    }

    public HostAliasDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = kubernetesComponentConf;
    }


    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {

        Configuration flinkCfg = kubernetesComponentConf.getFlinkConfiguration();

        int aliasCount = Integer.parseInt(flinkCfg.getString(PREFIX_COUNT, "0"));
        log.info("host alias count: {}", aliasCount);
        if (aliasCount > 0) {
            PodBuilder podBuilder = flinkPod.getPodWithoutMainContainer().edit();
            PodFluent<PodBuilder>.SpecNested<PodBuilder> podBuilderSpecNested = podBuilder.editOrNewSpec();
            String hostAliasDesc = null;
            for (int i = 0; i < aliasCount; i++) {
                String ipHostPrefix = String.format(IP_HOST_PREFIX, i);
                hostAliasDesc = addHostAlias(ipHostPrefix, flinkCfg, podBuilderSpecNested);
                log.info("add host alias: {}", hostAliasDesc);
            }
            return new FlinkPod.Builder(flinkPod).withPod(podBuilder.build()).build();
        }

        return flinkPod;
    }

    private String addHostAlias(
            String ipHostPrefix,
            Configuration flinkCfg,
            PodFluent<PodBuilder>.SpecNested<PodBuilder> podBuilderSpecNested) {
        // 读取 hostnames
        int hostCount = Integer.parseInt(flinkCfg.getString(ipHostPrefix + HOST_NAMES_COUNT, "0"));
        if (hostCount < 1) {
            throw new IllegalArgumentException("key:" + ipHostPrefix + HOST_NAMES_COUNT
                    + " relevant value can not small than 1");
        }
        List<String> hostnames = new ArrayList<>();
        for (int i = 0; i < hostCount; i++) {
            hostnames.add(flinkCfg.getString(ipHostPrefix + HOST_NAMES + i, ""));
        }
        String ipAdddress = flinkCfg.getString(ipHostPrefix + HOST_IP, "");

        podBuilderSpecNested
                .addToHostAliases(
                        new HostAlias(hostnames, ipAdddress));
        podBuilderSpecNested.endSpec();
        return new StringBuffer()
                .append(ipAdddress)
                .append(":")
                .append(String.join(",", hostnames))
                .toString();
    }
}
