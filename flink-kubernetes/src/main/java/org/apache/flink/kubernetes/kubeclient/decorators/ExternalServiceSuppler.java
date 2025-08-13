package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;

import io.fabric8.kubernetes.api.model.Service;

import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-08-12 18:23
 **/
public interface ExternalServiceSuppler extends Function<KubernetesJobManagerParameters, Service> {
    //void decorateFlinkPod(PodBuilder podBuilder);
}
