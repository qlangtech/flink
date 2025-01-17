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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * The client to talk with kubernetes. The interfaces will be called both in Client and
 * ResourceManager. To avoid potentially blocking the execution of RpcEndpoint's main thread, these
 * interfaces {@link #createTaskManagerPod(KubernetesPod)}, {@link #stopPod(String)} should be
 * implemented asynchronously.
 */
public interface FlinkKubeClient extends AutoCloseable {

    /**
     * Create the Master components, this can include the Deployment, the ConfigMap(s), and the
     * Service(s).
     *
     * @param kubernetesJMSpec jobmanager specification
     */
    void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec);

    /**
     * Create task manager pod.
     *
     * @param kubernetesPod taskmanager pod
     * @return Return the taskmanager pod creation future
     */
    CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod);

    /**
     * Stop a specified pod by name.
     *
     * @param podName pod name
     * @return Return the pod stop future
     */
    CompletableFuture<Void> stopPod(String podName);

    /**
     * Stop cluster and clean up all resources, include services, auxiliary services and all running
     * pods.
     *
     * @param clusterId cluster id
     */
    void stopAndCleanupCluster(String clusterId);

    /**
     * Get the kubernetes service of the given flink clusterId.
     *
     * @param serviceName the name of the service
     * @return Return the optional kubernetes service of the specified name.
     */
    Optional<KubernetesService> getService(String serviceName);

    /**
     * Get the rest endpoint for access outside cluster.
     *
     * @param clusterId cluster id
     * @return Return empty if the service does not exist or could not extract the Endpoint from the
     *     service.
     */
   default Optional<Endpoint> getRestEndpoint(String clusterId){
       return getRestEndpoint(clusterId,true);
   }

    /**
     *
     * @param clusterId
     * @param envAware 是否需要关注环境，当TIS console节点部署在非k8s环境中，由于客户端有需要直接打开flink控制台的需求，运行时需要判断当前是否运行在k8s的环境中，如果是则返回内部clusterIp，如果否，则需要返回用户定义的服务地址
     * @return
     */
    Optional<Endpoint> getRestEndpoint(String clusterId,boolean envAware);

    /**
     * List the pods with specified labels.
     *
     * @param labels labels to filter the pods
     * @return pod list
     */
    List<KubernetesPod> getPodsWithLabels(Map<String, String> labels);

    /**
     * Watch the pods selected by labels and do the {@link WatchCallbackHandler}.
     *
     * @param labels labels to filter the pods to watch
     * @param podCallbackHandler podCallbackHandler which reacts to pod events
     * @return Return a watch for pods. It needs to be closed after use.
     */
    KubernetesWatch watchPodsAndDoCallback(
            Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler)
            throws Exception;

    /**
     * Create a leader elector service based on Kubernetes api.
     *
     * @param leaderElectionConfiguration election configuration
     * @param leaderCallbackHandler Callback when the current instance is leader or not.
     * @return Return the created leader elector. It should be started manually via {@code
     *     KubernetesLeaderElector#run}.
     */
    KubernetesLeaderElector createLeaderElector(
            KubernetesLeaderElectionConfiguration leaderElectionConfiguration,
            KubernetesLeaderElector.LeaderCallbackHandler leaderCallbackHandler);

    /**
     * Create the ConfigMap with specified content. If the ConfigMap already exists, a {@link
     * org.apache.flink.kubernetes.kubeclient.resources.KubernetesException} will be thrown.
     *
     * @param configMap ConfigMap to be created.
     * @return Return the ConfigMap create future. The returned future will be completed
     *     exceptionally if the ConfigMap already exists.
     */
    CompletableFuture<Void> createConfigMap(KubernetesConfigMap configMap);

    /**
     * Get the ConfigMap with specified name.
     *
     * @param name name of the ConfigMap to retrieve.
     * @return Return the ConfigMap, or empty if the ConfigMap does not exist.
     */
    Optional<KubernetesConfigMap> getConfigMap(String name);

    /**
     * Update an existing ConfigMap with the data. Benefit from <a
     * href=https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions>resource
     * version</a> and combined with {@link #getConfigMap(String)}, we could perform a
     * get-check-and-update transactional operation. Since concurrent modification could happen on a
     * same ConfigMap, the update operation may fail. We need to retry internally in the
     * implementation.
     *
     * @param configMapName configMapName specifies the name of the ConfigMap which shall be
     *     updated.
     * @param updateFunction Function to be applied to the obtained ConfigMap and get a new updated
     *     one. If the returned optional is empty, we will not do the update.
     * @return Return the ConfigMap update future. The boolean result indicates whether the
     *     ConfigMap is updated. The returned future will be completed exceptionally if the
     *     ConfigMap does not exist. A failure during the update operation will result in the future
     *     failing with a {@link PossibleInconsistentStateException} indicating that no clear
     *     decision can be made on whether the update was successful or not. The {@code
     *     PossibleInconsistentStateException} not being present indicates that the failure happened
     *     before writing the updated ConfigMap to Kubernetes. For the latter case, it can be
     *     assumed that the ConfigMap was not updated.
     */
    CompletableFuture<Boolean> checkAndUpdateConfigMap(
            String configMapName,
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction);

    /**
     * Delete a Kubernetes ConfigMap by name.
     *
     * @param configMapName ConfigMap name
     * @return Return the delete future.
     */
    CompletableFuture<Void> deleteConfigMap(String configMapName);

    /**
     * Create a shared watcher for ConfigMaps with specified name.
     *
     * @param name name of the ConfigMap to watch.
     * @return Return a shared watcher.
     */
    KubernetesConfigMapSharedWatcher createConfigMapSharedWatcher(String name);

    /** Close the Kubernetes client with no exception. */
    void close();

    /**
     * Load pod from template file.
     *
     * @param podTemplateFile The pod template file.
     * @return Return a Kubernetes pod loaded from the template.
     */
    KubernetesPod loadPodFromTemplateFile(File podTemplateFile);

    /**
     * Update the target ports of the given Kubernetes service.
     *
     * @param serviceName The name of the service which needs to be updated
     * @param portName The port name which needs to be updated
     * @param targetPort The updated target port
     * @return Return the update service target port future
     */
    CompletableFuture<Void> updateServiceTargetPort(
            String serviceName, String portName, int targetPort);

    /** Callback handler for kubernetes resources. */
    interface WatchCallbackHandler<T> {

        void onAdded(List<T> resources);

        void onModified(List<T> resources);

        void onDeleted(List<T> resources);

        void onError(List<T> resources);

        void handleError(Throwable throwable);
    }
}
