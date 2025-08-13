package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.kubeclient.decorators.HostAliasDecorator.HOST_IP;
import static org.apache.flink.kubernetes.kubeclient.decorators.HostAliasDecorator.HOST_NAMES;
import static org.apache.flink.kubernetes.kubeclient.decorators.HostAliasDecorator.HOST_NAMES_COUNT;
import static org.apache.flink.kubernetes.kubeclient.decorators.HostAliasDecorator.IP_HOST_PREFIX;
import static org.apache.flink.kubernetes.kubeclient.decorators.HostAliasDecorator.PREFIX_COUNT;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-08-13 11:50
 **/
class HostAliasDecoratorTest {

    /**
     * TC001: 正常输入，多个 host alias
     */
    @Test
    void testAddHostAliasToFlinkConfig_NormalInput() {
        Configuration flinkCfg = new Configuration();
        Map<String, List<String>> hostAliasList = new HashMap<>();
        hostAliasList.put("192.168.1.1", Arrays.asList("host1", "host2"));
        hostAliasList.put("192.168.1.2", Collections.singletonList("host3"));

        HostAliasDecorator.addHostAliasToFlinkConfig(flinkCfg, hostAliasList);

        assertEquals("2", flinkCfg.getString(PREFIX_COUNT, ""));

        String alias0Host = String.format(IP_HOST_PREFIX, String.valueOf(0));
        String alias0HostCount = alias0Host + HOST_NAMES_COUNT;
        assertEquals("2", flinkCfg.getString(alias0HostCount, ""));
        assertEquals("host1", flinkCfg.getString(alias0Host + HOST_NAMES + 0, ""));
        assertEquals("host2", flinkCfg.getString(alias0Host + HOST_NAMES + 1, ""));
        assertEquals("192.168.1.1", flinkCfg.getString(alias0Host + HOST_IP, ""));


        String alias1Host = String.format(IP_HOST_PREFIX, String.valueOf(1));
        String alias1HostCount = alias0Host + HOST_NAMES_COUNT;
        assertEquals("1", flinkCfg.getString(alias1HostCount, ""));
        assertEquals("host3", flinkCfg.getString(alias1Host + HOST_NAMES + 0, ""));
        assertEquals("192.168.1.2", flinkCfg.getString(alias1Host + HOST_IP, ""));
    }
}
