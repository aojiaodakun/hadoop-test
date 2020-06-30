package com.hzk.zk.curator;

import com.hzk.zk.constants.BasicConstants;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CuratorCreate {

    CuratorFramework client;

    String namespace = "create";

    @Before
    public void before(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        client = CuratorFrameworkFactory.builder()
                    .connectString(BasicConstants.IP_CLUSTER)
                    .sessionTimeoutMs(1000 * 20)
                    .retryPolicy(retryPolicy)
                    .namespace(namespace)
                    .build();
        client.start();
    }

    @After
    public void after(){
        client.close();
    }


    @Test
    public void create1() throws Exception{
        String node = "1";
        client.create()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/node" + node,("node" + node).getBytes());
        System.out.println("结束");
    }


    @Test
    public void create2() throws Exception{
        String node = "2";
        List<ACL> list = new ArrayList<>();
        Id id = new Id("ip", BasicConstants.IP);
        list.add(new ACL(ZooDefs.Perms.ALL,id));
        client.create()
                .withMode(CreateMode.PERSISTENT)
                .withACL(list)
                .forPath("/node" + node,("node" + node).getBytes());
        System.out.println("结束");
    }

    @Test
    public void create3() throws Exception{
        String node = "3";
        // 递归创建节点
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath("/node3/node" + node,("node" + node).getBytes());
        System.out.println("结束");
    }

    // 异步
    @Test
    public void create4() throws Exception{
        String node = "4";
        // 递归创建节点
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println(curatorEvent.getPath());
                        System.out.println(curatorEvent.getType());
                    }
                })
                .forPath("/node" + node,("node" + node).getBytes());
        Thread.sleep(1000 * 5);
        System.out.println("结束");
    }


}
