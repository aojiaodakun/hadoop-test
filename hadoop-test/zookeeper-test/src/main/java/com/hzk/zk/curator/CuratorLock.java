package com.hzk.zk.curator;

import com.hzk.zk.constants.BasicConstants;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 分布式锁
 */
public class CuratorLock {

    CuratorFramework client;

    @Before
    public void before(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        client = CuratorFrameworkFactory.builder()
                .connectString(BasicConstants.IP_CLUSTER)
                .sessionTimeoutMs(1000 * 20)
                .retryPolicy(retryPolicy)
                .namespace("create")
                .build();
        client.start();
    }

    @After
    public void after(){
        client.close();
    }

    @Test
    public void lock1() throws Exception{
        // 排它锁
        InterProcessLock lock = new InterProcessMutex(client,"/lock1");
        System.out.println("等待获取锁对象");
        lock.acquire();
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000 * 3);
            System.out.println(i);
        }
        lock.release();
        System.out.println("等待释放锁");
    }

    @Test
    public void lock2() throws Exception{
        // 读写锁
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client,"/lock1");
        InterProcessLock interProcessLock = lock.readLock();
        System.out.println("等待获取锁对象!");
        interProcessLock.acquire();
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000 * 3);
            System.out.println(i);
        }
        interProcessLock.release();
        System.out.println("等待释放锁");
    }

    @Test
    public void lock3() throws Exception{
        // 读写锁
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client,"/lock1");
        InterProcessLock interProcessLock = lock.writeLock();
        System.out.println("等待获取锁对象!");
        interProcessLock.acquire();
        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000 * 3);
            System.out.println(i);
        }
        interProcessLock.release();
        System.out.println("等待释放锁");
    }


}
