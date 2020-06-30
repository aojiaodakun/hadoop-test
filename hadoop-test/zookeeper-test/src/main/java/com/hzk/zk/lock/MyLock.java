package com.hzk.zk.lock;

import com.hzk.zk.constants.BasicConstants;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MyLock {
    //创建一个计数器对象
    CountDownLatch countDownLatch = new CountDownLatch(1);

    static ZooKeeper zooKeeper;

    String LOCK_ROOT_PATH = "/locks";

    String LOCK_NODE_NAME = "lock_";
    // 锁节点路径
    String lockPath;


    public MyLock(){
        try {
            zooKeeper = new ZooKeeper(BasicConstants.IP_ALONE, 1000 * 20, (e)->{
                if(e.getType() == Watcher.Event.EventType.None){
                    if(e.getState() == Watcher.Event.KeeperState.SyncConnected){
                        System.out.println("连接成功");
                        countDownLatch.countDown();
                    }
                }
            });
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // 获取锁
    public void acquireLock() throws Exception{
        createLock();
        attemptLock();
    }

    // 创建锁节点
    public void createLock() throws Exception{
        Stat stat = zooKeeper.exists(LOCK_ROOT_PATH,false);
        if(stat == null){
            zooKeeper.create(LOCK_ROOT_PATH,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 临时有序节点
        lockPath = zooKeeper.create(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("节点创建成功:" + lockPath);
    }


    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                synchronized (this){
                    notifyAll();
                }
            }
        }
    };

    // 尝试获取锁
    public void attemptLock() throws Exception{
        List<String> childrenList = zooKeeper.getChildren(LOCK_ROOT_PATH, false);
        Collections.sort(childrenList);
        // /locks/lock_0000000000
        int index = childrenList.indexOf(lockPath.substring(LOCK_ROOT_PATH.length()+1));
        if(index == 0){
            System.out.println("获取锁成功");
            return;
        }else{
            String path = childrenList.get(index -1);
            Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + path,watcher);
            if(stat == null){
                attemptLock();
            }else{
                synchronized (watcher){
                    watcher.wait();
                }
                attemptLock();
            }
        }
    }

    // 释放锁
    public void releaseLock() throws Exception{
        zooKeeper.delete(this.lockPath,-1);
        zooKeeper.close();
        System.out.println("锁已经释放:" + this.lockPath);
    }

}
