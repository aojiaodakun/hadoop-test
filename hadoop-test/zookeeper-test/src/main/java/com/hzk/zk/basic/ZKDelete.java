package com.hzk.zk.basic;

import com.hzk.zk.constants.BasicConstants;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ZKDelete {

    String context = "I am context";
    ZooKeeper zooKeeper = null;

    @Before
    public void before(){
        try{
            //创建一个计数器对象
            CountDownLatch countDownLatch = new CountDownLatch(1);
            //第一个参数是服务器ip和端口号，第二个参数是客户端与服务器的会话超时时间单位ms，第三个参数是监视器对象
            zooKeeper = new ZooKeeper(BasicConstants.IP_ALONE, 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if(event.getState()== Watcher.Event.KeeperState.SyncConnected){
                        System.out.println("连接创建成功");
                        //通知主线程解除阻塞
                        countDownLatch.countDown();
                    }
                }
            });
            //主线程阻塞，等待连接对象的创建成功
            countDownLatch.await();
//            System.out.println("会话编号"+zooKeeper.getSessionId());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @After
    public void after(){
        if(zooKeeper!=null) {
            try {
                zooKeeper.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void delete1() throws Exception{
        zooKeeper.delete("/delete/node1",-1);
    }


    @Test
    public void delete2() throws Exception{
        zooKeeper.delete("/delete/node2", -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                // 0 成功
                System.out.println(rc);
                // 节点的路径
                System.out.println(path);
                // 上下文参数
                System.out.println(ctx);
            }
        },context);
        Thread.sleep(1000 * 10);
        System.out.println("结束");
    }

}
