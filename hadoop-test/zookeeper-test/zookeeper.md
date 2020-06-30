###hadoop各种组件的简单API测试
####一、zk伪集群搭建(单机启3个应用)
#####1、zk官网下载linux版本：zookeeper-3.4.14.tar.gz
#####2、解压：tar -zxvf zookeeper-3.4.14.tar.gz，产生zookeeper-3.4.14目录
#####3、改名：mv zookeeper-3.4.14 zookeeper2181
#####4、拷贝2个目录：cp -rf zookeeper2181 zookeeper2182、cp -rf zookeeper2181 zookeeper2183
#####5、修改zk配置，以2181为例
    cd zookeeper2181
    创建数据目录
    mkdir data
    设置当前应用的集群id，2182设置成2,2183设置成3
    echo "1" > myid
    修改配置信息
    cd zookeeper2181/conf
    cp zoo_sample.cfg zoo.cfg
    vi zoo.cfg
        dataDir=/opt/hadoop/zookeeper/zookeeper2181/data
        2287端口：zk通讯端口。3387端口：leader选举端口
        server.1=192.168.44.128:2287:3387
        server.2=192.168.44.128:2288:3388
        server.3=192.168.44.128:2289:3389
#####6、配置好3个应用后，分别启动
    cd zookeeper2181/bin
    ./zkServer.sh start
    3台启动后可使用./zkServer.sh status查看当前zk的角色
#####7、客户端连接
    ./zkCli.sh -server 192.168.44.128:2181                   
#####视频参考地址：https://www.bilibili.com/video/BV1M741137qY?p=41


####二、zk伪集群：原生API、curator-API测试
#####备注：配合尚硅谷视频使用
#####视频地址：https://www.bilibili.com/video/BV1M741137qY



