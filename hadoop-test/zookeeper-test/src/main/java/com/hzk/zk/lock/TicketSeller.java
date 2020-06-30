package com.hzk.zk.lock;

public class TicketSeller {

    private void sell(){
        System.out.println("售票开始");
        int sleepMillis = 1000 * 5;
        try {
            Thread.sleep(sleepMillis);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("售票结束");
    }


    public void sellTicketWithLock() throws Exception{
        MyLock lock = new MyLock();
        lock.acquireLock();
        sell();
        lock.releaseLock();
    }

    public static void main(String[] args) throws Exception {
        TicketSeller ticketSeller = new TicketSeller();
        for (int i = 0; i < 10; i++) {
            ticketSeller.sellTicketWithLock();
        }
    }


}
