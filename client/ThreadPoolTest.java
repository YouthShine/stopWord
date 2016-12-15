package data.yunsom.com.client;



/**
 * Created on 2011-12-28
 * <p>Description: [Java 线程池学习]</p>
 * @author         shixing_11@sina.com
 */
public class ThreadPoolTest implements Runnable { 
     public void run() { 
          synchronized(this) { 
            try{
                System.out.println(Thread.currentThread().getName());
                Thread.sleep(3000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
          } 
     } 

}