package edu.mcw.rgd.process;



import java.util.concurrent.*;

/**
 * Created by jthota on 11/14/2018.
 */
public class MyThreadPoolExecutor extends ThreadPoolExecutor {
   // Logger log= LogManager.getLogger(Manager.class);
    public MyThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @Override
    public void afterExecute(Runnable r, Throwable t){
        super.afterExecute(r,t);
        if(t==null && r instanceof Future){
            try{
                Object result=((Future) r).get();

            }catch (CancellationException e){
                t=e;
            }catch (ExecutionException e){
                t=e.getCause();
            }catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
        if(t!=null){
        //    log.error("Uncaught exception! "+t +" STACKTRACE:"+ Arrays.toString(t.getStackTrace()));
            System.exit(1);
        }
    }
}

