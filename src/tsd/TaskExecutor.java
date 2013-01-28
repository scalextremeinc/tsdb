package net.opentsdb.tsd;

import java.lang.Runnable;
import java.lang.Exception;
import java.lang.InterruptedException;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutor {
    
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);
    
    private static ExecutorService executorService;
    
    private static int corePoolSize = 24;
    private static int maxPoolSize = 48;
    private static long keepAliveTime = 5000;
    
    public static void init(int corePoolSize, int maxPoolSize, long keepAliveTime) {
        TaskExecutor.corePoolSize = corePoolSize;
        TaskExecutor.maxPoolSize = maxPoolSize;
        TaskExecutor.keepAliveTime = keepAliveTime;
        executorService = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        LOG.info("Initialized executor service, corePoolSize: " + corePoolSize
            + ", maxPoolSize: " + maxPoolSize + ", keepAliveTime: " + keepAliveTime);
    }
    
    public static interface Task {
        public Object execute() throws Exception;
        public int getId();
    }
    
    public static final class TaskResult {
        public int id;
        public Object value;
        public Exception exception;
    }
    
    private static class TaskRunnable implements Runnable {
        
        private Task task;
        
        private BlockingQueue<TaskResult> resultQueue;
        
        public TaskRunnable(Task task, BlockingQueue<TaskResult> resultQueue) {
            this.task = task;
            this.resultQueue = resultQueue;
        }
        
        public void run() {
            TaskResult result = new TaskResult();
            try {
                result.id = task.getId();
                result.value = task.execute();
            } catch (Exception e) {
                LOG.error("Exception during task execution: " + e.getMessage());
                result.exception = e;
            }
            while (true)
                try {
                    resultQueue.put(result);
                    break;
                } catch (InterruptedException e) {
                    LOG.error("Unable to put to result queue: " + e.getMessage());
                }
        }
        
    }
    
    private BlockingQueue<TaskResult> resultQueue;
    
    public TaskExecutor() {
        if (executorService == null) {
            executorService = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        }
        resultQueue = new LinkedBlockingQueue<TaskResult>();
    }
    
    public List<TaskResult> parallelize(List<Task> tasks) {
        LOG.info("Executing paraller tasks, number: " + tasks.size());
        for (Task t : tasks) {
            executorService.execute(new TaskRunnable(t, resultQueue));
        }
        List<TaskResult> results = new LinkedList<TaskResult>();
        int i = 0;
        while (i < tasks.size()) {
            while (true)
                try {
                    results.add(resultQueue.take());
                    break;
                } catch (InterruptedException e) {
                    LOG.error("Unable to take from result queue: " + e.getMessage());
                }
            i++;
        }
        return results;
    }
    
}
