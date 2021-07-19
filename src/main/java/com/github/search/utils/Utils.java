package com.github.search.utils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public final class Utils {
    public static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    private Utils(){
        // no instance.
    }

    /**
     * get the stack trace from an exception as a string
     */
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    public static void sleepQuietly(long millis){
        try{
            Thread.sleep(millis);
        }catch (InterruptedException ie){
            // ignore.
        }
    }

    /**
     * Returns the current working directory as specified by the {@code user.dir} system property.
     * @return current working directory
     */
    public static Path getCurrentWorkingDirectory() {
        return Paths.get(System.getProperty("user.dir"));
    }

    /**
     * directory file scan on file filter.
     * */
    public static Collection<String> dirFileScan(File baseDir, Predicate<Path> fileFilter){
        if(!baseDir.exists()){
            throw new IllegalArgumentException(
                    String.format("the directory %s does not exist.",baseDir)
            );
        }

        if(!baseDir.isDirectory()){
            throw new IllegalArgumentException(
                    String.format("the %s is not a directory.",baseDir)
            );
        }

        FilterFileVisitor fileVisitor = new FilterFileVisitor(fileFilter);
        try{
            Files.walkFileTree(
                    Paths.get(baseDir.getCanonicalPath()),
                    EnumSet.of(FileVisitOption.FOLLOW_LINKS),
                    Integer.MAX_VALUE,
                    fileVisitor
                    );
        }catch (Exception ex){
            String errorMsg = stackTrace(ex);
            System.err.println(errorMsg);
        }

        return fileVisitor.getFilterFiles();
    }

    /**
     * 查找指定命令行参数的索引位置.
     * @param args 命令行参数数组.
     * @param param 待查找的命令行参数.
     * @return index 参数索引位置,-1表示没有查找到.
     * */
    public static int paramIndexSearch(String[] args,String param){
        int index = -1;
        for (int i = 0; i < args.length; i++) {
            if(args[i].equalsIgnoreCase(param)){
                index = i;
                break;
            }
        }

        return index;
    }

    /**
     * dir exists check.
     * */
    public static boolean dirExists(String dir){
        File file = new File(dir);
        return file.exists();
    }

    /**
     * long parse.
     * */
    public static long longParse(String str){
        return Long.parseLong(str);
    }

    /**
     * parse str to timestamp.
     * */
    public static long parseTs(String str){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        long ts = 0;
        try{
            Date date = sdf.parse(str);
            ts = date.getTime();
        }catch (Exception ex){
            ex.printStackTrace();
        }

        return ts;
    }

    /**
     * read file line as string and match words.
     * */
    public static boolean readAndLineMatch(File file, Charset charset,List<String> fileContentWordsList){
        boolean matchLineSuccess = false;
        String threadName = Thread.currentThread().getName();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset))){
            int lineNum = 0;
            for (;;) {
                String line = reader.readLine();
                if (line == null){
                    break;
                }else{
                    lineNum++;
                    for (String s : fileContentWordsList) {
                        // 文本行内容匹配
                        // TODO
                        if(line.contains(s)){
                            String matchMessage = String.format("%s,%s,%s,[ %s ]",threadName,file,lineNum,line);
                            System.out.println(matchMessage);
                            matchLineSuccess = true;
                        }
                    }
                }
            }
        }catch (IOException ioe){
            String errorMsg = stackTrace(ioe);
            System.err.println("==================error message=======================");
            System.err.println(errorMsg);
            System.err.println("==================error message=======================");
        }

        return matchLineSuccess;
    }

    /**
     * create thread pool.
     * */
    public static ThreadPoolExecutor newCachedThreadPool(int corePoolSize,int maxPoolSize,long keepAliveTime,int blockingQueueSize){
        // 线程任务拒绝策略: 线程任务超过线程池大小和阻塞队列时,新来的任务由main线程执行
        RejectedExecutionHandler rejectionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        String threadGroupName = "search-thread";
        NamedThreadFactory threadFactory = new NamedThreadFactory(threadGroupName,false);
        BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<>(blockingQueueSize);

        // 线程池创建
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                blockingQueue,
                threadFactory,
                rejectionHandler);
        threadPool.allowCoreThreadTimeOut(true);

        return threadPool;
    }

    /**
     * create file content words search task.
     * */
    public static Runnable fileContentWordsSearchTask(ConcurrentLinkedQueue<File> filesQueue,
                                                     List<String> fileContentWordsList){
        return new Runnable() {
            @Override
            public void run() {
                while(true){
                    File file = filesQueue.poll();
                    if(file != null){
                        Utils.readAndLineMatch(file, Charset.defaultCharset(), fileContentWordsList);
                    }

                    try{
                        Thread.sleep(200);
                    }catch (InterruptedException ie){
                        // 响应线程池shutdownNow()发送的Thread.interrupt信号来关闭当前线程(即break退出loop)
                        break;
                    }
                }
            }
        };
    }

    /**
     * given a time expressed in milliseconds,
     * append the time formatted as "hh[:mm[:ss]]".
     * @param millis Milliseconds
     */
    public static String appendPosixTime(long millis) {
        StringBuilder sb = new StringBuilder();
        if (millis < 0) {
            sb.append('-');
            millis = -millis;
        }

        long hours = millis / 3600000;
        sb.append(hours);
        millis -= hours * 3600000;
        if (millis == 0) {
            return sb.toString();
        }

        sb.append(':');

        long minutes = millis / 60000;
        if (minutes < 10) {
            sb.append('0');
        }
        sb.append(minutes);
        millis -= minutes * 60000;
        if (millis == 0) {
            return sb.toString();
        }

        sb.append(':');

        long seconds = millis / 1000;
        if (seconds < 10) {
            sb.append('0');
        }
        sb.append(seconds);

        return sb.toString();
    }


    private static class NamedThreadFactory implements ThreadFactory{
        private final ThreadGroup threadGroup;
        private final String name;
        private final boolean makeDaemons;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        public NamedThreadFactory(String name,boolean makeDaemons){
            this.threadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup(),name);
            this.name = name;
            this.makeDaemons = makeDaemons;
        }

        public Thread newThread(Runnable runnable){
            Thread thread = new Thread(threadGroup,runnable,name + "-" + threadNumber.getAndIncrement());
            thread.setDaemon(makeDaemons);
            if(thread.getPriority() != Thread.NORM_PRIORITY){
                thread.setPriority(Thread.NORM_PRIORITY);
            }
            return thread;
        }
    }
}
