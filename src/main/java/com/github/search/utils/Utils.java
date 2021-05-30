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

    public static List<String> readAllLines(File file, Charset charset) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
        try {
            List<String> result = new ArrayList<>();
            for (;;) {
                String line = reader.readLine();
                if (line == null){
                    break;
                }
                result.add(line);
            }
            return result;
        }
        finally {
            reader.close();
        }
    }

    public static boolean readAndLineMatch(File file, Charset charset,List<String> fileContentWordsList) throws IOException{
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
        boolean matchLineSuccess = false;
        try {
            int lineNum = 0;
            for (;;) {
                String line = reader.readLine();
                if (line == null){
                    break;
                }else{
                    lineNum++;
                    for (String s : fileContentWordsList) {
                        if(line.contains(s)){
                            String matchMessage = String.format("match:%s,%s,[ %s ]",file,lineNum,line);
                            System.out.println(matchMessage);
                            matchLineSuccess = true;
                        }
                    }
                }
            }
        }
        finally {
            reader.close();
        }

        return matchLineSuccess;
    }

    public static ThreadPoolExecutor newCachedThreadPool(int corePoolSize,int maxPoolSize,long keepAliveTime,int blockingQueueSize){
        // 线程任务拒绝策略: 线程任务超过线程池大小和阻塞队列时,新来的任务由main线程执行
        RejectedExecutionHandler rejectionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        String threadGroupName = "fast-search";
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

    public static Runnable fileContentWordsSearchTask(ConcurrentLinkedQueue<File> filesQueue,
                                                     List<String> fileContentWordsList){
        return new Runnable() {
            @Override
            public void run() {
                while(true){
                    File file = filesQueue.poll();
                    if(file != null){
                        try {
                            boolean matchLineSuccess = Utils.readAndLineMatch(
                                    file,
                                    Charset.defaultCharset(),
                                    fileContentWordsList);
                            if(matchLineSuccess){
                                System.out.println("match,thread:" + Thread.currentThread().getId() + ",file:" + file.getName());
                            }
                        } catch (IOException e) {
                            // ignore
                        }
                    }else{
                        sleepQuietly(100);
                    }
                }
            }
        };
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
