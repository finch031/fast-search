package com.github.search;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;
import com.github.search.utils.Tuple;
import com.github.search.utils.Utils;

import static com.github.search.utils.Utils.AVAILABLE_PROCESSORS;
import static com.github.search.utils.Utils.paramIndexSearch;

public class FastSearch {
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String USAGE =
            "usage: " + LINE_SEPARATOR +
                    " java -jar fast-search-all.jar " + LINE_SEPARATOR +
                    "    ---dirs dirs(comma-delimited) " + LINE_SEPARATOR +
                    "    ---file_prefixes file_prefixes(comma-delimited) " + LINE_SEPARATOR +
                    "    ---file_suffixes file_suffixes(comma-delimited) " + LINE_SEPARATOR +
                    "    ---file_names file_names(comma-delimited) " + LINE_SEPARATOR +
                    "    ---file_size_range file_size_range(format must be:[min_bytes,max_bytes]) " + LINE_SEPARATOR +
                    "    ---file_modified_time_range file_modified_time_range(format must be:[yyyyMMddHHmmss,yyyyMMddHHmmss]) " + LINE_SEPARATOR +
                    "    ---file_access read|write|execute(comma-delimited) " + LINE_SEPARATOR +
                    "    ---file_content_words file_content_words(comma-delimited)"
            ;

    private static void printUsageAndExit(String...messages){
        for (String message : messages) {
            System.err.println(message);
        }
        System.err.println(USAGE);
        System.exit(1);
    }

    private static List<String> getDirsParam(String[] args){
        List<String> dirList = new ArrayList<>();
        int index = paramIndexSearch(args,"---dirs");
        if(index != -1){
            String dirsStr = args[index+1];
            String[] tempArr = dirsStr.split(",");
            for (String s : tempArr) {
                if(Utils.dirExists(s)){
                    dirList.add(s);
                }else{
                    System.err.printf("warn: dir %s not exists.",s);
                }
            }
            if(dirList.isEmpty()){
                printUsageAndExit("error: ---dirs is empty:" + dirsStr);
            }
        }else {
            printUsageAndExit("error: ---dirs not found!");
        }

        return dirList;
    }

    private static List<String> getFilePrefixParam(String[] args){
        List<String> prefixList = new ArrayList<>();
        int index = paramIndexSearch(args,"---file_prefixes");
        if(index != -1){
            String prefixStr = args[index+1];
            String[] tempArr = prefixStr.split(",");
            for (String s : tempArr) {
                if(s != null && !s.isEmpty()){
                    prefixList.add(s);
                }
            }
            if(prefixList.isEmpty()){
                printUsageAndExit("error: ---file_prefixes is empty:" + prefixStr);
            }
        }

        return prefixList;
    }

    private static List<String> getFileSuffixParam(String[] args){
        List<String> suffixList = new ArrayList<>();
        int index = paramIndexSearch(args,"---file_suffixes");
        if(index != -1){
            String suffixStr = args[index+1];
            String[] tempArr = suffixStr.split(",");
            for (String s : tempArr) {
                if(s != null && !s.isEmpty()){
                    suffixList.add(s);
                }
            }
            if(suffixList.isEmpty()){
                printUsageAndExit("error: ---file_suffixes is empty:" + suffixStr);
            }
        }

        return suffixList;
    }

    private static List<String> getFileNamesParam(String[] args){
        List<String> namesList = new ArrayList<>();
        int index = paramIndexSearch(args,"---file_names");
        if(index != -1){
            String namesStr = args[index+1];
            String[] tempArr = namesStr.split(",");
            for (String s : tempArr) {
                if(s != null && !s.isEmpty()){
                    namesList.add(s);
                }
            }
            if(namesList.isEmpty()){
                printUsageAndExit("error: ---file_names is empty:" + namesStr);
            }
        }

        return namesList;
    }

    private static Tuple<Long,Long> getFileModifiedTimeRangeParam(String[] args){
        Tuple<Long,Long> modifiedTimeRangeTuple = null;

        int index = paramIndexSearch(args,"---file_modified_time_range");
        if(index != -1){
            String modifiedTimeRangeStr = args[index+1];
            String[] tempArr = modifiedTimeRangeStr.replaceAll("[\\[\\]]","").split(",");
            if(tempArr.length != 2){
                printUsageAndExit("error: ---file_modified_time_range is invalid:" + modifiedTimeRangeStr);
            }

            long minTs = Utils.parseTs(tempArr[0]);
            long maxTs = Utils.parseTs(tempArr[1]);
            if(minTs <= 0 || maxTs <= 0 || minTs > maxTs){
                printUsageAndExit("error: ---file_modified_time_range is invalid:" + modifiedTimeRangeStr);
            }

            modifiedTimeRangeTuple = new Tuple<>(minTs,maxTs);
        }

        return modifiedTimeRangeTuple;
    }

    private static Tuple<Long,Long> getFileSizeRangeParam(String[] args){
        Tuple<Long,Long> sizeRangeTuple = null;

        int index = paramIndexSearch(args,"---file_size_range");
        if(index != -1){
            String fileSizeRangeStr = args[index+1];
            String[] tempArr = fileSizeRangeStr.replaceAll("[\\[\\]]","").split(",");
            if(tempArr.length != 2){
                printUsageAndExit("error: ---file_size_range is invalid:" + fileSizeRangeStr);
            }

            long minBytes = Utils.longParse(tempArr[0]);
            long maxBytes = Utils.longParse(tempArr[1]);

            if(minBytes < 0 || maxBytes < 0 || minBytes > maxBytes){
                printUsageAndExit("error: ---file_size_range is invalid:" + fileSizeRangeStr);
            }

            sizeRangeTuple = new Tuple<>(minBytes,maxBytes);
        }

        return sizeRangeTuple;
    }

    private static List<String> getFileAccessParam(String[] args){
        List<String> fileAccessList = new ArrayList<>();
        int index = paramIndexSearch(args,"---file_access");
        if(index != -1){
            String fileAccessStr = args[index+1];
            String[] tempArr = fileAccessStr.split(",");
            for (String s : tempArr) {
                if(s.equalsIgnoreCase("read") ||
                   s.equalsIgnoreCase("write") ||
                   s.equalsIgnoreCase("execute")){
                    fileAccessList.add(s);
                }else{
                    System.err.printf("warn: access %s is invalid. %n",s);
                }
            }

            if(fileAccessList.isEmpty()){
                printUsageAndExit("error: ---file_access is empty:" + fileAccessStr);
            }
        }

        return fileAccessList;
    }

    private static List<String> getFileContentWordsParam(String[] args){
        List<String> fileContentWordsList = new ArrayList<>();
        int index = paramIndexSearch(args,"---file_content_words");
        if(index != -1){
            String fileContentWordsStr = args[index+1];
            String[] tempArr = fileContentWordsStr.split(",");
            for (String s : tempArr) {
                if(s != null && !s.isEmpty()){
                    fileContentWordsList.add(s);
                }
            }

            if(fileContentWordsList.isEmpty()){
                printUsageAndExit("error: ---file_content_words is empty:" + fileContentWordsStr);
            }
        }

        return fileContentWordsList;
    }

    public static void main(String[] args){
        List<String> dirList = getDirsParam(args);
        List<String> prefixList = getFilePrefixParam(args);
        List<String> suffixList = getFileSuffixParam(args);
        List<String> fileNames = getFileNamesParam(args);
        Tuple<Long,Long> modifiedTimeRange = getFileModifiedTimeRangeParam(args);
        Tuple<Long,Long> fileSizeRange = getFileSizeRangeParam(args);
        List<String> fileAccessList = getFileAccessParam(args);
        List<String> fileContentWordsList = getFileContentWordsParam(args);

        if(prefixList.isEmpty() && suffixList.isEmpty() &&
           fileNames.isEmpty() && modifiedTimeRange == null &&
           fileSizeRange == null && fileAccessList.isEmpty() &&
           fileContentWordsList.isEmpty()){
            System.err.println("error: no search condition found!");
            System.exit(1);
        }

        long startTs = System.currentTimeMillis();

        // 文件内容搜索队列.
        ConcurrentLinkedQueue<File> filesQueue = new ConcurrentLinkedQueue<>();

        // 设定过滤规则
        Predicate<Path> fileFilter = new Predicate<Path>() {
            @Override
            public boolean test(Path path) {
                File file = path.toFile();
                // 文件名前缀过滤规则.
                if(!prefixList.isEmpty()){
                    boolean prefixFlag = false;
                    for (String s : prefixList) {
                        if(file.getName().startsWith(s)){
                            prefixFlag = true;
                            break;
                        }
                    }
                    if(!prefixFlag){
                        return false;
                    }
                }

                // 文件名后缀过滤规则.
                if(!suffixList.isEmpty()){
                    boolean suffixFlag = false;
                    for (String s : suffixList) {
                        if(file.getName().endsWith(s)){
                            suffixFlag = true;
                            break;
                        }
                    }
                    if(!suffixFlag){
                        return false;
                    }
                }

                // 文件名过滤规则.
                if(!fileNames.isEmpty()){
                    boolean fileNameFlag = false;
                    String currFileName = file.getName();
                    for (String fileName : fileNames) {
                        // contains or equals?
                        if(currFileName.contains(fileName)){
                            fileNameFlag = true;
                            break;
                        }
                    }
                    if(!fileNameFlag){
                        return false;
                    }
                }

                // 文件修改时间过滤规则
                if(modifiedTimeRange != null){
                    long lastModified = file.lastModified();
                    if(lastModified >= modifiedTimeRange.v1() && lastModified <= modifiedTimeRange.v2()){
                        // pass.
                    }else{
                        return false;
                    }
                }

                // 文件大小过滤规则
                if(fileSizeRange != null){
                    long fileSize = file.length();
                    if(fileSize >= fileSizeRange.v1() && fileSize <= fileSizeRange.v2()){
                        // pass.
                    }else{
                        return false;
                    }
                }

                // 文件权限过滤规则
                if(!fileAccessList.isEmpty()){
                    boolean fileAccessFlag = true;
                    for (String s : fileAccessList) {
                        if(s.equalsIgnoreCase("read")){
                            if(!file.canRead()){
                                fileAccessFlag = false;
                            }
                        }

                        if(s.equalsIgnoreCase("write")){
                            if(!file.canWrite()){
                                fileAccessFlag = false;
                            }
                        }

                        if(s.equalsIgnoreCase("execute")){
                            if(!file.canExecute()){
                                fileAccessFlag = false;
                            }
                        }
                    }

                    if(!fileAccessFlag){
                        return false;
                    }
                }

                // 文件内容过滤规则.
                if(!fileContentWordsList.isEmpty()){
                    // boolean fileContentFlag = Utils.readAndLineMatch(file, Charset.defaultCharset(),fileContentWordsList);
                    // if(!fileContentFlag){
                    //     return false;
                    // }

                    // 放入队列中进行异步文件内容过滤.
                    filesQueue.offer(file);
                    return false;
                }

                return true;
            }
        };

        ThreadPoolExecutor poolExecutor = null;
        // 文件内容搜索线程任务.
        Runnable task = Utils.fileContentWordsSearchTask(filesQueue,fileContentWordsList);

        if(!fileContentWordsList.isEmpty()){
            // 最少4个线程任务.
            int realThreadNum = Math.max(AVAILABLE_PROCESSORS, 4);
            // 文件内容搜索线程池.
            poolExecutor = Utils.newCachedThreadPool(realThreadNum,2 * realThreadNum,30,100);

            // 启动文件内容搜索线程任务.
            for(int i = 0; i < realThreadNum; i++){
                poolExecutor.submit(task);
            }
        }

        int i = 0;
        for (String dir : dirList) {
            Path path = Paths.get(dir);
            Collection<String> scanFiles = Utils.dirFileScan(path.toFile(),fileFilter);
            for (String scanFile : scanFiles) {
                System.out.println(++i + " => " + scanFile);
            }
        }

        // 等待文件内容搜索线程池结束.
        if(poolExecutor != null){
            while (true){
                // 文件队列为空,文件内容搜索任务已全部消费完.
                if(filesQueue.isEmpty()){
                    // 有序关闭线程池中任务已结束的线程
                    poolExecutor.shutdown();
                    try{
                        /*
                         * Blocks until all tasks have completed execution after a shutdown
                         * request, or the timeout occurs, or the current thread is
                         * interrupted, whichever happens first.
                         * true:if this executor terminated
                         * false:if the timeout elapsed before termination
                         * */
                        if(!poolExecutor.awaitTermination(1L, TimeUnit.SECONDS)) {
                            // There are no guarantees beyond best-effort attempts to stop
                            // processing actively executing tasks.
                            // This implementation cancels tasks via Thread.interrupt,
                            // so any task that fails to respond to interrupts may never terminate.
                            poolExecutor.shutdownNow();
                        }

                        if(!poolExecutor.awaitTermination(1L, TimeUnit.SECONDS)){
                            System.err.println("Unable to shutdown properly.");
                        }
                    }catch (InterruptedException ie){
                        ie.printStackTrace();
                    }

                    break;
                }else{
                    Utils.sleepQuietly(100);
                }
            } // end of while
        }

        System.out.println("fast search time: " + Utils.appendPosixTime(System.currentTimeMillis() - startTs));
    }
}
