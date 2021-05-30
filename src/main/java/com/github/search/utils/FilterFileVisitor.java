package com.github.search.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Predicate;

public class FilterFileVisitor extends SimpleFileVisitor<Path> {
    private final Predicate<Path> fileFilter;
    private final List<String> filterFiles;

    public FilterFileVisitor(Predicate<Path> fileFilter){
        this.fileFilter = fileFilter;
        this.filterFiles = new LinkedList<>();
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (fileFilter.test(file)) {
            filterFiles.add(file.toFile().getCanonicalPath());
        }
        return super.visitFile(file, attrs);
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc){
        return FileVisitResult.CONTINUE;
    }

    public Collection<String> getFilterFiles() {
        return Collections.unmodifiableCollection(filterFiles);
    }
}
