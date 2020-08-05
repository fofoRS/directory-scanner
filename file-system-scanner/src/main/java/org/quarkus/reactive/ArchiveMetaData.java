package org.quarkus.reactive;

import java.time.LocalDateTime;

public class ArchiveMetaData {
    private String name;
    private LocalDateTime createdAt;
    private Long size;
    private boolean isDirectory;
    private String absolutePath;
    private String fileRootPath;

    public ArchiveMetaData(String name, LocalDateTime createdAt,
                           Long size, boolean isDirectory, String absolutePath, String fileRootPath) {
        this.name = name;
        this.createdAt = createdAt;
        this.size = size;
        this.isDirectory = isDirectory;
        this.absolutePath = absolutePath;
        this.fileRootPath = fileRootPath;
    }

    public ArchiveMetaData(){}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public void setDirectory(boolean directory) {
        isDirectory = directory;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public void setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
    }

    public String getFileRootPath() {
        return fileRootPath;
    }

    public void setFileRootPath(String fileRootPath) {
        this.fileRootPath = fileRootPath;
    }
}
