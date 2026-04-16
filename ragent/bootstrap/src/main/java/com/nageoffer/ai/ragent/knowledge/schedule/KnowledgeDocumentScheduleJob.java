/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.knowledge.schedule;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.ai.ragent.framework.context.LoginUser;
import com.nageoffer.ai.ragent.framework.context.UserContext;
import com.nageoffer.ai.ragent.framework.exception.ClientException;
import com.nageoffer.ai.ragent.knowledge.config.KnowledgeScheduleProperties;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeBaseDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentScheduleDO;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentScheduleExecDO;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentScheduleExecMapper;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentScheduleMapper;
import com.nageoffer.ai.ragent.knowledge.enums.DocumentStatus;
import com.nageoffer.ai.ragent.knowledge.enums.ScheduleRunStatus;
import com.nageoffer.ai.ragent.knowledge.enums.SourceType;
import com.nageoffer.ai.ragent.knowledge.handler.RemoteFileFetcher;
import com.nageoffer.ai.ragent.knowledge.service.impl.KnowledgeDocumentServiceImpl;
import com.nageoffer.ai.ragent.rag.dto.StoredFileDTO;
import com.nageoffer.ai.ragent.rag.service.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * 知识库文档定时刷新任务
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KnowledgeDocumentScheduleJob {

    private static final String SYSTEM_USER = "system";

    private final KnowledgeDocumentScheduleMapper scheduleMapper;
    private final KnowledgeDocumentScheduleExecMapper execMapper;
    private final KnowledgeDocumentMapper documentMapper;
    private final KnowledgeBaseMapper kbMapper;
    private final KnowledgeDocumentServiceImpl documentService;
    private final FileStorageService fileStorageService;
    private final RemoteFileFetcher remoteFileFetcher;
    private final Executor knowledgeChunkExecutor;
    private final KnowledgeScheduleProperties scheduleProperties;

    private final String instanceId = resolveInstanceId();

    /**
     * 恢复长时间卡在 RUNNING 状态的文档（进程崩溃等异常场景）
     * 超过配置阈值未完成的 RUNNING 文档重置为 FAILED，允许用户手动重试
     */
    @Scheduled(fixedDelay = 60_000, initialDelay = 30_000)
    public void recoverStuckRunningDocuments() {
        long timeoutMinutes = Math.max(scheduleProperties.getRunningTimeoutMinutes(), 10);
        Date threshold = new Date(System.currentTimeMillis() - timeoutMinutes * 60 * 1000);
        int recovered = documentMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                        .set(KnowledgeDocumentDO::getStatus, DocumentStatus.FAILED.getCode())
                        .set(KnowledgeDocumentDO::getUpdatedBy, SYSTEM_USER)
                        .eq(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
                        .lt(KnowledgeDocumentDO::getUpdateTime, threshold)
        );
        if (recovered > 0) {
            log.warn("恢复了 {} 个卡在 RUNNING 状态超过 {} 分钟的文档，已重置为 FAILED", recovered, timeoutMinutes);
        }
    }

    @Scheduled(fixedDelayString = "${rag.knowledge.schedule.scan-delay-ms:10000}")
    public void scan() {
        Date now = new Date();
        List<KnowledgeDocumentScheduleDO> schedules = scheduleMapper.selectList(
                new LambdaQueryWrapper<KnowledgeDocumentScheduleDO>()
                        .eq(KnowledgeDocumentScheduleDO::getEnabled, 1)
                        .and(wrapper -> wrapper.isNull(KnowledgeDocumentScheduleDO::getNextRunTime)
                                .or()
                                .le(KnowledgeDocumentScheduleDO::getNextRunTime, now))
                        .and(wrapper -> wrapper.isNull(KnowledgeDocumentScheduleDO::getLockUntil)
                                .or()
                                .lt(KnowledgeDocumentScheduleDO::getLockUntil, now))
                        .orderByAsc(KnowledgeDocumentScheduleDO::getNextRunTime)
                        .last("LIMIT " + Math.max(scheduleProperties.getBatchSize(), 1))
        );

        if (schedules == null || schedules.isEmpty()) {
            return;
        }

        Date lockUntil = new Date(System.currentTimeMillis() + Math.max(scheduleProperties.getLockSeconds(), 60) * 1000);
        for (KnowledgeDocumentScheduleDO schedule : schedules) {
            if (schedule == null || schedule.getId() == null) {
                continue;
            }
            if (!tryAcquireLock(schedule.getId(), now, lockUntil)) {
                continue;
            }
            try {
                knowledgeChunkExecutor.execute(() -> executeSchedule(schedule.getId()));
            } catch (RejectedExecutionException e) {
                log.error("定时任务提交失败: scheduleId={}, docId={}, kbId={}",
                        schedule.getId(), schedule.getDocId(), schedule.getKbId(), e);
                releaseLock(schedule.getId());
            }
        }
    }

    private void executeSchedule(String scheduleId) {
        Date startTime = new Date();
        KnowledgeDocumentScheduleDO schedule = scheduleMapper.selectById(scheduleId);
        if (schedule == null) {
            return;
        }
        renewLock(scheduleId);

        KnowledgeDocumentDO document = documentMapper.selectById(schedule.getDocId());
        if (document == null || (document.getDeleted() != null && document.getDeleted() == 1)) {
            disableSchedule(schedule, "文档不存在或已删除");
            releaseLock(scheduleId);
            return;
        }
        if (document.getEnabled() != null && document.getEnabled() == 0) {
            disableSchedule(schedule, "文档已禁用");
            releaseLock(scheduleId);
            return;
        }

        String cron = document.getScheduleCron();
        boolean enabled = document.getScheduleEnabled() != null && document.getScheduleEnabled() == 1;
        if (!StringUtils.hasText(cron) || !SourceType.URL.getValue().equalsIgnoreCase(document.getSourceType())) {
            enabled = false;
        }

        schedule.setCronExpr(cron);
        Date nextRunTime;
        if (enabled) {
            try {
                nextRunTime = CronScheduleHelper.nextRunTime(cron, startTime);
            } catch (IllegalArgumentException e) {
                disableSchedule(schedule, "定时表达式不合法");
                releaseLock(scheduleId);
                return;
            }
            if (nextRunTime == null) {
                disableSchedule(schedule, "无法计算下次执行时间");
                releaseLock(scheduleId);
                return;
            }
        } else {
            disableSchedule(schedule, "定时已关闭");
            releaseLock(scheduleId);
            return;
        }

        KnowledgeDocumentScheduleExecDO exec = KnowledgeDocumentScheduleExecDO.builder()
                .scheduleId(scheduleId)
                .docId(document.getId())
                .kbId(document.getKbId())
                .status(ScheduleRunStatus.RUNNING.getCode())
                .startTime(startTime)
                .build();
        execMapper.insert(exec);

        String oldFileUrl = null;
        StoredFileDTO stored = null;
        boolean documentOccupied = false;
        boolean switchedToNewFile = false;
        try (RemoteFileFetcher.RemoteFetchResult fetchResult = remoteFileFetcher.fetchIfChanged(
                document.getSourceLocation(),
                schedule.getLastEtag(),
                schedule.getLastModified(),
                schedule.getLastContentHash(),
                document.getDocName()
        )) {
            if (!fetchResult.changed()) {
                markScheduleSkipped(schedule, exec.getId(), startTime, nextRunTime, fetchResult);
                return;
            }

            if (DocumentStatus.RUNNING.getCode().equals(document.getStatus())) {
                markScheduleSkipped(schedule, exec.getId(), startTime, nextRunTime, "文档正在分块中，跳过本次调度");
                return;
            }

            renewLock(scheduleId);
            if (!tryMarkDocumentRunning(document.getId())) {
                markScheduleSkipped(schedule, exec.getId(), startTime, nextRunTime, "文档正在分块中，跳过本次调度");
                return;
            }
            documentOccupied = true;

            KnowledgeBaseDO kbDO = kbMapper.selectById(document.getKbId());
            if (kbDO == null) {
                throw new ClientException("知识库不存在");
            }

            oldFileUrl = document.getFileUrl();
            try (InputStream tempIn = Files.newInputStream(fetchResult.tempFile())) {
                stored = fileStorageService.upload(
                        kbDO.getCollectionName(),
                        tempIn,
                        fetchResult.size(),
                        fetchResult.fileName(),
                        fetchResult.contentType()
                );
            }

            KnowledgeDocumentDO runtimeDoc = documentMapper.selectById(document.getId());
            if (runtimeDoc == null) {
                throw new ClientException("文档不存在");
            }
            runtimeDoc.setDocName(stored.getOriginalFilename());
            runtimeDoc.setFileUrl(stored.getUrl());
            runtimeDoc.setFileType(stored.getDetectedType());
            runtimeDoc.setFileSize(stored.getSize());
            runtimeDoc.setUpdatedBy(SYSTEM_USER);

            renewLock(scheduleId);
            UserContext.set(LoginUser.builder().username(SYSTEM_USER).build());
            try {
                documentService.chunkDocument(runtimeDoc);
            } finally {
                UserContext.clear();
            }

            KnowledgeDocumentDO latest = documentMapper.selectById(document.getId());
            if (latest == null || !DocumentStatus.SUCCESS.getCode().equals(latest.getStatus())) {
                markScheduleFailed(schedule, exec.getId(), startTime, nextRunTime, "分块失败");
                return;
            }

            applyRefreshedFileMetadata(document.getId(), stored);
            switchedToNewFile = true;

            renewLock(scheduleId);
            markScheduleSuccess(schedule, exec.getId(), startTime, nextRunTime, fetchResult, stored);
        } catch (Exception e) {
            log.error("定时刷新失败: scheduleId={}, docId={}, kbId={}",
                    scheduleId, document.getId(), document.getKbId(), e);
            if (!switchedToNewFile) {
                if (documentOccupied) {
                    markDocumentFailedIfRunning(document.getId());
                }
                markScheduleFailed(schedule, exec.getId(), startTime, nextRunTime, e.getMessage());
            }
        } finally {
            if (switchedToNewFile) {
                deleteOldFileQuietly(oldFileUrl, stored != null ? stored.getUrl() : null);
            } else if (stored != null) {
                deleteOldFileQuietly(stored.getUrl(), null);
            }
            releaseLock(scheduleId);
        }
    }

    private void markScheduleSkipped(KnowledgeDocumentScheduleDO schedule,
                                     String execId,
                                     Date startTime,
                                     Date nextRunTime,
                                     RemoteFileFetcher.RemoteFetchResult fetchResult) {
        KnowledgeDocumentScheduleDO update = KnowledgeDocumentScheduleDO.builder()
                .id(schedule.getId())
                .cronExpr(schedule.getCronExpr())
                .lastRunTime(startTime)
                .nextRunTime(nextRunTime)
                .lastStatus(ScheduleRunStatus.SKIPPED.getCode())
                .lastError(fetchResult.message())
                .lastEtag(fetchResult.etag())
                .lastModified(fetchResult.lastModified())
                .lastContentHash(fetchResult.contentHash())
                .build();
        scheduleMapper.updateById(update);

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = new KnowledgeDocumentScheduleExecDO();
            execUpdate.setId(execId);
            execUpdate.setStatus(ScheduleRunStatus.SKIPPED.getCode());
            execUpdate.setMessage(fetchResult.message());
            execUpdate.setEndTime(new Date());
            execUpdate.setContentHash(fetchResult.contentHash());
            execUpdate.setEtag(fetchResult.etag());
            execUpdate.setLastModified(fetchResult.lastModified());
            execMapper.updateById(execUpdate);
        }
    }

    private void markScheduleSkipped(KnowledgeDocumentScheduleDO schedule,
                                     String execId,
                                     Date startTime,
                                     Date nextRunTime,
                                     String message) {
        KnowledgeDocumentScheduleDO update = KnowledgeDocumentScheduleDO.builder()
                .id(schedule.getId())
                .cronExpr(schedule.getCronExpr())
                .lastRunTime(startTime)
                .nextRunTime(nextRunTime)
                .lastStatus(ScheduleRunStatus.SKIPPED.getCode())
                .lastError(message)
                .build();
        scheduleMapper.updateById(update);

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = KnowledgeDocumentScheduleExecDO.builder()
                    .id(execId)
                    .status(ScheduleRunStatus.SKIPPED.getCode())
                    .message(message)
                    .endTime(new Date())
                    .build();
            execMapper.updateById(execUpdate);
        }
    }

    private void markScheduleSuccess(KnowledgeDocumentScheduleDO schedule,
                                     String execId,
                                     Date startTime,
                                     Date nextRunTime,
                                     RemoteFileFetcher.RemoteFetchResult fetchResult,
                                     StoredFileDTO stored) {
        Date endTime = new Date();
        scheduleMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentScheduleDO.class)
                        .set(KnowledgeDocumentScheduleDO::getCronExpr, schedule.getCronExpr())
                        .set(KnowledgeDocumentScheduleDO::getLastRunTime, startTime)
                        .set(KnowledgeDocumentScheduleDO::getNextRunTime, nextRunTime)
                        .set(KnowledgeDocumentScheduleDO::getLastSuccessTime, endTime)
                        .set(KnowledgeDocumentScheduleDO::getLastStatus, ScheduleRunStatus.SUCCESS.getCode())
                        .set(KnowledgeDocumentScheduleDO::getLastError, null)
                        .set(KnowledgeDocumentScheduleDO::getLastEtag, fetchResult.etag())
                        .set(KnowledgeDocumentScheduleDO::getLastModified, fetchResult.lastModified())
                        .set(KnowledgeDocumentScheduleDO::getLastContentHash, fetchResult.contentHash())
                        .eq(KnowledgeDocumentScheduleDO::getId, schedule.getId())
        );

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = KnowledgeDocumentScheduleExecDO.builder()
                    .id(execId)
                    .status(ScheduleRunStatus.SUCCESS.getCode())
                    .message("刷新成功")
                    .endTime(endTime)
                    .fileName(stored.getOriginalFilename())
                    .fileSize(stored.getSize())
                    .contentHash(fetchResult.contentHash())
                    .etag(fetchResult.etag())
                    .lastModified(fetchResult.lastModified())
                    .build();
            execMapper.updateById(execUpdate);
        }
    }

    private void markScheduleFailed(KnowledgeDocumentScheduleDO schedule,
                                    String execId,
                                    Date startTime,
                                    Date nextRunTime,
                                    String errorMessage) {
        KnowledgeDocumentScheduleDO update = KnowledgeDocumentScheduleDO.builder()
                .id(schedule.getId())
                .cronExpr(schedule.getCronExpr())
                .lastRunTime(startTime)
                .nextRunTime(nextRunTime)
                .lastStatus(ScheduleRunStatus.FAILED.getCode())
                .lastError(truncate(errorMessage))
                .build();
        scheduleMapper.updateById(update);

        if (execId != null) {
            KnowledgeDocumentScheduleExecDO execUpdate = new KnowledgeDocumentScheduleExecDO();
            execUpdate.setId(execId);
            execUpdate.setStatus(ScheduleRunStatus.FAILED.getCode());
            execUpdate.setMessage(truncate(errorMessage));
            execUpdate.setEndTime(new Date());
            execMapper.updateById(execUpdate);
        }
    }

    private void disableSchedule(KnowledgeDocumentScheduleDO schedule, String reason) {
        scheduleMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentScheduleDO.class)
                        .set(KnowledgeDocumentScheduleDO::getEnabled, 0)
                        .set(KnowledgeDocumentScheduleDO::getNextRunTime, null)
                        .set(KnowledgeDocumentScheduleDO::getLastStatus, ScheduleRunStatus.FAILED.getCode())
                        .set(KnowledgeDocumentScheduleDO::getLastError, truncate(reason))
                        .eq(KnowledgeDocumentScheduleDO::getId, schedule.getId())
        );
    }

    private boolean tryAcquireLock(String scheduleId, Date now, Date lockUntil) {
        return scheduleMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentScheduleDO.class)
                        .set(KnowledgeDocumentScheduleDO::getLockOwner, instanceId)
                        .set(KnowledgeDocumentScheduleDO::getLockUntil, lockUntil)
                        .eq(KnowledgeDocumentScheduleDO::getId, scheduleId)
                        .and(w -> w.isNull(KnowledgeDocumentScheduleDO::getLockUntil)
                                .or()
                                .lt(KnowledgeDocumentScheduleDO::getLockUntil, now))
        ) > 0;
    }

    private void releaseLock(String scheduleId) {
        scheduleMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentScheduleDO.class)
                        .set(KnowledgeDocumentScheduleDO::getLockOwner, null)
                        .set(KnowledgeDocumentScheduleDO::getLockUntil, null)
                        .eq(KnowledgeDocumentScheduleDO::getId, scheduleId)
                        .eq(KnowledgeDocumentScheduleDO::getLockOwner, instanceId)
        );
    }

    private void renewLock(String scheduleId) {
        if (scheduleId == null) {
            return;
        }
        Date lockUntil = new Date(System.currentTimeMillis() + Math.max(scheduleProperties.getLockSeconds(), 60) * 1000);
        scheduleMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentScheduleDO.class)
                        .set(KnowledgeDocumentScheduleDO::getLockUntil, lockUntil)
                        .eq(KnowledgeDocumentScheduleDO::getId, scheduleId)
                        .eq(KnowledgeDocumentScheduleDO::getLockOwner, instanceId)
        );
    }

    private boolean tryMarkDocumentRunning(String docId) {
        return documentMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                        .set(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
                        .set(KnowledgeDocumentDO::getUpdatedBy, SYSTEM_USER)
                        .eq(KnowledgeDocumentDO::getId, docId)
                        .eq(KnowledgeDocumentDO::getDeleted, 0)
                        .eq(KnowledgeDocumentDO::getEnabled, 1)
                        .ne(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
        ) > 0;
    }

    private void markDocumentFailedIfRunning(String docId) {
        documentMapper.update(
                Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                        .set(KnowledgeDocumentDO::getStatus, DocumentStatus.FAILED.getCode())
                        .set(KnowledgeDocumentDO::getUpdatedBy, SYSTEM_USER)
                        .eq(KnowledgeDocumentDO::getId, docId)
                        .eq(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
        );
    }

    private void applyRefreshedFileMetadata(String docId, StoredFileDTO stored) {
        KnowledgeDocumentDO update = KnowledgeDocumentDO.builder()
                .id(docId)
                .docName(stored.getOriginalFilename())
                .fileUrl(stored.getUrl())
                .fileType(stored.getDetectedType())
                .fileSize(stored.getSize())
                .updatedBy(SYSTEM_USER)
                .build();
        int updated = documentMapper.updateById(update);
        if (updated == 0) {
            throw new ClientException("文档不存在");
        }
    }

    private void deleteOldFileQuietly(String oldFileUrl, String newFileUrl) {
        if (!StringUtils.hasText(oldFileUrl) || oldFileUrl.equals(newFileUrl)) {
            return;
        }
        try {
            fileStorageService.deleteByUrl(oldFileUrl);
        } catch (Exception e) {
            log.warn("定时刷新文件清理失败: {}", oldFileUrl, e);
        }
    }

    private String resolveInstanceId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        return "kb-schedule-" + host + "-" + UUID.randomUUID();
    }

    private String truncate(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        String trimmed = value.trim();
        if (trimmed.length() <= 512) {
            return trimmed;
        }
        return trimmed.substring(0, 512);
    }
}
