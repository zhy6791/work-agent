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

package com.nageoffer.ai.ragent.rag.core.memory;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.nageoffer.ai.ragent.rag.config.MemoryProperties;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationMessageDO;
import com.nageoffer.ai.ragent.rag.dao.entity.ConversationSummaryDO;
import com.nageoffer.ai.ragent.framework.convention.ChatMessage;
import com.nageoffer.ai.ragent.framework.convention.ChatRequest;
import com.nageoffer.ai.ragent.infra.chat.LLMService;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptTemplateLoader;
import com.nageoffer.ai.ragent.rag.service.ConversationGroupService;
import com.nageoffer.ai.ragent.rag.service.ConversationMessageService;
import com.nageoffer.ai.ragent.rag.service.bo.ConversationSummaryBO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.CONVERSATION_SUMMARY_PROMPT_PATH;

@Slf4j
@Service
@RequiredArgsConstructor
public class JdbcConversationMemorySummaryService implements ConversationMemorySummaryService {

    private static final String SUMMARY_PREFIX = "对话摘要：";
    private static final String SUMMARY_LOCK_PREFIX = "ragent:memory:summary:lock:";
    private static final Duration SUMMARY_LOCK_TTL = Duration.ofMinutes(5);

    private final ConversationGroupService conversationGroupService;
    private final ConversationMessageService conversationMessageService;
    private final MemoryProperties memoryProperties;
    private final LLMService llmService;
    private final PromptTemplateLoader promptTemplateLoader;
    private final RedissonClient redissonClient;

    @Qualifier("memorySummaryThreadPoolExecutor")
    private final Executor memorySummaryExecutor;

    @Override
    public void compressIfNeeded(String conversationId, String userId, ChatMessage message) {
        if (!memoryProperties.getSummaryEnabled()) {
            return;
        }
        //助手回复标志着一轮对话的完成（用户提问 → 助手回答）
        if (message.getRole() != ChatMessage.Role.ASSISTANT) {
            return;
        }
        CompletableFuture.runAsync(() -> doCompressIfNeeded(conversationId, userId), memorySummaryExecutor)
                .exceptionally(ex -> {
                    log.error("对话记忆摘要异步任务失败 - conversationId: {}, userId: {}",
                            conversationId, userId, ex);
                    return null;
                });
    }

    @Override
    public ChatMessage loadLatestSummary(String conversationId, String userId) {
        ConversationSummaryDO summary = conversationGroupService.findLatestSummary(conversationId, userId);
        return toChatMessage(summary);
    }

    @Override
    public ChatMessage decorateIfNeeded(ChatMessage summary) {
        if (summary == null || StrUtil.isBlank(summary.getContent())) {
            return summary;
        }

        String content = summary.getContent().trim();
        //检查摘要内容是否已包含前缀标记。如果内容已以 SUMMARY_PREFIX 或"摘要："开头，说明已经处理过，直接返回原对象；否则将其包装为系统消息并添加前缀标记，确保摘要格式统一。
        if (content.startsWith(SUMMARY_PREFIX) || content.startsWith("摘要：")) {
            return summary;
        }
        return ChatMessage.system(SUMMARY_PREFIX + content);
    }



    private void doCompressIfNeeded(String conversationId, String userId) {
        // 1. 记录摘要操作开始时间
        long startTime = System.currentTimeMillis();
        // 2. 获取配置的摘要触发轮数阈值 触发条件：对话达到多少轮后才开始摘要
        int triggerTurns = memoryProperties.getSummaryStartTurns();
        // 3. 获取配置的历史消息保留轮数 摘要范围：保留最近多少轮不摘要
        int maxTurns = memoryProperties.getHistoryKeepTurns();
        // 4. 参数校验，无效则直接返回
        if (maxTurns <= 0 || triggerTurns <= 0) {
            return;
        }
        // 5. 构建分布式锁键并获取锁
        String lockKey = SUMMARY_LOCK_PREFIX + buildLockKey(conversationId, userId);
        RLock lock = redissonClient.getLock(lockKey);
        // 6. 尝试获取锁，失败则直接返回
        if (!tryLock(lock)) {
            return;
        }
        try {
            // 7. 统计当前会话的总消息轮数
            long total = conversationGroupService.countUserMessages(conversationId, userId);
            // 8. 检查是否达到触发轮数阈值
            if (total < triggerTurns) {
                return;
            }

            // 9. 查询最新的摘要记录
            ConversationSummaryDO latestSummary = conversationGroupService.findLatestSummary(conversationId, userId);
            // 10. 获取用户最近的若干条纯用户消息（maxTurns 条），用于确定需要摘要的消息范围
            List<ConversationMessageDO> latestUserTurns = conversationGroupService.listLatestUserOnlyMessages(
                    conversationId,
                    userId,
                    maxTurns
            );
            // 11. 空值检查
            if (latestUserTurns.isEmpty()) {
                return;
            }
            // 12. 从最新的用户消息列表中计算出截止消息 ID（即需要摘要的消息范围的上限）
            String cutoffId = resolveCutoffId(latestUserTurns);
            // 13. 空值检查
            if (StrUtil.isBlank(cutoffId)) {
                return;
            }
            // 14. 获取摘要起始位置 ID（从上次摘要结束的位置开始）
            String afterId = resolveSummaryStartId(conversationId, userId, latestSummary);
            // 15. 检查是否需要更新摘要
            if (afterId != null && Long.parseLong(afterId) >= Long.parseLong(cutoffId)) {
                return;
            }

            // 16. 获取需要摘要的消息列表
            List<ConversationMessageDO> toSummarize = conversationGroupService.listMessagesBetweenIds(
                    conversationId,
                    userId,
                    afterId,
                    cutoffId
            );
            // 17. 空值检查
            if (CollUtil.isEmpty(toSummarize)) {
                return;
            }

            // 18. 解析最后一条消息的 ID
            String lastMessageId = resolveLastMessageId(toSummarize);
            // 19. 空值检查
            if (StrUtil.isBlank(lastMessageId)) {
                return;
            }

            // 20. 获取已存在的摘要内容
            String existingSummary = latestSummary == null ? "" : latestSummary.getContent();
        /*
        *  基于 LLM 的智能摘要合并机制，而非传统的字符串拼接。它通过精心设计的 Prompt 模板，让大模型理解：
            哪些信息需要保留（话题、状态、约束条件）
            哪些信息需要忽略（具体答案、详细步骤）
            如何合并新旧内容（去重、更新、冲突解决）
            最终生成的摘要是语义层面的压缩，而非简单的文本截断。
        * */
            // 21. 调用 LLM 生成智能摘要
            String summary = summarizeMessages(toSummarize, existingSummary);
            // 22. 空值检查
            if (StrUtil.isBlank(summary)) {
                return;
            }

            // 23. 创建并保存摘要记录
            createSummary(conversationId, userId, summary, lastMessageId);
            // 24. 记录摘要成功日志
            log.info("摘要成功 - conversationId：{}，userId：{}，消息数：{}，耗时：{}ms",
                    conversationId, userId, toSummarize.size(),
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            // 25. 记录摘要异常日志
            log.error("摘要失败 - conversationId：{}，userId：{}", conversationId, userId, e);
        } finally {
            // 26. 释放分布式锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private boolean tryLock(RLock lock) {
        try {
            return lock.tryLock(0, SUMMARY_LOCK_TTL.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private String summarizeMessages(List<ConversationMessageDO> messages, String existingSummary) {
        // ConversationMessageDO（数据库实体）转为 ChatMessage（大模型对话格式），过滤掉空值和无效数据，并统一角色标识（user/assistant）。
        List<ChatMessage> histories = toHistoryMessages(messages);
        if (CollUtil.isEmpty(histories)) {
            return existingSummary;
        }

        int summaryMaxChars = memoryProperties.getSummaryMaxChars();
        List<ChatMessage> summaryMessages = new ArrayList<>();
        String summaryPrompt = promptTemplateLoader.render(
                CONVERSATION_SUMMARY_PROMPT_PATH,
                Map.of("summary_max_chars", String.valueOf(summaryMaxChars))
        );

        //将摘要提示词包装为系统消息并添加到消息列表中。
        summaryMessages.add(ChatMessage.system(summaryPrompt));

        //将现有摘要添加到消息列表中供 LLM 参考,其包装为助手消息（assistant role）
        if (StrUtil.isNotBlank(existingSummary)) {
            summaryMessages.add(ChatMessage.assistant(
                    "历史摘要（仅用于合并去重，不得作为事实新增来源；若与本轮对话冲突，以本轮对话为准）：\n"
                            + existingSummary.trim()
            ));
        }

        summaryMessages.addAll(histories);
        summaryMessages.add(ChatMessage.user(
                "合并以上对话与历史摘要，去重后输出更新摘要。要求：严格≤" + summaryMaxChars + "字符；仅一行。"
        ));

        //设置消息列表（包含系统指令、历史摘要、新对话）配置生成参数：temperature=0.3（低随机性，保证稳定性）、topP=0.9（核采样阈值）、关闭深度思考
        ChatRequest request = ChatRequest.builder()
                .messages(summaryMessages)
                .temperature(0.3D)
                .topP(0.9D)
                .thinking(false)
                .build();
        try {
            String result = llmService.chat(request);
            log.info("对话摘要生成 - resultChars: {}", result.length());

            return result;
        } catch (Exception e) {
            log.error("对话记忆摘要生成失败, conversationId相关消息数: {}", messages.size(), e);
            return existingSummary;
        }
    }

    private List<ChatMessage> toHistoryMessages(List<ConversationMessageDO> messages) {
        if (CollUtil.isEmpty(messages)) {
            return List.of();
        }
        return messages.stream()
                .filter(item -> item != null
                        && StrUtil.isNotBlank(item.getContent())
                        && StrUtil.isNotBlank(item.getRole()))
                .map(item -> {
                    String role = item.getRole().toLowerCase();
                    if ("user".equals(role)) {
                        return ChatMessage.user(item.getContent());
                    } else if ("assistant".equals(role)) {
                        return ChatMessage.assistant(item.getContent());
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private ChatMessage toChatMessage(ConversationSummaryDO record) {
        if (record == null || StrUtil.isBlank(record.getContent())) {
            return null;
        }
        return new ChatMessage(ChatMessage.Role.SYSTEM, record.getContent());
    }

    /*
    * 确定摘要的起始位置 ID，优先级如下：
        如果摘要记录为空，返回 null（从头开始）
        如果摘要记录了最后一条消息 ID，直接返回该 ID
        否则使用摘要的更新时间（或创建时间）查找对应时间点之前的最大消息 ID
    * */
    private String resolveSummaryStartId(String conversationId, String userId, ConversationSummaryDO summary) {
        if (summary == null) {
            return null;
        }
        if (summary.getLastMessageId() != null) {
            return summary.getLastMessageId();
        }

        Date after = summary.getUpdateTime();
        if (after == null) {
            after = summary.getCreateTime();
        }
        return conversationGroupService.findMaxMessageIdAtOrBefore(conversationId, userId, after);
    }

    private String resolveCutoffId(List<ConversationMessageDO> latestUserTurns) {
        if (CollUtil.isEmpty(latestUserTurns)) {
            return null;
        }

        // 倒序列表的最后一个就是最早的
        ConversationMessageDO oldest = latestUserTurns.get(latestUserTurns.size() - 1);
        return oldest == null ? null : oldest.getId();
    }

    private String resolveLastMessageId(List<ConversationMessageDO> toSummarize) {
        for (int i = toSummarize.size() - 1; i >= 0; i--) {
            ConversationMessageDO item = toSummarize.get(i);
            if (item != null && item.getId() != null) {
                return item.getId();
            }
        }
        return null;
    }

    private void createSummary(String conversationId,
                               String userId,
                               String content,
                               String lastMessageId) {
        ConversationSummaryBO summaryRecord = ConversationSummaryBO.builder()
                .conversationId(conversationId)
                .userId(userId)
                .content(content)
                .lastMessageId(lastMessageId)
                .build();
        conversationMessageService.addMessageSummary(summaryRecord);
    }

    private String buildLockKey(String conversationId, String userId) {
        return userId.trim() + ":" + conversationId.trim();
    }
}
