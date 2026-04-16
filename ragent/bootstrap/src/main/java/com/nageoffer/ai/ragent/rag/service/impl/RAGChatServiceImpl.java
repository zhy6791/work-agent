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

package com.nageoffer.ai.ragent.rag.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.nageoffer.ai.ragent.framework.context.UserContext;
import com.nageoffer.ai.ragent.framework.convention.ChatMessage;
import com.nageoffer.ai.ragent.framework.convention.ChatRequest;
import com.nageoffer.ai.ragent.framework.trace.RagTraceContext;
import com.nageoffer.ai.ragent.infra.chat.LLMService;
import com.nageoffer.ai.ragent.infra.chat.StreamCallback;
import com.nageoffer.ai.ragent.infra.chat.StreamCancellationHandle;
import com.nageoffer.ai.ragent.rag.aop.ChatRateLimit;
import com.nageoffer.ai.ragent.rag.core.guidance.GuidanceDecision;
import com.nageoffer.ai.ragent.rag.core.guidance.IntentGuidanceService;
import com.nageoffer.ai.ragent.rag.core.intent.IntentResolver;
import com.nageoffer.ai.ragent.rag.core.memory.ConversationMemoryService;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptContext;
import com.nageoffer.ai.ragent.rag.core.prompt.PromptTemplateLoader;
import com.nageoffer.ai.ragent.rag.core.prompt.RAGPromptService;
import com.nageoffer.ai.ragent.rag.core.retrieve.RetrievalEngine;
import com.nageoffer.ai.ragent.rag.core.rewrite.QueryRewriteService;
import com.nageoffer.ai.ragent.rag.core.rewrite.RewriteResult;
import com.nageoffer.ai.ragent.rag.dto.IntentGroup;
import com.nageoffer.ai.ragent.rag.dto.RetrievalContext;
import com.nageoffer.ai.ragent.rag.dto.SubQuestionIntent;
import com.nageoffer.ai.ragent.rag.service.RAGChatService;
import com.nageoffer.ai.ragent.rag.service.handler.StreamCallbackFactory;
import com.nageoffer.ai.ragent.rag.service.handler.StreamTaskManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.ArrayList;
import java.util.List;

import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.CHAT_SYSTEM_PROMPT_PATH;
import static com.nageoffer.ai.ragent.rag.constant.RAGConstant.DEFAULT_TOP_K;

/**
 * RAG 对话服务默认实现
 * <p>
 * 核心流程：
 * 记忆加载 -> 改写拆分 -> 意图解析 -> 歧义引导 -> 检索(MCP+KB) -> Prompt 组装 -> 流式输出
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RAGChatServiceImpl implements RAGChatService {

    private final LLMService llmService;
    private final RAGPromptService promptBuilder;
    private final PromptTemplateLoader promptTemplateLoader;
    private final ConversationMemoryService memoryService;
    private final StreamTaskManager taskManager;
    private final IntentGuidanceService guidanceService;
    private final StreamCallbackFactory callbackFactory;
    private final QueryRewriteService queryRewriteService;
    private final IntentResolver intentResolver;
    private final RetrievalEngine retrievalEngine;

    @Override
    @ChatRateLimit
    public void streamChat(String question, String conversationId, Boolean deepThinking, SseEmitter emitter) {
        //1、准备参数和用于处理聊天事件流的回调对象
        String actualConversationId = StrUtil.isBlank(conversationId) ? IdUtil.getSnowflakeNextIdStr() :conversationId;
        String taskId = StrUtil.isBlank(RagTraceContext.getTaskId()) ? IdUtil.getSnowflakeNextIdStr(): 			           RagTraceContext.getTaskId();
        log.info("开始流式对话，会话ID：{}，任务ID：{}", actualConversationId, taskId);
        boolean thinkingEnabled = Boolean.TRUE.equals(deepThinking);
        StreamCallback callback = callbackFactory.createChatEventHandler(emitter, actualConversationId, taskId);
        String userId = UserContext.getUserId();
        //2、加载并更新聊天历史记录
        List<ChatMessage> history=memoryService.loadAndAppend(actualConversationId, userId,ChatMessage.user(question));
        //3、查询优化器，通过语义改写提升检索准确性，支持多问句拆分实现并行检索，并配有术语归一化和规则拆分双重兜底机制。
        RewriteResult rewriteResult = queryRewriteService.rewriteWithSplit(question, history);
        log.info("查询优化器 问题改写结果：{}", rewriteResult);
        //4、语义分类器，将用户问题与意图节点树进行语义匹配，由 LLM 判断属于哪个知识库，为后续的定向检索提供路由依据。
        List<SubQuestionIntent> subIntents = intentResolver.resolve(rewriteResult);
        log.info("语义分类器 子意图：{}", subIntents);
        //5、分析重写后的问题和子意图，判断是否存在歧义或需要澄清
        GuidanceDecision guidanceDecision = guidanceService.detectAmbiguity(rewriteResult.rewrittenQuestion(), subIntents);
        //5.1、当问题模糊时，直接返回澄清提示并终止流程，让用户补充信息
        if (guidanceDecision.isPrompt()) {
            //接收 LLM 返回的增量内容，通过 SSE 推送给前端，实现打字机效果，同时累积到 StringBuilder answer 中，用于后续落库
            callback.onContent(guidanceDecision.getPrompt());
            //将 AI 回答保存到聊天记录； 自动为新会话生成标题（如"新对话"）；发送 FINISH 和 DONE 事件，前端停止 loading
            callback.onComplete();
            return;
        }
        //6、检查每个子意图的节点评分，判断是否都是系统级别的操作
        boolean allSystemOnly = subIntents.stream()
                .allMatch(si -> intentResolver.isSystemOnly(si.nodeScores()));
        //6.1、当问题只需系统内部处理时，直接使用预设 Prompt 生成回复，无需检索外部知识库
        if (allSystemOnly) {
            //从匹配的节点中获取最优先的 Prompt 模板用于系统回复
            String customPrompt = subIntents.stream().flatMap(si -> si.nodeScores().stream())
                    .map(ns -> ns.getNode().getPromptTemplate()) .filter(StrUtil::isNotBlank).findFirst().orElse(null);
            StreamCancellationHandle handle = streamSystemResponse(rewriteResult.rewrittenQuestion(), history,   customPrompt, callback);
            taskManager.bindHandle(taskId, handle);
            return;
        }
        //7、智能多路检索系统，根据意图识别结果动态选择检索策略：
        RetrievalContext ctx = retrievalEngine.retrieve(subIntents, DEFAULT_TOP_K);
        //7.1、是当知识库检索失败时，返回友好的空结果提示。
        if (ctx.isEmpty()) {
            String emptyReply = "未检索到与问题相关的文档内容。";
            callback.onContent(emptyReply);
            callback.onComplete();
            return;
        }
        //8、 整合所有子意图为一个整体，优化检索和回答的生成流程。
        IntentGroup mergedGroup = intentResolver.mergeIntentGroup(subIntents);
        //9、调用 streamLLMResponse() 方法，使用检索到的上下文生成 AI 回答
        StreamCancellationHandle handle = streamLLMResponse(
                rewriteResult, //改写后的用户问题
                ctx, //检索到的知识库和 MCP 上下文
                mergedGroup, //合并后的意图组：一个封装了所有子问题意图的数据结构，用于整合多个子问题的意图识别结果。
                history, //history：聊天历史记录
                thinkingEnabled, //thinkingEnabled：是否启用思考过程展示
                callback //callback：流式回调处理器（用于实时推送内容）
        );
        //10、任务管理：将返回的句柄（handle）与任务 ID 绑定，支持后续取消操作
        taskManager.bindHandle(taskId, handle);
    }

    @Override
    public void stopTask(String taskId) {
        taskManager.cancel(taskId);
    }

    // ==================== LLM 响应 ====================

    private StreamCancellationHandle streamSystemResponse(String question, List<ChatMessage> history,
                                                          String customPrompt, StreamCallback callback) {
        String systemPrompt = StrUtil.isNotBlank(customPrompt)
                ? customPrompt
                : promptTemplateLoader.load(CHAT_SYSTEM_PROMPT_PATH);

        List<ChatMessage> messages = new ArrayList<>();
        messages.add(ChatMessage.system(systemPrompt));
        if (CollUtil.isNotEmpty(history)) {
            messages.addAll(history.subList(0, history.size() - 1));
        }
        messages.add(ChatMessage.user(question));

        ChatRequest req = ChatRequest.builder()
                .messages(messages)
                .temperature(0.7D)
                .thinking(false)
                .build();
        return llmService.streamChat(req, callback);
    }

    private StreamCancellationHandle streamLLMResponse(RewriteResult rewriteResult, RetrievalContext ctx,
                                                       IntentGroup intentGroup, List<ChatMessage> history,
                                                       boolean deepThinking, StreamCallback callback) {
        PromptContext promptContext = PromptContext.builder()
                .question(rewriteResult.rewrittenQuestion())
                .mcpContext(ctx.getMcpContext())
                .kbContext(ctx.getKbContext())
                .mcpIntents(intentGroup.mcpIntents())
                .kbIntents(intentGroup.kbIntents())
                .intentChunks(ctx.getIntentChunks())
                .build();

        List<ChatMessage> messages = promptBuilder.buildStructuredMessages(
                promptContext,
                history,
                rewriteResult.rewrittenQuestion(),
                rewriteResult.subQuestions()  // 传入子问题列表
        );
        ChatRequest chatRequest = ChatRequest.builder()
                .messages(messages)
                .thinking(deepThinking)
                .temperature(ctx.hasMcp() ? 0.3D : 0D)  // MCP 场景稍微放宽温度
                .topP(ctx.hasMcp() ? 0.8D : 1D)
                .build();

        return llmService.streamChat(chatRequest, callback);
    }
}
