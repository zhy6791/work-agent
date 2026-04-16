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

package com.nageoffer.ai.ragent.infra.rerank;

import com.nageoffer.ai.ragent.framework.convention.RetrievedChunk;
import com.nageoffer.ai.ragent.infra.enums.ModelCapability;
import com.nageoffer.ai.ragent.infra.model.ModelRoutingExecutor;
import com.nageoffer.ai.ragent.infra.model.ModelSelector;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 路由式重排服务实现类
 * <p>
 * 该服务通过模型路由机制动态选择合适的重排客户端，并支持失败降级策略
 * 作为主要的重排服务实现，用于对检索到的文档块进行相关性重新排序
 */
@Service
@Primary
public class RoutingRerankService implements RerankService {

    private final ModelSelector selector;
    private final ModelRoutingExecutor executor;
    private final Map<String, RerankClient> clientsByProvider;

    public RoutingRerankService(ModelSelector selector, ModelRoutingExecutor executor, List<RerankClient> clients) {
        this.selector = selector;
        this.executor = executor;
        this.clientsByProvider = clients.stream()
                .collect(Collectors.toMap(RerankClient::provider, Function.identity()));
    }

    @Override
    public List<RetrievedChunk> rerank(String query, List<RetrievedChunk> candidates, int topN) {
        //调用 ModelRoutingExecutor 的降级执行方法，当主模型失败时切换到备选模型
        return executor.executeWithFallback(
                //执行器会根据这个类型筛选可用的模型
                ModelCapability.RERANK,
                //候选模型选择
                selector.selectRerankCandidates(),
                //客户端获取函数
                target
                        -> clientsByProvider.get(target.candidate().getProvider()),
                //接收选中的客户端和目标配置，调用具体客户端的 rerank 方法，将查询、候选集、返回数量和路由目标传递给底层客户端实现
                (client, target)
                        -> client.rerank(query, candidates, topN, target)
        );
    }
}
