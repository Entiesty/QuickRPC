# QuickRPC — 轻量级高性能分布式 RPC 框架

## 项目简介

**QuickRPC** 是一款为深入理解分布式系统通信原理而从零自主研发的高并发、轻量级 Java RPC 框架。

框架采用声明式的调用体验，彻底屏蔽底层网络通信细节。在架构设计上，QuickRPC 深入挖掘了底层网络性能与分布式高可用性。核心通信层基于 Netty 构建，采用自定义高性能变长二进制协议解决 TCP 粘包/拆包问题；服务治理层深度整合 Apache Curator 与 Zookeeper，实现了具备高一致性的服务动态注册与发现。

历经多个核心版本的架构演进，QuickRPC 现已具备生产环境所需的完整分布式协调能力。不仅实现了基于 SPI 的高扩展序列化机制（涵盖 Protostuff、Kryo、Hessian 等极致性能方案），更在节点路由与容错方面，内置了一致性哈希负载均衡、无锁化轮询、三态熔断降级、Guava 幂等重试、严格的令牌桶限流保护，以及基于 Zipkin 的全链路追踪系统。

QuickRPC 架构清晰，职责解耦，天然契合高并发与灾备场景的需求，为服务调用链的性能分析与故障排查提供了强大的可视化支持。

## 版本演进

| 版本 | 核心特性 |
| --- | --- |
| v1.1 | Java 原生 Socket + BIO + 反射 |
| v1.2 | Netty NIO 替代原生 Socket |
| v1.3 | 引入 Zookeeper 作为注册中心 |
| v2.1 | 自定义 Netty 编解码器 + 序列化器 |
| v2.2 | 客户端本地服务缓存 + ZK Watcher 动态更新 |
| v3.1 | 负载均衡（随机 / 轮询 / 一致性哈希） |
| v3.2 | 超时重试 + 白名单机制 |
| v4.1 | 令牌桶限流算法 |
| v4.2 | 三态熔断器 |
| v5.0 | SPI 可插拔机制 + 配置自动化 + Spring Boot 集成 + **Zipkin 全链路追踪** |

## 技术栈

| 组件 | 技术选型 | 版本 |
| --- | --- | --- |
| 编程语言 | Java | 21 |
| 网络通信 | Netty | 4.1.51.Final |
| 服务注册与发现 | Apache Curator + Zookeeper | 5.1.0 |
| JSON 序列化 | FastJSON | 1.2.83 |
| 二进制序列化 | Kryo | 4.0.2 |
| 跨语言序列化 | Hessian | 4.0.66 |
| 极致性能序列化 | Protostuff | 1.7.4 |
| 重试框架 | Guava Retrying | 2.0.0 |
| 链路追踪 | Zipkin | — |
| 配置加载 | Hutool Props | 5.8.10 |
| IOC 容器 | Spring Boot | 3.3.5 |

## 项目模块结构

```text
QuickRPC/
├── quickrpc-api/          # API 模块：服务接口定义、POJO、注解（@Retryable）
├── quickrpc-common/       # 公共模块：消息协议、序列化器、编解码器、SPI、配置工具
├── quickrpc-core/         # 核心框架：动态代理、Netty 通信、ZK 注册与发现、熔断限流、链路追踪
├── quickrpc-consumer/     # 服务消费者示例
└── quickrpc-provider/     # 服务提供者示例

```

---

## 一、Zookeeper 服务注册与发现

### 1.1 注册中心架构

使用 Apache Curator 作为 Zookeeper 客户端，连接地址为 `127.0.0.1:2181`，session 超时 40 秒，采用 ExponentialBackoffRetry 指数退避重试策略。所有 ZK 操作统一在 `/QuickRPC` 命名空间下进行。

### 1.2 Zookeeper 目录结构

```text
/QuickRPC
  /com.kama.service.UserService              (PERSISTENT 持久节点)
    /192.168.1.10:9999                       (EPHEMERAL 临时节点)
    /192.168.1.11:9999                       (EPHEMERAL 临时节点)
/CanRetry
  /192.168.1.10:9999
    /com.kama.service.UserService#getUserByUserId(java.lang.Integer)  (EPHEMERAL)

```

### 1.3 客户端发现与本地缓存

**两级缓存机制**：

* **一级缓存**：`ServiceCache`（ConcurrentHashMap），按服务名存储地址列表。
* **二级回源**：本地缓存未命中时，向 ZK 发起 `getChildren()` 查询并回填缓存。

**ZK Watcher 实时监听**：通过 CuratorCache 监听器订阅根路径的节点变更事件，实时同步更新本地 ServiceCache，确保客户端路由信息与服务端实例列表强一致。

---

## 二、动态代理与调用链路

客户端通过 JDK 动态代理（`Proxy.newProxyInstance()`）为目标接口生成透明代理对象。

**核心调用链路：**

1. 构建 RpcRequest。
2. 从 `CircuitBreakerProvider` 获取熔断器，执行前置拦截检查。
3. `ZKServiceCenter` 执行服务发现与负载均衡。
4. 创建 `NettyRpcClient`，连接目标节点。
5. 从 ZK 拉取白名单，命中则进入 `GuavaRetry` 容错调用，否则执行单次调用。
6. 根据响应状态码上报熔断器健康度统计。
7. 返回真实业务数据。

---

## 三、底层网络传输机制

### 3.1 自定义二进制协议

自定义了一套定长头部 + 变长负载的二进制通信协议，总头部固定 8 字节，有效解决 TCP 粘包/拆包问题：

```text
| messageType (2B) | serializerType (2B) | dataLength (4B) | serializedPayload (dataLength B) |

```

* **messageType**：0 = REQUEST，1 = RESPONSE
* **serializerType**：0=JDK, 1=FastJSON, 2=Kryo, 3=Hessian, 4=Protostuff
* **dataLength**：序列化后的消息体长度。

### 3.2 高性能 Netty 线程模型

* **服务端**：采用 Boss-Worker 线程模型（`NioServerSocketChannel`），并在 Handler 中前置接入令牌桶限流器，拒绝突发超载流量。
* **客户端**：静态共享 `NioEventLoopGroup` 以降低线程开销，利用 Channel 的 `AttributeKey<RpcResponse>` 机制实现跨异步线程的请求-响应安全隔离与同步等待。

---

## 四、序列化与 SPI 可插拔机制

框架内置 5 种序列化方案，并通过自研的 `SpiLoader` 实现了核心组件的可插拔设计：

* **配置路径**：`META-INF/serializer/com.kama.common.serializer.myserializer.Serializer`
* **加载机制**：懒加载 + 实例缓存（ConcurrentHashMap 双层缓存），按需反射实例化并复用单例对象。

---

## 五、负载均衡策略

提供三种实现，支持运行时动态增删节点路由：

1. **随机 (Random)**：基于 `CopyOnWriteArrayList` 线程安全随机选取。
2. **轮询 (RoundRobin)**：基于 `AtomicInteger` 的无锁化轮询计数器。
3. **一致性哈希 (Consistent Hash)**：采用 FNV1_32_HASH 算法，维护虚拟节点（每个真实节点映射 5 个虚拟节点）于 `TreeMap` 哈希环上，大幅降低节点上下线造成的缓存失效抖动。

---

## 六、高可用治理：熔断与限流

### 6.1 三态熔断器 (Circuit Breaker)

实现标准的 CLOSED -> OPEN -> HALF_OPEN 状态机流转：

* **CLOSED**：请求正常放行，失败达到阈值即触发熔断。
* **OPEN**：熔断生效，快速失败拒绝请求。等待 `retryTimePeriod`（默认 10s）后进入半开状态。
* **HALF_OPEN**：放行探测请求，成功率达标（如 50%）则恢复闭合，否则重新熔断。

### 6.2 令牌桶限流 (Token Bucket Rate Limiting)

基于时间戳与计算差值的无锁化/轻量级令牌桶实现：

* 高并发下通过 `volatile` 保证容量和时间戳可见性。
* 每次请求动态计算 `(currentTimestamp - lastTimestamp) / rate` 以生成补充令牌，若获取失败，服务端底层通道直接掐断拦截，保护业务线程池。

---

## 七、超时重试 (Guava-Retrying)

基于 `@Retryable` 注解与 Zookeeper `/CanRetry` 路径，实现安全的幂等方法重试：

* **框架集成**：Guava Retryer
* **触发条件**：网络异常或业务响应码为 500。
* **策略**：固定 2 秒退避等待，最大重试 3 次。严格的白名单过滤机制，防止对写操作造成脏数据。

---

## 八、全链路追踪与可观测性 (Zipkin)

为解决微服务架构下的复杂调用链排障问题，QuickRPC 深度集成了 Zipkin 分布式链路追踪：

* **TraceId 传递机制**：在自定义 `RpcRequest` 协议体中扩展了隐式传参（Attachments）结构。
* **跨异步线程透传**：针对 Netty 异步线程池与本地业务线程的上下文切换，通过深度定制和拦截机制，保证 `TraceId` 和 `SpanId` 能够在多线程并发调度时准确传播，避免上下文丢失。
* **可视化监控**：调用链耗时、网络延迟及报错节点等信息实时上报至 Zipkin Server，提供完整的拓扑图与性能瓶颈定位分析。

---

## 九、快速开始

### 1. 启动依赖组件

确保本地 Zookeeper (`127.0.0.1:2181`) 与 Zipkin Server 正常运行。

### 2. 启动服务提供者

```java
KRpcApplication.initialize();                          // 自动化加载 application.properties 配置
ServiceProvider serviceProvider = new ServiceProvider();
serviceProvider.provideService(new UserServiceImpl()); // 将业务服务注册到本地与 ZK
RpcServer rpcServer = new NettyRpcServer(serviceProvider);
rpcServer.start(9999);                                 // 暴露服务并启动 Netty

```

### 3. 启动服务消费者

```java
ClientProxy clientProxy = new ClientProxy();
UserService userService = clientProxy.getProxy(UserService.class); // 透明获取动态代理
User user = userService.getUserByUserId(1);                        // 发起远程调用

```

---

## 十、架构全景图

```text
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                            QuickRPC Consumer                            │
 │                                                                         │
 │  UserService proxy = ClientProxy.getProxy(UserService.class)            │
 │  proxy.getUserByUserId(1) ──── JDK 动态代理 ────┐                       │
 │                                                │                        │
 │  ┌─────────────────────────────────────────────┘                        │
 │  │                                                                      │
 │  ▼                                                                      │
 │  ClientProxy.invoke()                                                   │
 │  ├─ 构建 RpcRequest (注入 Zipkin TraceId/SpanId)                        │
 │  ├─ CircuitBreaker.allowRequest() ──── 熔断前置拦截                     │
 │  ├─ ZKServiceCenter.serviceDiscovery() ── ZK 发现 + 一致性哈希路由      │
 │  ├─ checkRetry() ── 拉取幂等白名单                                      │
 │  ├─ NettyRpcClient.sendRequest()                                        │
 │  │   └─ Pipeline: Encoder → Decoder → Handler (跨线程响应同步)          │
 │  └─ 响应码上报熔断状态机                                                │
 └───────────────────────┬─────────────────────────────────────────────────┘
                         │ 自定义二进制协议 (定长 Header + 变长 Payload)
                         │ Netty NIO 高性能传输
                         ▼
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                            QuickRPC Provider                            │
 │                                                                         │
 │  NettyRpcServer (BossGroup + WorkerGroup)                               │
 │  ├─ Pipeline: Encoder → Decoder → Handler                               │
 │  └─ NettyRpcServerHandler                                               │
 │       ├─ TokenBucketRateLimitImpl.getToken() ── 令牌桶防刷限流          │
 │       ├─ 提取 TraceId 并上报 Zipkin Span                                │
 │       └─ method.invoke(service, params) ────── 反射调用本地实例         │
 │                                                                         │
 │  ZKServiceRegister.register()                                           │
 │  └─ /QuickRPC/{服务名}/{host:port} (EPHEMERAL) ── 注册心跳节点          │
 │  └─ /CanRetry/{host:port}/{方法签名} (EPHEMERAL) ── 注册幂等白名单      │
 └───────────────────────┬─────────────────────────────────────────────────┘
                         │
           ┌─────────────┴─────────────┐
           ▼                           ▼
 ┌───────────────────┐       ┌───────────────────┐
 │     Zookeeper     │       │      Zipkin       │
 │  127.0.0.1:2181   │       │  Trace 链路分析台  │
 └───────────────────┘       └───────────────────┘

```
