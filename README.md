# KRPC — 轻量级 Java RPC 框架

## 项目简介

KRPC（Kama RPC）是一个从零构建的轻量级远程过程调用（RPC）框架，完整实现了 RPC 通信的核心机制，包括服务注册与发现、动态代理、自定义网络传输协议、序列化与反序列化、负载均衡、熔断降级、限流保护、超时重试等关键模块。项目历经 5 个版本迭代演进，从最初的 Java 原生 Socket 通信逐步发展为基于 Netty + Zookeeper + SPI 的成熟 RPC 框架，并集成了 Spring Boot 3.3.5。

## 版本演进

| 版本 | 核心特性 |
|------|---------|
| v1.1 | Java 原生 Socket + BIO + 反射 |
| v1.2 | Netty NIO 替代原生 Socket |
| v1.3 | 引入 Zookeeper 作为注册中心 |
| v2.1 | 自定义 Netty 编解码器 + 序列化器 |
| v2.2 | 客户端本地服务缓存 + ZK Watcher 动态更新 |
| v3.1 | 负载均衡（随机 / 轮询 / 一致性哈希） |
| v3.2 | 超时重试 + 白名单机制 |
| v4.1 | 令牌桶限流算法 |
| v4.2 | 三态熔断器 |
| v5.0 | SPI 可插拔机制 + 5 种序列化器 + 配置自动化 + Spring Boot 集成 |

## 技术栈

| 组件 | 技术选型 | 版本 |
|------|---------|------|
| 编程语言 | Java | 21 |
| 网络通信 | Netty | 4.1.51.Final |
| 服务注册与发现 | Apache Curator + Zookeeper | 5.1.0 |
| JSON 序列化 | FastJSON | 1.2.83 |
| 二进制序列化 | Kryo | 4.0.2 |
| 跨语言序列化 | Hessian | 4.0.66 |
| 极致性能序列化 | Protostuff | 1.7.4 |
| 重试框架 | Guava Retryer | 2.0.0 |
| 配置加载 | Hutool Props | 5.8.10 |
| IOC 容器 | Spring Boot | 3.3.5 |
| 代码简化 | Lombok | 1.18.30 |
| 构建工具 | Maven | — |

## 项目模块结构（version5）

```
version5/
├── krpc-api/          # API 模块：服务接口定义、POJO、注解（@Retryable）
├── krpc-common/       # 公共模块：消息协议、序列化器、编解码器、SPI、配置工具
├── krpc-core/         # 核心框架：动态代理、Netty 客户端/服务端、ZK 注册与发现、
│                      #   熔断器、限流器、负载均衡、重试
├── krpc-consumer/     # 服务消费者示例
└── krpc-provider/     # 服务提供者示例
```

---

## 一、Zookeeper 服务注册与发现

### 1.1 注册中心架构

使用 Apache Curator 作为 Zookeeper 客户端，连接地址为 `127.0.0.1:2181`，session 超时 40 秒，采用 ExponentialBackoffRetry 指数退避重试策略。所有 ZK 操作统一在 `/MyRPC` 命名空间下进行。

### 1.2 Zookeeper 目录结构

```
/MyRPC
  /com.kama.service.UserService              (PERSISTENT 持久节点)
    /192.168.1.10:9999                       (EPHEMERAL 临时节点)
    /192.168.1.11:9999                       (EPHEMERAL 临时节点)
/CanRetry
  /192.168.1.10:9999
    /com.kama.service.UserService#getUserByUserId(java.lang.Integer)  (EPHEMERAL)
```

### 1.3 服务端注册流程

`ZKServiceRegister.register()` 位于 `version5/krpc-core/.../server/serviceRegister/impl/ZKServiceRegister.java`：

1. 以接口全限定名（如 `com.kama.service.UserService`）作为服务名，在 ZK 中创建 PERSISTENT 持久根节点。
2. 在根节点下创建 EPHEMERAL 临时子节点，内容为 `{host}:{port}`。当服务进程断开连接时，临时节点自动删除，实现自动下线路由。
3. 扫描服务实现类中标记了 `@Retryable` 注解的方法，提取方法签名（格式：`接口名#方法名(参数类型列表)`），注册到 `/CanRetry/{host}:{port}/` 路径下，同样采用临时节点，供客户端拉取可重试方法白名单。

关键代码逻辑：

```java
// 创建持久根节点
if (client.checkExists().forPath("/" + serviceName) == null) {
    client.create().creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath("/" + serviceName);
}
// 创建临时地址节点
String path = "/" + serviceName + "/" + getServiceAddress(serviceAddress);
client.create().creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .forPath(path);
```

### 1.4 客户端发现与本地缓存

`ZKServiceCenter.serviceDiscovery()` 位于 `version5/krpc-core/.../client/servicecenter/ZKServiceCenter.java`：

**两级缓存机制**：
- 一级缓存：`ServiceCache`（ConcurrentHashMap），按服务名存储地址列表。
- 二级回源：本地缓存未命中时，向 ZK 发起 `getChildren()` 查询并回填缓存。

**ZK Watcher 实时监听**：`watchZK` 类通过 CuratorCache 监听器订阅 `/MyRPC` 根路径的节点变更事件（NODE_CREATED / NODE_CHANGED / NODE_DELETED），实时同步更新本地 ServiceCache，确保客户端路由信息与服务端实例列表强一致。

**发现流程**：
1. 从本地 ServiceCache 获取服务地址列表。
2. 若未命中，从 ZK 查询并回填。
3. 通过负载均衡策略从地址列表中选取一个目标地址。
4. 解析为 `InetSocketAddress` 返回。

### 1.5 重试白名单管理

客户端通过 `checkRetry()` 方法，从 ZK 的 `/CanRetry/{host}:{port}/` 路径拉取可重试方法签名列表，缓存至 `CopyOnWriteArraySet` 中，实现线程安全的高性能白名单查询。只有命中白名单的方法才会触发 Guava Retry 重试。

---

## 二、动态代理

`ClientProxy` 位于 `version5/krpc-core/.../client/proxy/ClientProxy.java`，是客户端的核心入口，实现了 `InvocationHandler` 接口，通过 JDK 动态代理（`Proxy.newProxyInstance()`）为目标接口生成透明代理对象。

### 2.1 代理调用链路

```
用户调用代理方法
  └─ ClientProxy.invoke()
       ├─ 1. 构建 RpcRequest（interfaceName / methodName / params / paramsType）
       ├─ 2. 从 CircuitBreakerProvider 获取熔断器，调用 allowRequest() 前置检查
       │     ├─ OPEN 状态：判断是否到达恢复时间 → HALF_OPEN 或直接拒绝
       │     ├─ HALF_OPEN 状态：请求计数 +1，放行
       │     └─ CLOSED 状态：放行
       ├─ 3. ZKServiceCenter.serviceDiscovery() 服务发现 + 负载均衡
       ├─ 4. 创建 NettyRpcClient，连接到目标地址
       ├─ 5. checkRetry() 查询重试白名单
       │     ├─ 命中白名单 → GuavaRetry.sendServiceWithRetry() 带重试调用
       │     └─ 未命中 → rpcClient.sendRequest() 单次调用
       ├─ 6. 根据 RpcResponse.code 上报熔断器：
       │     ├─ code == 200 → circuitBreaker.recordSuccess()
       │     └─ code == 500 → circuitBreaker.recordFailure()
       └─ 7. 返回 response.getData()
```

### 2.2 代理对象创建

```java
public <T> T getProxy(Class<T> clazz) {
    Object o = Proxy.newProxyInstance(
        clazz.getClassLoader(),
        new Class[]{clazz},
        this
    );
    return (T) o;
}
```

客户端使用方式：`UserService userService = clientProxy.getProxy(UserService.class)` 即可获得透明的远程调用代理。业务代码完全无感知底层 RPC 通信细节。

---

## 三、底层网络传输机制

### 3.1 自定义二进制协议

编解码器位于 `version5/krpc-common/.../serializer/mycoder/`，自定义了一套定长头部 + 变长负载的二进制通信协议，总头部固定 8 字节：

```
| messageType (2B) | serializerType (2B) | dataLength (4B) | serializedPayload (dataLength B) |
```

字段说明：
- **messageType**（2 字节）：0 = REQUEST（RpcRequest），1 = RESPONSE（RpcResponse）
- **serializerType**（2 字节）：0 = JDK 原生，1 = FastJSON，2 = Kryo，3 = Hessian（默认），4 = Protostuff
- **dataLength**（4 字节）：序列化后的消息体长度，用于 TCP 粘包/拆包处理
- **serializedPayload**（变长）：经过序列化器序列化的二进制数据

### 3.2 编码器（MyEncoder）

继承 Netty 的 `MessageToByteEncoder`，编码流程：
1. 判断消息类型（RpcRequest 或 RpcResponse），写入对应的 MessageType code。
2. 写入序列化器类型标识。
3. 调用 `serializer.serialize(msg)` 得到字节数组。
4. 写入字节数组长度（防止粘包）。
5. 写入字节数组内容。

### 3.3 解码器（MyDecoder）

继承 Netty 的 `ByteToMessageDecoder`，解码流程：
1. 读取 2 字节消息类型，确定解码目标类是 RpcRequest 还是 RpcResponse。
2. 读取 2 字节序列化器类型，通过 `Serializer.getSerializerByCode()` 获取对应序列化器实例。
3. 读取 4 字节数据长度。
4. 校验可读字节数是否 >= 数据长度（处理拆包场景：数据不全则等待下一次数据到达）。
5. 读取指定长度的字节数组。
6. 调用序列化器的 `deserialize()` 还原为 Java 对象。

### 3.4 Netty 服务端（NettyRpcServer）

位于 `version5/krpc-core/.../server/server/impl/NettyRpcServer.java`：

- 采用经典的 **Boss-Worker 线程模型**：BossGroup 负责接受连接，WorkerGroup 负责处理 I/O 读写。
- 通道类型：`NioServerSocketChannel`（非阻塞 I/O）。
- Pipeline 初始化（NettyServerInitializer）：
  ```
  MyEncoder → MyDecoder → NettyRpcServerHandler
  ```
- **NettyRpcServerHandler**（继承 `SimpleChannelInboundHandler<RpcRequest>`）处理逻辑：
  1. 通过 `RateLimitProvider` 获取限流器，调用 `getToken()` 检查令牌。
  2. 若无令牌，记录限流日志，**直接断开连接**，不返回任何响应。
  3. 若获取令牌成功，从 `ServiceProvider` 本地注册表中查找目标服务实例。
  4. 通过反射 `method.invoke(service, request.getParams())` 调用本地服务实现。
  5. 构造 `RpcResponse`，通过 `ctx.writeAndFlush()` 写回客户端。

### 3.5 Netty 客户端（NettyRpcClient）

位于 `version5/krpc-core/.../client/rpcclient/impl/NettyRpcClient.java`：

- **NioEventLoopGroup 静态共享**：所有客户端实例共享同一个 EventLoopGroup，避免频繁创建销毁线程资源。
- **AttributeKey 线程隔离**：使用 Netty Channel 的 `AttributeKey<RpcResponse>` 机制，每个请求将响应存入当前 Channel 的 Attribute 中，通过 `ChannelFutureListener` 同步等待响应返回。
- Pipeline 初始化（NettyClientInitializer）：
  ```
  MyEncoder → MyDecoder → NettyClientHandler
  ```
- **NettyClientHandler**：收到 RpcResponse 后，通过 `ctx.channel().attr(key).set(response)` 写入 Channel Attribute，唤醒等待线程。

### 3.6 TCP 粘包/拆包处理

通过在协议头部固定 4 字节记录消息体长度，解码器先读取完整头部（8 字节），再根据 dataLength 字段确认是否有足够数据来读取完整消息体。若当前可读字节不足，Netty 的 `ByteToMessageDecoder` 自动累积缓冲区数据，等待下一轮数据到达后继续解码，从而自然解决了 TCP 流式传输的粘包/拆包问题。

---

## 四、序列化与 SPI 可插拔机制

### 4.1 五种序列化器

所有序列化器实现统一的 `Serializer` 接口，定义 `serialize()` 和 `deserialize()` 两个核心方法，以及 `getType()` 返回序列化类型码。

| 序列化器 | 类型码 | 实现 | 特点 |
|---------|--------|------|------|
| JDK 原生 | 0 | ObjectSerializer | 标准 Java 序列化，兼容性好，性能一般 |
| FastJSON | 1 | JsonSerializer | 文本格式可读，含特殊类型转换逻辑 |
| Kryo | 2 | KryoSerializer | 高性能二进制，线程不安全需 ThreadLocal 隔离 |
| Hessian | 3 | HessianSerializer | 跨语言支持，默认序列化器 |
| Protostuff | 4 | ProtostuffSerializer | 极致性能，基于 Protobuf 的运行时方案 |

### 4.2 SPI 机制

`SpiLoader` 位于 `version5/krpc-common/.../spi/SpiLoader.java`：

- **配置文件路径**：`META-INF/serializer/com.kama.common.serializer.myserializer.Serializer`
- **配置格式**：`key=全限定类名`（如 `Hessian=com.kama.common.serializer.myserializer.HessianSerializer`）
- **加载机制**：通过 Hutool `ResourceUtil.getResources()` 扫描 classpath 下所有同名配置文件，逐行解析 key-value 映射。
- **懒加载 + 实例缓存**：
  - `loadedSpiMap`（ConcurrentHashMap）：按接口名缓存实现类的 key-Class 映射。
  - `instanceCache`（ConcurrentHashMap）：按实现类名缓存单例对象，避免重复实例化。
- **使用方法**：`SpiLoader.loadSpi(Serializer.class)` 加载后，通过 `SpiLoader.getInstance(Serializer.class, "Hessian")` 获取序列化器实例。

---

## 五、负载均衡

三种负载均衡策略均实现 `LoadBalance` 接口，位于 `version5/krpc-core/.../client/servicecenter/balance/`：

### 5.1 随机负载均衡（RandomLoadBalance）

使用 `java.util.Random.nextInt(size)` 从地址列表中随机选取一个节点。地址列表使用 `CopyOnWriteArrayList` 保证线程安全。

### 5.2 轮询负载均衡（RoundLoadBalance）

使用 `AtomicInteger.getAndUpdate(i -> (i + 1) % size)` 实现无锁线程安全的轮询计数器，每次请求依次分配到下一个节点。

### 5.3 一致性哈希负载均衡（ConsistencyHashBalance）

**哈希算法**：采用 **FNV1_32_HASH** 算法（Fowler-Noll-Vo 哈希），具有高离散度和低碰撞率的特性：

```
p = 16777619
hash = 2166136261 (FNV offset basis)
对每个字符: hash = (hash ^ char) * p
额外进行 5 次位混合操作（左移 13、右移 7、左移 3、右移 17、左移 5）以增强雪崩效应
最终取绝对值
```

**哈希环结构**：使用 `TreeMap<Integer, String>` 维护一个有序的哈希环，每个真实节点创建 5 个虚拟节点（格式：`host:port&&VN0` ~ `host:port&&VN4`），虚拟节点的哈希值作为环上的坐标，节点名称作为值。

**路由逻辑**：
1. 使用 `UUID.randomUUID()` 作为请求标识，计算其哈希值。
2. 通过 `TreeMap.tailMap(hash)` 找到哈希环上顺时针第一个大于等于该哈希值的虚拟节点。
3. 若没有更大的节点（即哈希值落在环的末尾），则回到环首的第一个虚拟节点（`shards.firstKey()`）。
4. 从虚拟节点名中解析出真实节点地址。

**动态增删**：`addNode()` 和 `delNode()` 方法支持运行时动态添加和移除节点及其对应的虚拟节点，适应服务上下线场景。

---

## 六、熔断器

`CircuitBreaker` 位于 `version5/krpc-core/.../client/circuitbreaker/CircuitBreaker.java`，实现三态状态机：

```
        失败次数超阈值
  CLOSED ──────────────► OPEN
    ▲                      │
    │   成功率达阈值        │ 超时恢复
    │                      ▼
    └──────────────── HALF_OPEN
                          │
                          └── 再次失败 → OPEN
```

### 6.1 状态说明

- **CLOSED（关闭）**：正常状态，所有请求放行。失败次数累计达到 `failureThreshold` 时切换到 OPEN。
- **OPEN（开启）**：熔断生效，直接拒绝所有请求（返回 null）。等待 `retryTimePeriod`（默认 10 秒）后切换到 HALF_OPEN。
- **HALF_OPEN（半开）**：允许有限请求通过作为探测。若成功率达到 `halfOpenSuccessRate`（默认 0.5）则恢复为 CLOSED；若发生失败则立即回到 OPEN。

### 6.2 默认参数

- `failureThreshold` = 1（失败 1 次即触发熔断）
- `halfOpenSuccessRate` = 0.5（半开状态下 50% 成功率即可恢复）
- `retryTimePeriod` = 10 秒（熔断恢复等待时间）

### 6.3 线程安全

所有状态变更和计数器操作均使用 `synchronized` 方法级锁。`failureCount`、`successCount`、`requestCount` 使用 `AtomicInteger`。`CircuitBreakerProvider` 通过 `ConcurrentHashMap` 按方法名管理独立的熔断器实例。

---

## 七、令牌桶限流

`TokenBucketRateLimitImpl` 位于 `version5/krpc-core/.../server/ratelimit/impl/TokenBucketRateLimitImpl.java`：

### 7.1 算法原理

- **rate** = 100ms（令牌生成间隔，每 100ms 生成 1 个令牌）
- **capacity** = 10（桶容量上限，允许的最大突发流量）
- **curCapacity**：`volatile` 修饰，当前桶内可用令牌数，初始为满桶。
- **lastTimestamp**：`volatile` 修饰，上次请求时间戳。

### 7.2 获取令牌流程

1. 若 `curCapacity > 0`，直接消耗一个令牌并返回 true。
2. 否则根据 `当前时间 - lastTimestamp` 计算该时间段内新生成的令牌数：`generatedTokens = (currentTimestamp - lastTimestamp) / rate`。
3. 若 `generatedTokens > 1`，将桶容量更新为 `min(capacity, curCapacity + generatedTokens - 1)`（-1 是因为当前请求需要消耗一个）。
4. 更新 `lastTimestamp` 并返回 true。
5. 若生成的令牌也不足以满足当前请求，返回 false。

### 7.3 限流行为

在 `NettyRpcServerHandler` 中，若 `getToken()` 返回 false，服务端**直接关闭连接**，丢弃请求，不会返回任何响应。`RateLimitProvider` 按接口名维护独立的限流器实例。

---

## 八、超时重试

### 8.1 注解标记

`@Retryable` 注解位于 `krpc-api`，标记在服务接口方法上，声明该方法可被客户端安全重试（即幂等方法，如查询操作）。

### 8.2 Guava Retry 策略

`GuavaRetry` 位于 `version5/krpc-core/.../client/retry/GuavaRetry.java`，使用 Guava Retryer 框架：

- **重试条件**：发生异常 或 响应的 `code == 500`
- **等待策略**：固定等待 2 秒（`WaitStrategies.fixedWait(2, TimeUnit.SECONDS)`）
- **最大重试次数**：3 次（`StopStrategies.stopAfterAttempt(3)`）
- **白名单过滤**：仅对已注册到 ZK `/CanRetry/` 路径的方法执行重试，避免对非幂等操作（如写操作）进行重复调用。

---

## 九、配置系统

`KRpcApplication`（DCL 单例）位于 `version5/krpc-core/.../KRpcApplication.java`，是框架的全局入口，负责加载和管理全局配置。

### 9.1 配置文件

使用 `application.properties`，支持多环境：通过 `application-{env}.properties` 切换不同环境配置。

### 9.2 配置项（KRpcConfig）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| name | krpc | 应用名称 |
| port | 9999 | 服务端口 |
| host | localhost | 服务主机地址 |
| version | 1.0.0 | 版本号 |
| registry | zookeeper | 注册中心类型 |
| serializer | Hessian | 序列化器类型 |
| loadBalance | ConsistencyHash | 负载均衡策略 |

### 9.3 配置加载

`ConfigUtil` 基于 Hutool Props 工具类实现，通过 `RpcConstant.CONFIG_PREFIX = "rpc"` 前缀加载配置，自动映射到 `KRpcConfig` 对象。

---

## 十、快速开始

### 10.1 启动 Zookeeper

确保本地 Zookeeper 运行在 `127.0.0.1:2181`。

### 10.2 启动服务提供者

```java
// krpc-provider: ProviderTest.java
KRpcApplication.initialize();                          // 加载配置
ServiceProvider serviceProvider = new ServiceProvider();
serviceProvider.provideService(new UserServiceImpl()); // 注册服务到本地 + ZK
RpcServer rpcServer = new NettyRpcServer(serviceProvider);
rpcServer.start(9999);                                 // 启动 Netty 服务端
```

### 10.3 启动服务消费者

```java
// krpc-consumer: ConsumerTest.java
ClientProxy clientProxy = new ClientProxy();
UserService userService = clientProxy.getProxy(UserService.class); // 获取动态代理
User user = userService.getUserByUserId(1);                        // 透明远程调用
Integer result = userService.insertUserId(User.builder()...build()); // 写操作
```

---

## 十一、项目结构速查

```
version5/
├── krpc-api/src/main/java/com/kama/
│   ├── annotation/Retryable.java           # @Retryable 注解
│   ├── pojo/User.java                      # 传输 POJO
│   └── service/UserService.java            # 服务接口
├── krpc-common/src/main/java/common/
│   ├── message/
│   │   ├── RpcRequest.java                 # 请求消息体
│   │   ├── RpcResponse.java                # 响应消息体
│   │   └── MessageType.java                # 消息类型枚举
│   ├── serializer/myserializer/
│   │   ├── Serializer.java                 # 序列化器接口
│   │   ├── ObjectSerializer.java           # JDK 原生序列化
│   │   ├── JsonSerializer.java             # FastJSON 序列化
│   │   ├── KryoSerializer.java             # Kryo 序列化
│   │   ├── HessianSerializer.java          # Hessian 序列化
│   │   └── ProtostuffSerializer.java       # Protostuff 序列化
│   ├── serializer/mycoder/
│   │   ├── MyEncoder.java                  # Netty 编码器
│   │   └── MyDecoder.java                  # Netty 解码器
│   ├── spi/SpiLoader.java                  # SPI 加载器
│   ├── util/ConfigUtil.java                # 配置工具
│   └── exception/SerializeException.java   # 序列化异常
├── krpc-core/src/main/java/com/kama/
│   ├── KRpcApplication.java                # 框架全局入口（DCL 单例）
│   ├── config/
│   │   ├── KRpcConfig.java                 # 配置 POJO
│   │   └── RpcConstant.java                # 配置常量
│   ├── client/
│   │   ├── proxy/ClientProxy.java          # JDK 动态代理
│   │   ├── rpcclient/impl/
│   │   │   └── NettyRpcClient.java         # Netty 客户端
│   │   ├── netty/
│   │   │   ├── NettyClientInitializer.java # 客户端 Pipeline 初始化
│   │   │   └── NettyClientHandler.java     # 客户端响应处理
│   │   ├── servicecenter/
│   │   │   ├── ZKServiceCenter.java        # ZK 服务发现 + 负载均衡
│   │   │   ├── ZKWatcher/watchZK.java      # ZK 事件监听器
│   │   │   └── balance/impl/               # 负载均衡实现
│   │   │       ├── RandomLoadBalance.java
│   │   │       ├── RoundLoadBalance.java
│   │   │       └── ConsistencyHashBalance.java
│   │   ├── cache/ServiceCache.java         # 本地服务地址缓存
│   │   ├── circuitbreaker/
│   │   │   ├── CircuitBreaker.java         # 三态熔断器
│   │   │   └── CircuitBreakerProvider.java # 熔断器管理器
│   │   └── retry/GuavaRetry.java           # Guava 重试
│   └── server/
│       ├── server/impl/NettyRpcServer.java # Netty 服务端
│       ├── netty/
│       │   ├── NettyServerInitializer.java # 服务端 Pipeline 初始化
│       │   └── NettyRpcServerHandler.java  # 服务端请求处理（限流 + 反射调用）
│       ├── serviceRegister/impl/
│       │   └── ZKServiceRegister.java      # ZK 服务注册
│       ├── provider/ServiceProvider.java   # 本地服务注册表
│       └── ratelimit/impl/
│           └── TokenBucketRateLimitImpl.java # 令牌桶限流
└── krpc-core/src/main/resources/META-INF/serializer/
    └── com.kama.common.serializer.myserializer.Serializer  # SPI 配置文件
```

---

## 十二、架构全景图

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                         KRPC Consumer                               │
 │                                                                     │
 │  UserService proxy = ClientProxy.getProxy(UserService.class)        │
 │  proxy.getUserByUserId(1) ──── JDK 动态代理 ────┐                   │
 │                                                    │                 │
 │  ┌─────────────────────────────────────────────────┘                 │
 │  │                                                                  │
 │  ▼                                                                  │
 │  ClientProxy.invoke()                                               │
 │  ├─ 构建 RpcRequest                                                 │
 │  ├─ CircuitBreaker.allowRequest() ──── 熔断前置检查                  │
 │  ├─ ZKServiceCenter.serviceDiscovery() ── ZK 服务发现 + 一致性哈希   │
 │  ├─ checkRetry() ── 白名单判断                                     │
 │  ├─ NettyRpcClient.sendRequest()                                    │
 │  │   └─ Netty Pipeline: Encoder → Decoder → Handler                │
 │  └─ 响应码上报熔断器                                                │
 └───────────────────────┬─────────────────────────────────────────────┘
                         │ 自定义二进制协议 (8B Header + Payload)
                         │ Netty NIO 传输
                         ▼
 ┌─────────────────────────────────────────────────────────────────────┐
 │                         KRPC Provider                               │
 │                                                                     │
 │  NettyRpcServer (BossGroup + WorkerGroup)                           │
 │  ├─ Netty Pipeline: Encoder → Decoder → Handler                    │
 │  └─ NettyRpcServerHandler                                          │
 │       ├─ TokenBucketRateLimitImpl.getToken() ── 令牌桶限流          │
 │       └─ method.invoke(service, params) ────── 反射调用本地实现      │
 │                                                                     │
 │  ZKServiceRegister.register()                                       │
 │  └─ /MyRPC/{服务名}/{host:port} (EPHEMERAL) ── 服务注册到 ZK        │
 │  └─ /CanRetry/{host:port}/{方法签名} (EPHEMERAL) ── 重试白名单注册  │
 └───────────────────────┬─────────────────────────────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │     Zookeeper       │
              │  127.0.0.1:2181     │
              │  /MyRPC 命名空间     │
              │  /CanRetry 命名空间  │
              └─────────────────────┘
```

## 关于微服务生态与链路追踪

当前 KRPC 是一个**独立研发的轻量级 RPC 框架**，并非基于 Dubbo / Spring Cloud 等成熟微服务框架的二次开发。项目未集成 Zipkin、Jaeger、OpenTelemetry 等分布式链路追踪系统，也未包含 Trace ID / Span ID 的生成和传递机制。version5 引入了 Spring Boot 3.3.5，为后续集成完整的微服务生态（如 Spring Cloud 服务治理、Sentinel 流量控制、SkyWalking 链路追踪等）预留了扩展接口。
