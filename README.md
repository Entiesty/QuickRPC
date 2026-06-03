# KRPC 鈥?杞婚噺绾?RPC 妗嗘灦

## 椤圭洰绠€浠?
KRPC 鏄竴涓熀浜?Java 瀹炵幇鐨勮交閲忕骇杩滅▼杩囩▼璋冪敤锛圧PC锛夋鏋讹紝閲囩敤 Netty 浣滀负搴曞眰缃戠粶閫氫俊寮曟搸锛孉pache Zookeeper 浣滀负鏈嶅姟娉ㄥ唽涓庡彂鐜颁腑蹇冿紝骞堕€氳繃 JDK 鍔ㄦ€佷唬鐞嗗疄鐜伴€忔槑鐨勮繙绋嬫湇鍔¤皟鐢ㄣ€傞」鐩粡鍘嗕簡浠庡師鐢?Socket 鍒?Netty銆佷粠纭紪鐮佸埌 Zookeeper 娉ㄥ唽涓績銆佷粠鍗曚竴瀹炰緥鍒拌礋杞藉潎琛°€佸啀鍒扮啍鏂檺娴佷笌 SPI 鍙彃鎷斿簭鍒楀寲鐨勪簲涓増鏈凯浠ｃ€?
---

## 椤圭洰鏋舵瀯

### 妯″潡鍒掑垎锛圴ersion 5锛?
```
version5
鈹溾攢鈹€ krpc-api          # 鏈嶅姟鎺ュ彛瀹氫箟銆丳OJO銆佹敞瑙ｏ紙@Retryable锛?鈹溾攢鈹€ krpc-common       # 鍏叡缁勪欢锛氭秷鎭崗璁€佸簭鍒楀寲鍣ㄣ€佽嚜瀹氫箟缂栬В鐮佸櫒銆丼PI 鍔犺浇鍣ㄣ€侀厤缃伐鍏?鈹溾攢鈹€ krpc-core         # 鏍稿績妗嗘灦锛氬姩鎬佷唬鐞嗐€丯etty 瀹㈡埛绔?鏈嶅姟绔€乑K 鏈嶅姟鍙戠幇/娉ㄥ唽銆佽礋杞藉潎琛°€佺啍鏂€侀檺娴併€侀噸璇?鈹溾攢鈹€ krpc-consumer     # 鏈嶅姟娑堣垂鑰呯ず渚?鈹斺攢鈹€ krpc-provider     # 鏈嶅姟鎻愪緵鑰呯ず渚?+ 鎺ュ彛瀹炵幇
```

### 鐗堟湰婕旇繘

| 鐗堟湰 | 鎻愪氦淇℃伅 | 鏍稿績鐗规€?|
|------|----------|----------|
| v1.1 | Version1:Part1 | 鍩轰簬 Java 鍘熺敓 Socket 鐨勫熀鏈?RPC 閫氫俊 |
| v1.2 | Version1:part2 | 浣跨敤 Netty 鏇夸唬鍘熺敓 Socket |
| v1.3 | version1:part3 | 寮曞叆 Zookeeper 浣滀负娉ㄥ唽涓績 |
| v2.1 | version2:part1 | 鑷畾涔?Netty 缂栬В鐮佸櫒涓庡簭鍒楀寲鍣?|
| v2.2 | version2:part2 | 瀹㈡埛绔湰鍦版湇鍔＄紦瀛?+ ZK 鍔ㄦ€佹洿鏂?|
| v3.1 | version3:part1 | 璐熻浇鍧囪　锛堥殢鏈恒€佽疆璇€佷竴鑷存€у搱甯岋級 |
| v3.2 | version3:part2 | 瓒呮椂閲嶈瘯 + 鐧藉悕鍗曟満鍒?|
| v4.1 | version4:part1 | 鏈嶅姟闄愭祦锛堜护鐗屾《绠楁硶锛?|
| v4.2 | version4:part2 | 鏈嶅姟鐔旀柇锛堜笁鎬佺姸鎬佹満锛?|
| v5.0 | version5:part1 | SPI 鏈哄埗銆佸搴忓垪鍖栧櫒鏀寔銆侀厤缃嚜鍔ㄥ寲 |

---

## 鏍稿績鏈哄埗璇﹁В

### 涓€銆乑ookeeper 鏈嶅姟娉ㄥ唽涓庡彂鐜?
**鏈嶅姟娉ㄥ唽锛堟湇鍔＄锛?*

`ZKServiceRegister` 瀹炵幇 `ServiceRegister` 鎺ュ彛锛屽湪鏈嶅姟鎻愪緵鑰呭惎鍔ㄦ椂灏嗚嚜韬敞鍐屽埌 Zookeeper锛?
- 浣跨敤 **CuratorFramework** 杩炴帴 ZK 鏈嶅姟鍣紙榛樿 `127.0.0.1:2181`锛夛紝閲囩敤 `ExponentialBackoffRetry` 鎸囨暟閫€閬块噸璇曠瓥鐣ワ紝浼氳瘽瓒呮椂 40s
- 鍦?`/MyRPC/{鎺ュ彛鍏ㄩ檺瀹氬悕}` 璺緞涓嬪垱寤?*鎸佷箙鑺傜偣**锛圥ERSISTENT锛変綔涓烘湇鍔℃牴鑺傜偣
- 鍦ㄨ鏍硅妭鐐逛笅鍒涘缓**涓存椂鑺傜偣**锛圗PHEMERAL锛夛紝鑺傜偣鍚嶄负 `{host}:{port}` 鏍煎紡鐨勫湴鍧€瀛楃涓诧紝杩欐牱褰撴湇鍔℃彁渚涜€呮柇寮€杩炴帴鏃?ZK 浼氳嚜鍔ㄥ垹闄ゅ搴旇妭鐐?- 鍚屾椂鍦?`/CanRetry/` 鍛藉悕绌洪棿涓嬫敞鍐屾爣娉ㄤ簡 `@Retryable` 娉ㄨВ鐨勬柟娉曠鍚嶏紝浣滀负閲嶈瘯鐧藉悕鍗?
**鏈嶅姟鍙戠幇锛堝鎴风锛?*

`ZKServiceCenter` 瀹炵幇 `ServiceCenter` 鎺ュ彛锛岃礋璐ｄ粠 ZK 鑾峰彇鍙敤鏈嶅姟鍦板潃锛?
- 浣跨敤 `ServiceCache` 缁存姢鏈湴鏈嶅姟鍦板潃缂撳瓨锛圕oncurrentHashMap锛夛紝鍑忓皯瀵?ZK 鐨勭洿鎺ヨ闂?- 閫氳繃 `watchZK` 瀹炵幇 **CuratorCache 鐩戝惉鍣?*锛岀洃鍚?ZK 鑺傜偣鐨?CREATE / CHANGE / DELETE 浜嬩欢骞跺疄鏃跺悓姝ユ湰鍦扮紦瀛?- 鏈嶅姟鍙戠幇鏃朵紭鍏堜粠鏈湴缂撳瓨璇诲彇锛屾湭鍛戒腑鏃跺洖閫€鍒?ZK 鏌ヨ

**ZK 鐩綍缁撴瀯绀烘剰**

```
/MyRPC
  鈹溾攢鈹€ com.kama.service.UserService
  鈹?    鈹溾攢鈹€ 192.168.1.10:9999 锛堜复鏃惰妭鐐癸級
  鈹?    鈹斺攢鈹€ 192.168.1.11:9999 锛堜复鏃惰妭鐐癸級
  鈹斺攢鈹€ com.kama.service.OrderService
        鈹斺攢鈹€ 192.168.1.10:8888 锛堜复鏃惰妭鐐癸級

/CanRetry
  鈹斺攢鈹€ 192.168.1.10:9999
        鈹溾攢鈹€ com.kama.service.UserService#getUserByUserId(java.lang.Integer)
        鈹斺攢鈹€ com.kama.service.UserService#insertUserId(com.kama.pojo.User)
```

### 浜屻€丣DK 鍔ㄦ€佷唬鐞嗘満鍒?
`ClientProxy` 鏄鎴风閫忔槑鐨勬牳蹇冿紝瀹炵幇浜?`InvocationHandler` 鎺ュ彛锛?
1. **浠ｇ悊鍒涘缓**锛氶€氳繃 `Proxy.newProxyInstance(classLoader, interfaces, this)` 涓虹洰鏍囨帴鍙ｅ垱寤轰唬鐞嗗璞★紝璋冪敤鏂瑰儚璋冪敤鏈湴鏂规硶涓€鏍蜂娇鐢?2. **璇锋眰鏋勫缓**锛氬湪 `invoke()` 涓弽灏勮幏鍙栨柟娉曞悕銆佸弬鏁般€佸弬鏁扮被鍨嬨€佹帴鍙ｅ悕锛屾瀯寤?`RpcRequest` 瀵硅薄
3. **鐔旀柇鍓嶇疆妫€鏌?*锛氶€氳繃 `CircuitBreakerProvider` 鑾峰彇瀵瑰簲鏂规硶鐨勭啍鏂櫒锛岃皟鐢?`allowRequest()` 鍒ゆ柇鏄惁鏀捐
4. **鏈嶅姟鍙戠幇**锛氳皟鐢?`ZKServiceCenter.serviceDiscovery(request)` 鑾峰彇鐩爣鏈嶅姟鍦板潃锛堝惈璐熻浇鍧囪　锛?5. **閫氫俊涓庨噸璇?*锛氬垱寤?`NettyRpcClient`锛岃嫢鏂规硶鍦ㄩ噸璇曠櫧鍚嶅崟涓垯閫氳繃 `GuavaRetry` 鎵ц甯﹂噸璇曠殑璋冪敤锛屽惁鍒欏崟娆¤皟鐢?6. **缁撴灉涓婃姤**锛氭牴鎹?`RpcResponse.code`锛?00 鎴愬姛 / 500 澶辫触锛夊悜鐔旀柇鍣ㄤ笂鎶ユ垚鍔熸垨澶辫触
7. **杩斿洖鍊?*锛氬皢 `RpcResponse.data` 杩斿洖缁欒皟鐢ㄦ柟锛岃皟鐢ㄦ柟鏃犳劅鐭ヨ繙绋嬭繃绋?
### 涓夈€佸簳灞傜綉缁滀紶杈撴満鍒?
**鑷畾涔夐€氫俊鍗忚**

閲囩敤瀹氶暱澶撮儴 + 鍙橀暱杞借嵎鐨勪簩杩涘埗鍗忚锛岀敱 `MyEncoder` 鍜?`MyDecoder` 瀹炵幇锛?
```
鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?鈹? messageType    鈹? serializerType  鈹?  dataLength    鈹?  serializedData    鈹?鈹?  (2 bytes)     鈹?   (2 bytes)     鈹?  (4 bytes)     鈹?  (dataLength bytes)鈹?鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹粹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹粹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹粹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?```

- **messageType**锛氬尯鍒嗚姹傦紙0锛夊拰鍝嶅簲锛?锛?- **serializerType**锛氭爣璇嗗簭鍒楀寲鏂瑰紡锛?=Java, 1=JSON, 2=Kryo, 3=Hessian, 4=Protostuff锛?- **dataLength**锛氬簭鍒楀寲鍚庣殑鏁版嵁闀垮害锛岀敤浜庣矘鍖?鎷嗗寘澶勭悊
- **serializedData**锛氬疄闄呭簭鍒楀寲杞借嵎

**Netty 鏈嶅姟绔紙NettyRpcServer锛?*

- 閲囩敤缁忓吀鐨?**Boss-Worker 绾跨▼妯″瀷**锛坄NioEventLoopGroup`锛夛紝Boss 绾跨▼璐熻矗鎺ユ敹杩炴帴锛學orker 绾跨▼璐熻矗 I/O 璇诲啓
- 浣跨敤 `NioServerSocketChannel`锛岄€氳繃 `ServerBootstrap` 缁戝畾绔彛
- 绠￠亾鍒濆鍖?`NettyServerInitializer`锛屼緷娆℃坊鍔?`MyEncoder` 鈫?`MyDecoder` 鈫?`NettyRpcServerHandler`
- `NettyRpcServerHandler` 缁ф壙 `SimpleChannelInboundHandler<RpcRequest>`锛屽湪 `channelRead0` 涓畬鎴愶細闄愭祦浠ょ墝鑾峰彇 鈫?鍙嶅皠璋冪敤鏈湴鏈嶅姟瀹炵幇 鈫?鍐欏洖 `RpcResponse`

**Netty 瀹㈡埛绔紙NettyRpcClient锛?*

- 浣跨敤 `Bootstrap` 杩炴帴鏈嶅姟绔紝闈欐€佸垵濮嬪寲锛堜竴涓?JVM 鍐呭叡浜?EventLoopGroup 鍜?Bootstrap锛?- 绠￠亾鍒濆鍖?`NettyClientInitializer`锛屾坊鍔?`MyEncoder` 鈫?`MyDecoder` 鈫?`NettyClientHandler`
- 鍙戦€佽姹傚悗閫氳繃 `channel.closeFuture().sync()` 鍚屾闃诲绛夊緟缁撴灉
- `NettyClientHandler` 鏀跺埌 `RpcResponse` 鍚庨€氳繃 `AttributeKey` 瀛樺叆 Channel 灞炴€э紝渚?`sendRequest()` 璇诲彇
- 鍩轰簬 **AttributeKey 鐨勭嚎绋嬮殧绂?*鐗规€т繚璇佸苟鍙戝畨鍏?
**鍗忚浼樺娍**

- 瀹氶暱澶撮儴浣胯В鐮佸櫒鑳藉噯纭垽鏂暟鎹畬鏁存€э紝閬垮厤 TCP 绮樺寘/鎷嗗寘闂
- 鍙墿灞曪細閫氳繃 serializerType 瀛楁鏀寔澶氱搴忓垪鍖栨柟寮忥紝鏃犻渶淇敼鍗忚缁撴瀯
- 鍒嗙娑堟伅绫诲瀷浣跨紪瑙ｇ爜鍣ㄨ兘鍖哄垎璇锋眰涓庡搷搴旓紝鍦ㄨВ鐮佹椂鍑嗙‘鍙嶅簭鍒楀寲涓烘纭殑 Java 绫诲瀷

### 鍥涖€佸簭鍒楀寲涓?SPI 鏈哄埗

**搴忓垪鍖栧櫒浣撶郴**

```
Serializer (鎺ュ彛)
鈹溾攢鈹€ ObjectSerializer   (type=0, Java 鍘熺敓搴忓垪鍖?
鈹溾攢鈹€ JsonSerializer     (type=1, FastJSON)
鈹溾攢鈹€ KryoSerializer     (type=2, Kryo 楂樻€ц兘浜岃繘鍒?
鈹溾攢鈹€ HessianSerializer  (type=3, 榛樿, 璺ㄨ瑷€鏀寔)
鈹斺攢鈹€ ProtostuffSerializer (type=4, Protobuf 鍙樹綋, 鏋佽嚧鎬ц兘)
```

**SPI 鍔犺浇鏈哄埗**

`SpiLoader` 浠?`META-INF/serializer/` 鍔犺浇閰嶇疆鏂囦欢锛屾牸寮忎负 `key=鍏ㄩ檺瀹氱被鍚峘锛?
```
Hessian=common.serializer.myserializer.HessianSerializer
json=common.serializer.myserializer.JsonSerializer
kryo=common.serializer.myserializer.KryoSerializer
jdk=common.serializer.myserializer.ObjectSerializer
protobuf=common.serializer.myserializer.ProtostuffSerializer
```

- 閲囩敤**鎳掑姞杞?*锛氶娆¤皟鐢ㄦ椂璇诲彇閰嶇疆鏂囦欢骞剁敤 Class.forName 鍔犺浇瀹炵幇绫?- **瀹炰緥缂撳瓨**锛氫娇鐢?ConcurrentHashMap 缂撳瓨宸插垱寤虹殑搴忓垪鍖栧櫒瀹炰緥锛岄伩鍏嶉噸澶嶅弽灏勫垱寤?- 閫氳繃 `Serializer.getSerializerByCode(code)` 鏍规嵁缂栫爜蹇€熻幏鍙栧搴斿簭鍒楀寲鍣?
### 浜斻€佽礋杞藉潎琛?
涓夌绛栫暐鍧囧疄鐜?`LoadBalance` 鎺ュ彛锛?
| 绠楁硶 | 瀹炵幇绫?| 鏍稿績鏈哄埗 |
|------|--------|----------|
| 闅忔満 | `RandomLoadBalance` | `java.util.Random.nextInt(size)` |
| 杞 | `RoundLoadBalance` | `AtomicInteger.getAndUpdate()` 淇濊瘉绾跨▼瀹夊叏鐨勮嚜澧炲彇妯?|
| 涓€鑷存€у搱甯?| `ConsistencyHashBalance` | FNV1_32_HASH 绠楁硶 + 铏氭嫙鑺傜偣锛埫?锛? TreeMap.tailMap 瀹氫綅 |

榛樿浣跨敤**涓€鑷存€у搱甯?*绛栫暐锛岄€氳繃 5 涓櫄鎷熻妭鐐圭紦瑙ｆ暟鎹€炬枩锛屼娇鐢?UUID 浣滀负璇锋眰鏍囪瘑杩涜鍝堝笇鐜畾浣嶃€?
### 鍏€佹湇鍔＄啍鏂?
`CircuitBreaker` 瀹炵幇**涓夋€佺姸鎬佹満**锛?
```
              澶辫触鏁?>= failureThreshold
       鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?       鈹?                                    鈹?       鈻?                                    鈹?   鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹?   瓒呰繃 retryTimePeriod     鈹屸攢鈹€鈹€鈹€鈹粹攢鈹?   鈹?OPEN 鈹?鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈻?鈹侶ALF  鈹?   鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹?                           鈹?OPEN 鈹?        鈻?                             鈹斺攢鈹€鈹攢鈹€鈹€鈹?        鈹?    鍗婂紑鐘舵€佸彂鐢熷け璐?             鈹?        鈹溾攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?        鈹?     鎴愬姛鐜?>= halfOpenSuccessRate
        鈹?        鈹斺攢鈹€鈹€鈹€ CLOSED 鈼勨攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?```

鏍稿績鍙傛暟锛?- `failureThreshold`锛氬け璐ユ鏁伴槇鍊硷紝瓒呰繃鍚庝粠 CLOSED 鈫?OPEN
- `halfOpenSuccessRate`锛氬崐寮€鐘舵€佷笅杈惧埌鎸囧畾鎴愬姛鐜囧悗鎭㈠涓?CLOSED
- `retryTimePeriod`锛歄PEN 鐘舵€佺殑鎭㈠绛夊緟鏃堕棿锛堥粯璁?10s锛?
`CircuitBreakerProvider` 涓烘瘡涓柟娉曠淮鎶ょ嫭绔嬬殑鐔旀柇鍣ㄥ疄渚嬶紙ConcurrentHashMap锛夛紝鎵€鏈夌姸鎬佸彉鏇村拰璁℃暟鍣ㄦ搷浣滃潎浣跨敤 `synchronized` 淇濊瘉绾跨▼瀹夊叏銆?
### 涓冦€佹湇鍔￠檺娴?
閲囩敤**浠ょ墝妗剁畻娉?*锛坄TokenBucketRateLimitImpl`锛夛細

- `rate`锛氫护鐗岀敓鎴愰€熺巼锛坢s/涓級锛岄粯璁?100ms
- `capacity`锛氭《瀹归噺涓婇檺锛岄粯璁?10
- `curCapacity`锛氬綋鍓嶅彲鐢ㄤ护鐗屾暟锛坴olatile 淇グ锛?- `lastTimestamp`锛氫笂娆¤姹傛椂闂存埑

鑾峰彇浠ょ墝鏃跺厛灏濊瘯娑堣€楀凡鏈変护鐗岋紝鑻ユ《绌哄垯鏍规嵁鏃堕棿宸绠楁柊鐢熸垚鐨勪护鐗屾暟骞惰ˉ鍏咃紙涓嶈秴杩?capacity锛夈€俙RateLimitProvider` 鎸夋帴鍙ｅ悕涓烘瘡涓湇鍔℃帴鍙ｅ垱寤虹嫭绔嬬殑闄愭祦鍣ㄣ€?
鍦?`NettyRpcServerHandler.getResponse()` 涓紝鑻?`rateLimit.getToken()` 杩斿洖 false锛屽垯鐩存帴杩斿洖 500 鍝嶅簲锛?鏈嶅姟闄愭祦锛屾帴鍙?xxx 褰撳墠鏃犳硶澶勭悊璇锋眰"锛屽疄鐜板揩閫熷け璐ラ檷绾с€?
### 鍏€佽秴鏃堕噸璇曚笌鐧藉悕鍗?
浣跨敤 **Guava Retryer** 妗嗘灦锛坄GuavaRetry`锛夛細

```
閲嶈瘯鏉′欢锛氬紓甯?鎴?鍝嶅簲鐮佷负 500
绛夊緟绛栫暐锛氬浐瀹氱瓑寰?2 绉?鍋滄绛栫暐锛氭渶澶氶噸璇?3 娆?```

**鐧藉悕鍗曟満鍒?*锛?
- 鍙湁鏍囨敞浜?`@Retryable` 娉ㄨВ鐨勬柟娉曟墠浼氳鍔犲叆閲嶈瘯鐧藉悕鍗?- 鏈嶅姟娉ㄥ唽鏃讹紝閫氳繃鍙嶅皠鎵弿鎺ュ彛鏂规硶涓婄殑 `@Retryable`锛屽皢鏂规硶绛惧悕鍐欏叆 ZK 鐨?`/CanRetry/` 鍛藉悕绌洪棿
- 瀹㈡埛绔皟鐢?`checkRetry()` 鏃朵粠 ZK 鍔犺浇鐧藉悕鍗曞苟缂撳瓨鍒?`CopyOnWriteArraySet`锛岀‘淇濆箓绛夋€х害瀹?
### 涔濄€侀厤缃嚜鍔ㄥ寲

`ConfigUtil` 浣跨敤 Hutool 鐨?Props 宸ュ叿鍔犺浇 `application.properties`锛堟敮鎸佸鐜 `application-{env}.properties`锛夛紝鑷姩鏄犲皠鍒?`KRpcConfig` 瀵硅薄銆傞粯璁ら厤缃細

- 搴旂敤鍚嶏細`krpc`锛岀鍙ｏ細`9999`锛屼富鏈猴細`localhost`
- 娉ㄥ唽涓績锛歚zookeeper`锛屽簭鍒楀寲鍣細`Hessian`锛坈ode=3锛?- 璐熻浇鍧囪　锛歚ConsistencyHash`

`KRpcApplication` 浣跨敤**鍙岄噸妫€鏌ラ攣瀹?*锛圖CL锛夊崟渚嬫ā寮忕鐞嗗叏灞€閰嶇疆锛屾敮鎸佽嚜瀹氫箟閰嶇疆娉ㄥ叆鎴栭粯璁ら厤缃姞杞姐€?
---

## 鎶€鏈爤

| 缁勪欢 | 鎶€鏈€夊瀷 | 鐗堟湰 |
|------|----------|------|
| 璇█ | Java | 21 |
| 缃戠粶閫氫俊 | Netty | 4.1.51 |
| 鏈嶅姟娉ㄥ唽/鍙戠幇 | Apache Curator + Zookeeper | Curator 鍐呯疆 |
| 搴忓垪鍖?| FastJSON / Kryo / Hessian / Protostuff | 澶氱増鏈?|
| 閲嶈瘯妗嗘灦 | Guava Retryer | 2.0.0 |
| 閰嶇疆鍔犺浇 | Hutool Props | (Spring Boot 浼犻€? |
| IOC 瀹瑰櫒 | Spring Boot | 3.3.5 |
| 绠€鍖栦唬鐮?| Lombok | 1.18.30 |
| 鏋勫缓宸ュ叿 | Maven | - |

---

## 蹇€熷紑濮?
### 鍚姩 Zookeeper

纭繚鏈湴 Zookeeper 杩愯鍦?`127.0.0.1:2181`銆?
### 鍚姩鏈嶅姟鎻愪緵鑰?
```bash
# 杩愯 ProviderTest
mvn exec:java -pl krpc-provider -Dexec.mainClass="com.kama.provider.ProviderTest"
```

### 鍚姩鏈嶅姟娑堣垂鑰?
```bash
# 杩愯 ConsumerTest锛?0 绾跨▼骞跺彂锛屽叡 120 娆¤皟鐢級
mvn exec:java -pl krpc-consumer -Dexec.mainClass="com.kama.consumer.ConsumerTest"
```

### 鑷畾涔夐厤缃?
鍦?classpath 涓嬪垱寤?`application.properties`锛?
```properties
rpc.name=my-krpc
rpc.port=8888
rpc.host=192.168.1.100
rpc.version=2.0.0
```

---

## 璋冪敤娴佺▼

```
Consumer                         Zookeeper                      Provider
   鈹?                                鈹?                             鈹?   鈹? 1. getProxy(UserService.class) 鈹?                             鈹?   鈹傗攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?                             鈹?   鈹?                                鈹?                             鈹?   鈹? 2. proxy.getUserByUserId(1)   鈹?                             鈹?   鈹? 鈹€鈹€鈻?ClientProxy.invoke()      鈹?                             鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?       鈹溾攢 鐔旀柇鍣ㄦ鏌?             鈹?                             鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?       鈹溾攢 3. 鏈嶅姟鍙戠幇 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈻衡攤                              鈹?   鈹?       鈹傗梽鈹€鈹€鈹€ 杩斿洖鍦板潃鍒楄〃 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?                             鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?       鈹溾攢 4. 璐熻浇鍧囪　閫夋嫨鍦板潃      鈹?                             鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?       鈹溾攢 5. 妫€鏌ラ噸璇曠櫧鍚嶅崟        鈹?                             鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?       鈹溾攢 6. Netty 鍙戦€佽姹?鈹€鈹€鈹€鈹€鈹€鈹傗攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈻衡攤
   鈹?       鈹?                       鈹?     7. NettyRpcServerHandler 鈹?   鈹?       鈹?                       鈹?        鈹溾攢 浠ょ墝妗堕檺娴佹鏌?      鈹?   鈹?       鈹?                       鈹?        鈹溾攢 鍙嶅皠璋冪敤鏈湴瀹炵幇     鈹?   鈹?       鈹?                       鈹?        鈹斺攢 杩斿洖 RpcResponse   鈹?   鈹?       鈹傗梽鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹傗攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?       鈹溾攢 8. 涓婃姤鐔旀柇鍣ㄧ姸鎬?       鈹?                             鈹?   鈹?       鈹?                       鈹?                             鈹?   鈹?  return User{id=1, ...}       鈹?                             鈹?```

---

## 椤圭洰鐗圭偣

1. **閫忔槑鍖栬繙绋嬭皟鐢?*锛氬熀浜?JDK 鍔ㄦ€佷唬鐞嗭紝璋冪敤杩滅▼鏈嶅姟涓庤皟鐢ㄦ湰鍦版柟娉曚綋楠屼竴鑷?2. **楂樺彲鐢ㄨ璁?*锛氱啍鏂?闄愭祦-閲嶈瘯涓変綅涓€浣撶殑瀹归敊浣撶郴锛屼护鐗屾《闄愭祦瀹炵幇鏈嶅姟绔嚜鎴戜繚鎶?3. **鍙彃鎷旀灦鏋?*锛歋PI 鏈哄埗浣垮簭鍒楀寲鍣ㄥ彲鑷敱鏇挎崲鍜屾墿灞曪紝鏃犻渶淇敼鏍稿績浠ｇ爜
4. **楂樻€ц兘閫氫俊**锛歂etty NIO + 鑷畾涔変簩杩涘埗鍗忚鐨勭揣鍑戠紪鐮侊紝鏀寔澶氱楂樻€ц兘搴忓垪鍖栧櫒锛圞ryo銆丳rotostuff锛?5. **鍔ㄦ€佹湇鍔℃不鐞?*锛氬熀浜?Zookeeper Watch 鏈哄埗瀹炵幇鏈嶅姟鍒楄〃鐨勫疄鏃跺悓姝ュ拰鏈湴缂撳瓨
6. **璐熻浇鍧囪　鐏垫椿**锛氭敮鎸侀殢鏈恒€佽疆璇€佷竴鑷存€у搱甯屼笁绉嶇瓥鐣ワ紝鍙€氳繃閰嶇疆鍒囨崲
7. **骞傜瓑鎬т繚闅?*锛氶€氳繃 `@Retryable` 娉ㄨВ鐧藉悕鍗曟満鍒讹紝浠呭鏍囪涓哄箓绛夌殑鏂规硶鍚敤閲嶈瘯
