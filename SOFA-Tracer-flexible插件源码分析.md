# SOFA Tracer flexible 插件源码分析

> 本文基于 `sofa-tracer-flexible-plugin` 及其在 `tracer-sofa-boot-starter` 中的 Spring Boot 集成代码，梳理该插件的定位、核心类、调用链路与使用方式。

---

## 一、插件定位

`sofa-tracer-flexible-plugin` 是一个**面向业务方法的自定义埋点插件**。

SOFA Tracer 的大多数插件（redis / httpclient / datasource 等）都是针对**特定框架自动埋点**，覆盖范围由框架决定；而 flexible 插件正好相反——它把埋点选择权交给业务方：

- 想追踪哪个方法，就在方法上打 `@Tracer` 注解，或调用编程式 API；
- 输出独立的 `biz-digest.log`（明细）/ `biz-stat.log`（统计）日志；
- 组件名 `ComponentNameConstants.FLEXIBLE = "flexible-biz"`（`tracer-core/.../ComponentNameConstants.java:40`）。

它解决的是"内置插件覆盖不到的业务代码"的链路追踪问题。

---

## 二、模块结构

该插件和 redis 插件一样，**核心逻辑与 Spring Boot 集成是分离的两个模块**：

### 1. 插件核心模块 `sofa-tracer-plugins/sofa-tracer-flexible-plugin`

依赖非常轻量（`pom.xml`）：仅 `tracer-core` + `slf4j-api`，不依赖 Spring。

```
com.alipay.sofa.tracer.plugin.flexible
├── annotations/Tracer.java              // @Tracer 注解定义
├── FlexibleTracer.java                  // 核心 Tracer，继承 SofaTracer
├── FlexibleLogEnum.java                 // 日志文件名枚举
├── FlexibleDigestJsonEncoder.java        // 明细 JSON 编码器（基类）
├── FlexibleDigestEncoder.java           // 明细文本编码器
├── FlexibleStatJsonReporter.java        // 统计 JSON 上报器
└── FlexibleStatReporter.java            // 统计文本上报器
```

### 2. Spring Boot 集成模块 `tracer-sofa-boot-starter`

```
com.alipay.sofa.tracer.boot
├── configuration/SofaTracerAutoConfiguration.java   // 创建 Tracer Bean
└── flexible
    ├── configuration/TracerAnnotationConfiguration.java          // AOP 自动装配
    ├── aop/SofaTracerAdvisingBeanPostProcessor.java              // BeanPostProcessor
    ├── aop/TracerAnnotationClassAdvisor.java                     // PointcutAdvisor
    ├── aop/TracerAnnotationClassPointcut.java                    // 切点（匹配 @Tracer）
    └── processor/
        ├── MethodInvocationProcessor.java                        // 处理器接口
        ├── SofaTracerMethodInvocationProcessor.java              // 处理器实现
        └── SofaTracerIntroductionInterceptor.java                // AOP 拦截器
```

---

## 三、核心类详解

### 3.1 `@Tracer` 注解

`annotations/Tracer.java:30`

```java
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD })
public @interface Tracer {
    /** 操作名，不填则默认使用方法名 */
    String operateName() default "";
}
```

- 仅作用于**方法**；
- `operateName` 留空时，拦截器会回退到 `method.getName()` 作为 span 的 operationName（见 `SofaTracerMethodInvocationProcessor.java:48`）。

### 3.2 `FlexibleTracer` —— 核心 Tracer

`FlexibleTracer.java:49` 继承 `SofaTracer`，是整个插件的引擎。它有两个构造器，对应两种使用形态：

#### (1) 两个构造器

```java
// 形态 A：自定义 Sampler + Reporter（可把 span 推到自己的监控系统）
public FlexibleTracer(Sampler sampler, Reporter reporter) {
    super(ComponentNameConstants.FLEXIBLE, sampler);
    this.reporter = reporter;
}

// 形态 B：默认构造，使用官方 DiskReporterImpl，写到 biz-digest.log / biz-stat.log
public FlexibleTracer() {
    super(ComponentNameConstants.FLEXIBLE, null, null, initSampler(), null);
    this.reporter = initReporter();
}
```

`initReporter()`（`FlexibleTracer.java:118`）会根据 `FlexibleLogEnum` 的 key 读取滚动策略、保留天数配置，构造 `DiskReporterImpl`；编码器/统计器再根据 `SofaTracerConfiguration.isJsonOutput()` 选择 JSON 或文本版本。

#### (2) 重写 `reportSpan` —— 可插拔上报

`FlexibleTracer.java:88`

```java
@Override
public void reportSpan(SofaTracerSpan span) {
    if (span == null) return;
    // 1. 采样：仅对根 span 做采样判定
    if (this.getSampler() != null && span.getParentSofaTracerSpan() == null) {
        span.getSofaTracerSpanContext().setSampled(
            this.getSampler().sample(span).isSampled());
    }
    // 2. 触发 ReportListener 扩展点
    invokeReportListeners(span);
    // 3. 走 Reporter 上报；没有 Reporter 则告警
    if (this.reporter != null) {
        this.reporter.report(span);
    } else {
        SelfLog.warn("No reporter implement in flexible tracer");
    }
}
```

三个扩展点：**采样**、**ReportListener**、**Reporter**。这也是插件名 "flexible" 的由来——上报通道完全可定制。

#### (3) 编程式埋点 API：`beforeInvoke` / `afterInvoke`

`FlexibleTracer.java:158` `beforeInvoke(operationName)`：

1. 从 `SofaTraceContext` 弹出当前 server span 作为**父 span**；
2. `buildSpan(operationName).asChildOf(serverSpan).start()` 创建方法级子 span；
3. `setParentSofaTracerSpan(serverSpan)` 主动缓存父 span（因为 `asChildOf` 只关心 spanContext）；
4. 若构建异常，走 `errorSpan()` 兜底（保留 baggage 的根 span）；
5. 打 tag：`local.app`、`method`、`span.kind=client`、`current.thread.name`；
6. `log(CLIENT_SEND_EVENT)` + push 进 context。

`FlexibleTracer.java:196` `afterInvoke(error)`：

1. pop 出 client span；
2. 根据 error 是否为空设置 `result.code`（success / error）；
3. `log(CLIENT_RECV_EVENT)` + `Tags.ERROR = error`；
4. `finish()` 结束 span；
5. 把父 span push 回去，**恢复调用栈**。

这套 push/pop 机制保证了业务方法 span 与外层链路正确嵌套。

### 3.3 `FlexibleLogEnum` —— 日志文件定义

`FlexibleLogEnum.java:28`

```java
FLEXIBLE_DIGEST("biz_digest_log_name", "biz-digest.log", "biz_digest_rolling"),
FLEXIBLE_STAT  ("biz_stat_log_name",   "biz-stat.log",   "biz_stat_rolling");
```

- `logNameKey`：用于在 `SofaTracerConfiguration` 中读取日志保留天数的配置 key；
- `defaultLogName`：默认文件名；
- `rollingKey`：滚动策略配置 key。

### 3.4 明细编码器

#### `FlexibleDigestJsonEncoder`（基类，`FlexibleDigestJsonEncoder.java:35`）

`appendComponentSlot` 的逻辑：

1. 先把 `METHOD` tag 写入 JSON；
2. 遍历 str / num / bool 三类 tag，**排除掉框架通用 tag**（`isFlexible()`），把剩下的"自定义业务 tag"全部追加。

`isFlexible()`（`FlexibleDigestJsonEncoder.java:70`）排除的通用 tag：

```
local.app / traceId / spanId / current.thread.name / method /
time / time.cost.milliseconds / span.kind
```

> 设计意图：通用 tag 已由父类 `AbstractDigestSpanEncoder` 统一输出，这里只输出"业务额外打的 tag"，避免重复。

#### `FlexibleDigestEncoder`（文本版，继承 JSON 版，`FlexibleDigestEncoder.java:33`）

复用 `isFlexible()` 过滤逻辑，把自定义 tag 收集进 `Map` 后用 `StringUtils.mapToString` 拼成字符串追加到 `XStringBuilder`。

### 3.5 统计上报器

#### `FlexibleStatReporter`（文本，`FlexibleStatReporter.java:34`）

`doReportStat` 维度与取值：

| 字段 | 取值 |
|------|------|
| statKey.key | `local.app` + `method` |
| statKey.result | error tag 为空=success，否则=fail |
| statKey.end | 压测标记 `loadTestMark` |
| statKey.loadTest | 是否压测 |
| values | `[1, duration]`（调用次数 + 耗时） |

#### `FlexibleStatJsonReporter`（JSON，`FlexibleStatJsonReporter.java:36`）

逻辑相同，区别是用 `StatMapKey`（带具名 key）替代 `StatKey`，输出结构化 JSON 统计。

---

## 四、Spring Boot 集成（AOP 链路）

`@Tracer` 注解能生效，靠的是下面这套 AOP 装配链。

### 4.1 Tracer Bean 的创建 —— `SofaTracerAutoConfiguration`

`configuration/SofaTracerAutoConfiguration.java:61`

```java
@Bean
@ConditionalOnMissingBean
public Tracer tracer(SofaTracerProperties sofaTracerProperties) throws Exception {
    String reporterName = sofaTracerProperties.getReporterName();
    if (StringUtils.isNotBlank(reporterName)) {
        // 配置了 com.alipay.sofa.tracer.reporterName -> 反射实例化自定义 Reporter
        Reporter reporter = (Reporter) Class.forName(reporterName).newInstance();
        Sampler sampler = SamplerFactory.getSampler();
        return new FlexibleTracer(sampler, reporter);   // 形态 A
    }
    return new FlexibleTracer();                          // 形态 B（默认 biz 日志）
}
```

> 通过配置项 `com.alipay.sofa.tracer.reporterName` 指定自定义 Reporter 全限定类名，即可切换上报通道，无需改代码。

### 4.2 AOP 自动装配 —— `TracerAnnotationConfiguration`

`flexible/configuration/TracerAnnotationConfiguration.java:40`

```java
@Configuration
@ConditionalOnProperty(prefix = "com.alipay.sofa.tracer.flexible",
                       value = "enable", matchIfMissing = true)   // 默认开启
@AutoConfigureAfter(SofaTracerAutoConfiguration.class)
@ConditionalOnBean(Tracer.class)                                 // 必须有 Tracer Bean
public class TracerAnnotationConfiguration {

    @Bean @ConditionalOnMissingBean
    MethodInvocationProcessor sofaMethodInvocationProcessor(Tracer tracer) {
        return new SofaTracerMethodInvocationProcessor(tracer);
    }

    @Bean @ConditionalOnMissingBean
    SofaTracerIntroductionInterceptor sofaTracerIntroductionInterceptor(
            MethodInvocationProcessor methodInvocationProcessor) {
        return new SofaTracerIntroductionInterceptor(methodInvocationProcessor);
    }

    @Bean @ConditionalOnMissingBean
    SofaTracerAdvisingBeanPostProcessor tracerAnnotationBeanPostProcessor(
            SofaTracerIntroductionInterceptor methodInterceptor) {
        return new SofaTracerAdvisingBeanPostProcessor(methodInterceptor);
    }
}
```

三个 Bean 逐层依赖：`Processor` → `Interceptor` → `BeanPostProcessor`。

### 4.3 BeanPostProcessor —— `SofaTracerAdvisingBeanPostProcessor`

`aop/SofaTracerAdvisingBeanPostProcessor.java:29` 继承 `AbstractAdvisingBeanPostProcessor`：

```java
@Override
public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
    setBeforeExistingAdvisors(true);   // 优先于其他 Advisor
    setExposeProxy(true);              // 暴露代理对象，支持同类内部方法互调
    this.advisor = new TracerAnnotationClassAdvisor(this.interceptor);
}
```

`AbstractAdvisingBeanPostProcessor` 会对容器中**所有 Bean** 尝试套用 advisor，但只有命中切点的才会真正被代理。

### 4.4 切点与切面

#### `TracerAnnotationClassAdvisor`（`aop/TracerAnnotationClassAdvisor.java:28`）

继承 `AbstractPointcutAdvisor`，组合 `pointcut` + `advice`：

```java
public TracerAnnotationClassAdvisor(MethodInterceptor interceptor) {
    this.advice = interceptor;                       // 拦截器
    this.pointcut = new TracerAnnotationClassPointcut(); // 切点
}
```

#### `TracerAnnotationClassPointcut`（`aop/TracerAnnotationClassPointcut.java:34`）

继承 `DynamicMethodMatcherPointcut`，关键在 `getClassFilter()`：

```java
public ClassFilter getClassFilter() {
    return (clazz) -> new AnnotationClassOrMethodFilter(Tracer.class).matches(clazz);
}
```

`AnnotationClassOrMethodFilter`（内部类）的 `matches`：

```java
public boolean matches(Class<?> clazz) {
    // 类上标了 @Tracer  OR  类里有方法标了 @Tracer
    return super.matches(clazz) || this.methodsResolver.hasAnnotatedMethods(clazz);
}
```

`AnnotationMethodsResolver.hasAnnotatedMethods` 用 `ReflectionUtils.doWithMethods` 遍历方法，通过 `AnnotationUtils.findAnnotation` 判断是否带 `@Tracer`（支持注解的派生属性）。

> 即：**只要类本身或其任一方法带 `@Tracer`，该类就会被代理**；具体方法是否拦截，由拦截器运行时再判断。

### 4.5 拦截器 —— `SofaTracerIntroductionInterceptor`

`processor/SofaTracerIntroductionInterceptor.java:33` 实现 `IntroductionInterceptor`：

```java
@Override
public Object invoke(MethodInvocation invocation) throws Throwable {
    Method method = invocation.getMethod();
    if (method == null) return invocation.proceed();

    Method mostSpecificMethod = AopUtils.getMostSpecificMethod(method, invocation.getThis().getClass());
    Tracer tracerSpan = findAnnotation(mostSpecificMethod, Tracer.class);
    if (tracerSpan == null) return invocation.proceed();   // 无注解直接放行
    return sofaMethodInvocationProcessor.process(invocation, tracerSpan);
}
```

`findAnnotation`（`:62`）先在当前方法找 `@Tracer`，找不到再回退到**声明类对应方法**找（兼容代理场景）。`implementsInterface` 恒返回 `true`，使其作为 IntroductionInterceptor 可对任意类生效。

### 4.6 处理器 —— `SofaTracerMethodInvocationProcessor`

`processor/SofaTracerMethodInvocationProcessor.java:43` 是真正调用 `FlexibleTracer` 的地方：

```java
if (tracer instanceof FlexibleTracer) {
    try {
        // 1. operationName：注解值优先，为空则用方法名
        String operationName = tracerSpan.operateName();
        if (StringUtils.isBlank(operationName)) {
            operationName = invocation.getMethod().getName();
        }
        // 2. 开 span，打 method / param.types tag
        SofaTracerSpan sofaTracerSpan = ((FlexibleTracer) tracer).beforeInvoke(operationName);
        sofaTracerSpan.setTag(CommonSpanTags.METHOD, invocation.getMethod().getName());
        if (invocation.getArguments() != null && invocation.getArguments().length != 0) {
            StringBuilder sb = new StringBuilder();
            for (Object obj : invocation.getArguments()) {
                sb.append(obj.getClass().getName()).append(";");
            }
            sofaTracerSpan.setTag("param.types", sb.substring(0, sb.length() - 1));
        }
        // 3. 执行真实方法
        return invocation.proceed();
    } catch (Throwable t) {
        ((FlexibleTracer) tracer).afterInvoke(t.getMessage());  // 记录异常
        throw t;
    } finally {
        ((FlexibleTracer) tracer).afterInvoke();               // 收尾
    }
} else {
    return invocation.proceed();  // 非 FlexibleTracer 不处理
}
```

注意 `catch` 与 `finally` 都调了 `afterInvoke`：`afterInvoke(error)` 设 error tag，`afterInvoke()`（无参）设成功码。结合 `afterInvoke` 内部 `pop` 逻辑，**必须保证 push/pop 配对**，所以收尾放在 `finally`。

---

## 五、完整调用链路

以 `@Tracer(operateName = "createOrder")` 标注的业务方法为例：

```
业务调用 createOrder()
   │
   ▼  (Spring AOP 代理)
SofaTracerAdvisingBeanPostProcessor 织入的代理
   │
   ▼
SofaTracerIntroductionInterceptor.invoke()
   │  findAnnotation(method, Tracer.class)  找到注解
   ▼
SofaTracerMethodInvocationProcessor.process()
   │
   ├─ FlexibleTracer.beforeInvoke("createOrder")
   │     ├─ pop 父 span (server span)
   │     ├─ buildSpan.asChildOf(parent).start()  创建子 span
   │     ├─ setTag: local.app / method / span.kind=client / thread
   │     ├─ log(CLIENT_SEND)
   │     └─ push methodSpan
   │
   ├─ setTag: method / param.types
   │
   ├─ invocation.proceed()  ──► 真实业务逻辑
   │
   └─ finally: FlexibleTracer.afterInvoke()
         ├─ pop methodSpan
         ├─ setTag: result.code (success/error) + Tags.ERROR
         ├─ log(CLIENT_RECV)
         ├─ span.finish()  ──► reportSpan()
         │       ├─ sampler 采样判定（根 span）
         │       ├─ invokeReportListeners(span)
         │       └─ reporter.report(span)
         │             ├─ DiskReporterImpl → biz-digest.log + biz-stat.log
         │             └─ 或自定义 Reporter
         └─ push 回父 span，恢复调用栈
```

---

## 六、使用方式

### 6.1 引入依赖

`tracer-sofa-boot-starter` 已包含 `sofa-tracer-flexible-plugin`：

```xml
<dependency>
    <groupId>com.alipay.sofa</groupId>
    <artifactId>tracer-sofa-boot-starter</artifactId>
</dependency>
```

### 6.2 配置

```properties
# 应用名（写入 local.app tag）
spring.application.name=my-app

# 开启 flexible AOP（默认开启，可省略）
com.alipay.sofa.tracer.flexible.enable=true

# 日志输出 JSON（true）或 文本（false）
com.alipay.sofa.tracer.jsonOutput=true

# （可选）自定义 Reporter 全限定类名，不配则用默认 DiskReporterImpl
# com.alipay.sofa.tracer.reporterName=com.example.MyReporter
```

### 6.3 方式一：`@Tracer` 注解（推荐）

```java
@Service
public class OrderService {

    // 显式指定操作名
    @Tracer(operateName = "createOrder")
    public Order createOrder(OrderRequest req) {
        // 业务逻辑
    }

    // 不填 operateName，默认用方法名 "queryOrder"
    @Tracer
    public Order queryOrder(Long id) {
        // 业务逻辑
    }
}
```

要点：
- 被标注的方法所在类会被 Spring AOP 代理，**类内自调用（this.xxx）不走代理**——如需生效，需通过 `AopContext.currentProxy()` 或拆到另一个 Bean 调用；
- 注解支持继承（`AnnotationUtils.findAnnotation` 可识别元注解）。

### 6.4 方式二：编程式手动埋点

适用于无法用注解（如循环内动态分支）或需要精细控制的场景：

```java
@Service
public class PayService {

    @Autowired
    private io.opentracing.Tracer tracer;  // 容器中的 FlexibleTracer

    public void doPay(PayRequest req) {
        FlexibleTracer flexibleTracer = (FlexibleTracer) tracer;
        flexibleTracer.beforeInvoke("manualPay");
        try {
            // 业务逻辑
        } catch (Throwable t) {
            flexibleTracer.afterInvoke(t.getMessage());
            throw t;
        } finally {
            flexibleTracer.afterInvoke();
        }
    }
}
```

### 6.5 方式三：自定义 Reporter

实现 `com.alipay.common.tracer.core.reporter.facade.Reporter`，把 span 推到自己的监控系统：

```java
public class MyReporter implements Reporter {
    @Override
    public String getReporterType() { return "my-reporter"; }

    @Override
    public void report(SofaTracerSpan span) {
        // 自定义上报：MQ / 远程 / 自定义存储
    }

    @Override
    public void close() { }
}
```

参考测试 `TestReporter.java`（把 operationName 写入 `biz.txt`）。启用方式二选一：

- 配置 `com.alipay.sofa.tracer.reporterName=com.example.MyReporter`，由 `SofaTracerAutoConfiguration` 反射创建；
- 或自己声明一个 `Tracer` Bean 覆盖默认（`@ConditionalOnMissingBean` 会让出）。

---

## 七、日志输出说明

| 文件 | 类型 | 内容 |
|------|------|------|
| `biz-digest.log` | 明细 | 每次调用一条记录：traceId、spanId、operationName、method、param.types、耗时、result.code、error（异常时）、以及业务自定义 tag |
| `biz-stat.log` | 统计 | 按 `local.app + method + 成功/失败 + 压测标记` 聚合：调用次数、总耗时 |

明细日志中被 `isFlexible()` 排除的通用 tag（local.app / traceId / spanId / time / time.cost.milliseconds / span.kind / method / current.thread.name）由父类统一输出，编码器只追加业务额外打的 tag。

测试验证见 `FlexibleTracerTest.java`：
- 调用 `/tracer` → biz-digest.log 产生 2 条（含外层 HTTP span）；
- 调用 `/tracerException` → 第 3 条 `error` 字段为 `"testException"`。

---

## 八、配置项汇总

| 配置项 | 默认值 | 作用 |
|--------|--------|------|
| `com.alipay.sofa.tracer.flexible.enable` | `true` | 是否启用 `@Tracer` AOP |
| `com.alipay.sofa.tracer.reporterName` | 无 | 自定义 Reporter 全限定类名 |
| `com.alipay.sofa.tracer.jsonOutput` | `false` | 日志 JSON / 文本格式 |
| `biz_digest_log_name` | （天数） | biz-digest.log 保留天数 |
| `biz_stat_log_name` | （天数） | biz-stat.log 保留天数 |
| `biz_digest_rolling` | （策略） | biz-digest.log 滚动策略 |
| `biz_stat_rolling` | （策略） | biz-stat.log 滚动策略 |
| `spring.application.name` | - | 写入 `local.app` tag |

---

## 九、与其他插件对比

| 维度 | redis 等组件插件 | flexible 插件 |
|------|-----------------|--------------|
| 埋点对象 | 特定框架（Spring Data Redis 等） | 任意业务方法 |
| 埋点方式 | BeanPostProcessor 装饰框架类 | `@Tracer` 注解 AOP / 编程式 API |
| 拦截机制 | 装饰器模式（委托 delegate） | Spring AOP（IntroductionInterceptor） |
| 组件名 | `java-redis` 等 | `flexible-biz` |
| 日志文件 | redis-digest.log 等 | biz-digest.log / biz-stat.log |
| 统计维度 | localApp + dbType | localApp + method |
| Reporter | 固定 Disk | 可插拔（自定义 Reporter / Sampler / ReportListener） |
| 业务侵入 | 零侵入 | 需加注解或写代码（半侵入） |

---

## 十、设计要点总结

1. **核心与集成分离**：插件模块只依赖 `tracer-core`，Spring Boot 集成独立放在 starter，便于非 Spring 场景复用。
2. **注解 + AOP 半侵入**：用 `@Tracer` 显式声明要追踪的方法，比自动埋点更可控，比纯手动 API 更简洁。
3. **三大扩展点**：`Sampler`（采样）、`ReportListener`（监听）、`Reporter`（上报通道），让上报行为高度可定制。
4. **push/pop 栈式 span 管理**：`beforeInvoke` push、`afterInvoke` pop，保证方法 span 与外层链路正确嵌套，并在结束时恢复父 span。
5. **通用 tag 与业务 tag 分离**：编码器用 `isFlexible()` 过滤通用 tag，只输出业务额外 tag，避免重复且支持业务自定义透传。
6. **切点匹配"类或方法"**：`AnnotationClassOrMethodFilter` 只要类或任一方法带注解就代理该类，运行时再由拦截器按方法精确判断，兼顾性能与灵活性。
