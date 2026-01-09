---
layout: page
title: 动态内存调整指南 (Dynamic Memory Adjustment Guide)
nav_order: 6
parent: Developer Overview
---
# Gluten 动态内存调整逻辑

## 概述

本文档描述了 Gluten 中动态内存调整逻辑的位置和架构。该内存管理系统旨在将原生内存分配与 Spark 的统一内存管理框架集成。

## 架构

Gluten 的动态内存调整逻辑分布在多个层次：

1. **Java 层** - JNI 接口和 Spark 集成
2. **原生层** - C++ 内存分配器和内存池
3. **后端特定层** - Velox 和 ClickHouse 实现

## 关键组件和文件位置

### 1. Java 内存管理层

#### 核心内存消费者
**位置:** `gluten-core/src/main/java/io/glutenproject/memory/GlutenMemoryConsumer.java`

此类扩展了 Spark 的 `MemoryConsumer`，提供了原生内存分配和 Spark 内存管理之间的桥梁。关键方法：
- `acquire(long size)` - 从 Spark 内存管理器获取内存
- `free(long size)` - 将内存释放回 Spark
- `spill(long size, MemoryConsumer trigger)` - 通过溢写数据处理内存压力

#### 预留监听器
**位置:** `gluten-data/src/main/java/io/glutenproject/memory/alloc/`

关键文件：
- `ReservationListener.java` - 内存预留回调接口
- `ManagedReservationListener.java` - 实现 `ReservationListener` 接口，提供与 Spark 集成的动态内存预留/释放
  - 实现 `reserve(long size)` - 动态地从 Spark 预留内存
  - 实现 `unreserve(long size)` - 将预留的内存释放回 Spark
  - 实现 `reserveOrThrow(long size)` - 预留内存或在内存不足时抛出异常

#### 原生内存分配器
**位置:** `gluten-data/src/main/java/io/glutenproject/memory/alloc/`

关键文件：
- `NativeMemoryAllocators.java` - 创建内存分配器的工厂类
- `NativeMemoryAllocator.java` - 原生内存分配接口
- `NativeMemoryAllocatorManager.java` - 管理原生分配器的生命周期

`NativeMemoryAllocators` 类提供：
- `contextInstance()` - 返回具有 Spark 内存管理的任务范围分配器
- `createSpillable(Spiller spiller)` - 创建可在内存压力下溢写数据的分配器
- `globalInstance()` - 返回全局共享分配器

### 2. 原生 C++ 内存管理层

#### 核心内存分配器
**位置:** `cpp/core/memory/MemoryAllocator.h` 和 `cpp/core/memory/MemoryAllocator.cc`

关键类：
- `MemoryAllocator` - 内存分配的抽象基类
- `ListenableMemoryAllocator` - 在分配变化时通知监听器的包装器
- `StdMemoryAllocator` - 使用 malloc/free 的标准实现

动态调整的关键方法：
- `reserveBytes(int64_t size)` - 预留内存而不实际分配
- `unreserveBytes(int64_t size)` - 取消先前预留的内存
- `reallocate()` / `reallocateAligned()` - 动态调整分配大小

#### Arrow 内存池
**位置:** `cpp/core/memory/ArrowMemoryPool.h` 和 `cpp/core/memory/ArrowMemoryPool.cc`

将 Gluten 的内存分配器与 Apache Arrow 的内存池接口集成。

### 3. 后端特定实现

#### Velox 后端

**内存池:** `cpp/velox/memory/VeloxMemoryPool.cc`

关键特性：
- `VeloxMemoryAllocator` - 为 Velox 包装 Gluten 分配器
- 通过回调机制实现动态内存预留：
  - `allocateNonContiguous()` - 使用预留回调分配
  - `allocateContiguous()` - 使用回调分配连续内存
  - `freeNonContiguous()` / `freeContiguous()` - 释放内存并更新预留

**溢写配置:** `cpp/velox/compute/WholeStageResultIterator.cc`

第 38-39 行定义了可溢写预留增长百分比：
```cpp
const std::string kSpillableReservationGrowthPct =
    "spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct";
```

第 277-278 行将默认增长百分比设置为 25%：
```cpp
configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
    getConfigValue(kSpillableReservationGrowthPct, "25");
```

此配置控制在执行期间需要更多内存时，内存预留增长的百分比。

#### ClickHouse 后端

**位置:** `backends-clickhouse/src/main/java/io/glutenproject/memory/alloc/`

关键文件：
- `CHNativeMemoryAllocators.java`
- `CHNativeMemoryAllocator.java`
- `CHReservationListener.java`
- `CHManagedCHReservationListener.java`

## 动态内存调整流程

### 1. 内存获取流程

```
原生代码请求
    ↓
ListenableMemoryAllocator.allocate()
    ↓
AllocationListener.allocationChanged(+size)
    ↓
ReservationListener.reserve(size)
    ↓
ManagedReservationListener.reserve()
    ↓
GlutenMemoryConsumer.acquire(size)
    ↓
Spark TaskMemoryManager.acquireExecutionMemory()
```

### 2. 内存释放流程

```
原生代码释放
    ↓
ListenableMemoryAllocator.free()
    ↓
AllocationListener.allocationChanged(-size)
    ↓
ReservationListener.unreserve(size)
    ↓
ManagedReservationListener.unreserve()
    ↓
GlutenMemoryConsumer.free(size)
    ↓
Spark TaskMemoryManager.releaseExecutionMemory()
```

### 3. 内存压力处理（溢写）

```
检测到内存压力
    ↓
Spark TaskMemoryManager 触发溢写
    ↓
GlutenMemoryConsumer.spill()
    ↓
Spiller.spill()
    ↓
原生后端将数据溢写到磁盘
    ↓
释放内存
```

## 配置选项

动态内存调整的关键配置选项：

### Velox 后端
- `spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct` (默认值: 25)
  - 控制需要时内存预留增长的百分比
- `spark.gluten.sql.columnar.backend.velox.aggregationSpillMemoryThreshold`
  - 聚合算子溢写的阈值
- `spark.gluten.sql.columnar.backend.velox.joinSpillMemoryThreshold`
  - Join 算子溢写的阈值
- `spark.gluten.sql.columnar.backend.velox.orderBySpillMemoryThreshold`
  - 排序算子溢写的阈值

### 通用 Spark 配置
- `spark.memory.offHeap.size` - Spark 可用的总堆外内存
- `spark.memory.offHeap.enabled` - 启用堆外内存使用

## 关键设计原则

1. **统一管理**: 原生内存分配通过 Spark 的统一内存管理框架进行跟踪和管理

2. **延迟预留**: 可以使用 `reserveBytes()` / `unreserveBytes()` 预留内存而不实际分配

3. **优雅降级**: 当内存不足时，系统可以：
   - 将数据溢写到磁盘（如果配置了溢写器）
   - 触发从其他消费者回收内存
   - 如果无法获取内存则抛出异常

4. **监听器模式**: `AllocationListener` 和 `ReservationListener` 实现内存变化的解耦通知

5. **任务级隔离**: 每个 Spark 任务都有自己的 `NativeMemoryAllocator` 实例，具有适当的记账

## 测试

内存管理的测试文件：
- `backends-clickhouse/src/test/java/org/apache/spark/memory/TestTaskMemoryManagerSuite.java`
- `cpp/core/tests/HbwAllocatorTest.cc`

## 总结

Gluten 中的动态内存调整逻辑主要位于：

1. **Java 层**: `gluten-core/src/main/java/io/glutenproject/memory/` 和 `gluten-data/src/main/java/io/glutenproject/memory/alloc/`
2. **原生层**: `cpp/core/memory/MemoryAllocator.{h,cc}`
3. **Velox 后端**: `cpp/velox/memory/VeloxMemoryPool.cc` 和 `cpp/velox/compute/WholeStageResultIterator.cc`
4. **ClickHouse 后端**: `backends-clickhouse/src/main/java/io/glutenproject/memory/alloc/`

该系统使用分层架构，其中原生内存分配通过监听器回调和预留机制透明地与 Spark 的内存管理集成，从而能够根据工作负载需求进行动态内存调整。
