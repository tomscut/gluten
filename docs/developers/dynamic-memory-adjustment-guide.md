# Dynamic Memory Adjustment Logic in Gluten

## Overview

This document describes the location and architecture of the dynamic memory adjustment logic in Gluten. The memory management system is designed to integrate native memory allocations with Spark's unified memory management framework.

## Architecture

Gluten's dynamic memory adjustment logic is distributed across multiple layers:

1. **Java Layer** - JNI interfaces and Spark integration
2. **Native Layer** - C++ memory allocators and pools
3. **Backend-specific Layer** - Velox and ClickHouse implementations

## Key Components and File Locations

### 1. Java Memory Management Layer

#### Core Memory Consumer
**Location:** `gluten-core/src/main/java/io/glutenproject/memory/GlutenMemoryConsumer.java`

This class extends Spark's `MemoryConsumer` and provides the bridge between native memory allocations and Spark's memory management. Key methods:
- `acquire(long size)` - Acquires memory from Spark's memory manager
- `free(long size)` - Releases memory back to Spark
- `spill(long size, MemoryConsumer trigger)` - Handles memory pressure by spilling data

#### Reservation Listeners
**Location:** `gluten-data/src/main/java/io/glutenproject/memory/alloc/`

Key files:
- `ReservationListener.java` - Interface for memory reservation callbacks
- `ManagedReservationListener.java` - Implements the `ReservationListener` interface to provide dynamic memory reservation/unreservation with Spark integration
  - Implements `reserve(long size)` - Dynamically reserves memory from Spark
  - Implements `unreserve(long size)` - Releases reserved memory back to Spark
  - Implements `reserveOrThrow(long size)` - Reserves memory or throws exception if insufficient

#### Native Memory Allocators
**Location:** `gluten-data/src/main/java/io/glutenproject/memory/alloc/`

Key files:
- `NativeMemoryAllocators.java` - Factory for creating memory allocators
- `NativeMemoryAllocator.java` - Interface for native memory allocation
- `NativeMemoryAllocatorManager.java` - Manages lifecycle of native allocators

The `NativeMemoryAllocators` class provides:
- `contextInstance()` - Returns a task-scoped allocator with Spark memory management
- `createSpillable(Spiller spiller)` - Creates an allocator that can spill data under memory pressure
- `globalInstance()` - Returns the global shared allocator

### 2. Native C++ Memory Management Layer

#### Core Memory Allocator
**Location:** `cpp/core/memory/MemoryAllocator.h` and `cpp/core/memory/MemoryAllocator.cc`

Key classes:
- `MemoryAllocator` - Abstract base class for memory allocation
- `ListenableMemoryAllocator` - Wrapper that notifies listeners on allocation changes
- `StdMemoryAllocator` - Standard implementation using malloc/free

Key methods for dynamic adjustment:
- `reserveBytes(int64_t size)` - Reserves memory without actual allocation
- `unreserveBytes(int64_t size)` - Unreserves previously reserved memory
- `reallocate()` / `reallocateAligned()` - Dynamically resizes allocations

#### Arrow Memory Pool
**Location:** `cpp/core/memory/ArrowMemoryPool.h` and `cpp/core/memory/ArrowMemoryPool.cc`

Integrates Gluten's memory allocator with Apache Arrow's memory pool interface.

### 3. Backend-Specific Implementations

#### Velox Backend

**Memory Pool:** `cpp/velox/memory/VeloxMemoryPool.cc`

Key features:
- `VeloxMemoryAllocator` - Wraps Gluten allocator for Velox
- Dynamic memory reservation through callback mechanisms:
  - `allocateNonContiguous()` - Allocates with reservation callbacks
  - `allocateContiguous()` - Allocates contiguous memory with callbacks
  - `freeNonContiguous()` / `freeContiguous()` - Releases memory and updates reservations

**Spill Configuration:** `cpp/velox/compute/WholeStageResultIterator.cc`

Lines 38-39 define the spillable reservation growth percentage:
```cpp
const std::string kSpillableReservationGrowthPct =
    "spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct";
```

Lines 277-278 set the default growth percentage to 25%:
```cpp
configs[velox::core::QueryConfig::kSpillableReservationGrowthPct] =
    getConfigValue(kSpillableReservationGrowthPct, "25");
```

This configuration controls how much memory reservation grows when more memory is needed during execution.

#### ClickHouse Backend

**Location:** `backends-clickhouse/src/main/java/io/glutenproject/memory/alloc/`

Key files:
- `CHNativeMemoryAllocators.java`
- `CHNativeMemoryAllocator.java`
- `CHReservationListener.java`
- `CHManagedCHReservationListener.java`

## Dynamic Memory Adjustment Flow

### 1. Memory Acquisition Flow

```
Native Code Request
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

### 2. Memory Release Flow

```
Native Code Free
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

### 3. Memory Pressure Handling (Spilling)

```
Memory Pressure Detected
    ↓
Spark TaskMemoryManager triggers spill
    ↓
GlutenMemoryConsumer.spill()
    ↓
Spiller.spill()
    ↓
Native Backend Spills Data to Disk
    ↓
Memory Released
```

## Configuration Options

Key configuration options for dynamic memory adjustment:

### Velox Backend
- `spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct` (default: 25)
  - Controls the percentage by which memory reservation grows when needed
- `spark.gluten.sql.columnar.backend.velox.aggregationSpillMemoryThreshold`
  - Threshold for aggregation operator spilling
- `spark.gluten.sql.columnar.backend.velox.joinSpillMemoryThreshold`
  - Threshold for join operator spilling
- `spark.gluten.sql.columnar.backend.velox.orderBySpillMemoryThreshold`
  - Threshold for sort operator spilling

### General Spark Configuration
- `spark.memory.offHeap.size` - Total off-heap memory available to Spark
- `spark.memory.offHeap.enabled` - Enable off-heap memory usage

## Key Design Principles

1. **Unified Management**: Native memory allocations are tracked and managed through Spark's unified memory management framework

2. **Lazy Reservation**: Memory can be reserved without actual allocation using `reserveBytes()` / `unreserveBytes()`

3. **Graceful Degradation**: When memory is insufficient, the system can:
   - Spill data to disk (if spiller is configured)
   - Trigger memory reclamation from other consumers
   - Throw exception if memory cannot be acquired

4. **Listener Pattern**: `AllocationListener` and `ReservationListener` enable decoupled notification of memory changes

5. **Per-Task Isolation**: Each Spark task gets its own `NativeMemoryAllocator` instance with proper accounting

## Testing

Test files for memory management:
- `backends-clickhouse/src/test/java/org/apache/spark/memory/TestTaskMemoryManagerSuite.java`
- `cpp/core/tests/HbwAllocatorTest.cc`

## Summary

The dynamic memory adjustment logic in Gluten is primarily located in:

1. **Java Layer**: `gluten-core/src/main/java/io/glutenproject/memory/` and `gluten-data/src/main/java/io/glutenproject/memory/alloc/`
2. **Native Layer**: `cpp/core/memory/MemoryAllocator.{h,cc}`
3. **Velox Backend**: `cpp/velox/memory/VeloxMemoryPool.cc` and `cpp/velox/compute/WholeStageResultIterator.cc`
4. **ClickHouse Backend**: `backends-clickhouse/src/main/java/io/glutenproject/memory/alloc/`

The system uses a layered architecture where native memory allocations are transparently integrated with Spark's memory management through listener callbacks and reservation mechanisms, enabling dynamic memory adjustment based on workload demands.
