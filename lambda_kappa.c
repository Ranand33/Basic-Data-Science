#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <math.h>
#include <assert.h>

#ifdef _WIN32
#include <windows.h>
#include <process.h>
#define THREAD_RETURN unsigned __stdcall
#define CREATE_THREAD(func, param) (HANDLE)_beginthreadex(NULL, 0, func, param, 0, NULL)
#define JOIN_THREAD(handle) WaitForSingleObject(handle, INFINITE)  
#define THREAD_SLEEP(ms) Sleep(ms)
#define ATOMIC_INCREMENT(x) InterlockedIncrement(x)
#define ATOMIC_DECREMENT(x) InterlockedDecrement(x)
#else
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#define THREAD_RETURN void*
#define CREATE_THREAD(func, param) ({ pthread_t t; pthread_create(&t, NULL, func, param) == 0 ? (void*)t : NULL; })
#define JOIN_THREAD(handle) pthread_join((pthread_t)handle, NULL)
#define THREAD_SLEEP(ms) usleep((ms) * 1000)
#define ATOMIC_INCREMENT(x) __sync_add_and_fetch(x, 1)
#define ATOMIC_DECREMENT(x) __sync_sub_and_fetch(x, 1)
#endif

#define MAX_EVENTS 10000
#define MAX_PARTITIONS 16
#define MAX_WORKERS 8
#define MAX_RETRIES 3
#define BUFFER_SIZE 1024
#define THROTTLE_WINDOW_MS 1000
#define BACKPRESSURE_THRESHOLD 0.8f

// =============================================================================
// CORE DATA STRUCTURES AND EVENTS
// =============================================================================

// Event types for our data processing system
typedef enum {
    EVENT_USER_ACTION,
    EVENT_TRANSACTION,
    EVENT_SENSOR_DATA,
    EVENT_LOG_ENTRY,
    EVENT_HEARTBEAT
} EventType;

// Core event structure
typedef struct {
    uint64_t id;
    EventType type;
    uint64_t timestamp;
    uint32_t user_id;
    char data[256];
    size_t data_size;
    uint32_t partition_key;
    uint8_t retry_count;
    bool processed;
} Event;

// Processing result
typedef struct {
    uint64_t event_id;
    bool success;
    char error_message[128];
    uint64_t processing_time_us;
    uint32_t worker_id;
} ProcessingResult;

// =============================================================================
// RETRY PATTERN IMPLEMENTATION
// =============================================================================

typedef enum {
    RETRY_POLICY_NONE,
    RETRY_POLICY_FIXED,
    RETRY_POLICY_EXPONENTIAL,
    RETRY_POLICY_LINEAR
} RetryPolicy;

typedef struct {
    RetryPolicy policy;
    uint32_t max_retries;
    uint32_t base_delay_ms;
    uint32_t max_delay_ms;
    float jitter_factor;     // 0.0 to 1.0 for randomization
    bool enable_circuit_breaker;
} RetryConfig;

typedef struct {
    uint32_t failure_count;
    uint32_t success_count;
    uint64_t last_failure_time;
    uint64_t last_success_time;
    bool circuit_open;
    uint32_t circuit_open_duration_ms;
} RetryState;

// Calculate delay for retry attempt
uint32_t calculate_retry_delay(const RetryConfig* config, uint8_t attempt) {
    uint32_t delay = 0;
    
    switch (config->policy) {
        case RETRY_POLICY_FIXED:
            delay = config->base_delay_ms;
            break;
            
        case RETRY_POLICY_EXPONENTIAL:
            delay = config->base_delay_ms * (1 << attempt); // 2^attempt
            break;
            
        case RETRY_POLICY_LINEAR:
            delay = config->base_delay_ms * (attempt + 1);
            break;
            
        default:
            return 0;
    }
    
    // Apply maximum delay cap
    if (delay > config->max_delay_ms) {
        delay = config->max_delay_ms;
    }
    
    // Apply jitter to prevent thundering herd
    if (config->jitter_factor > 0.0f) {
        float jitter = (float)rand() / RAND_MAX * config->jitter_factor;
        delay = (uint32_t)(delay * (1.0f + jitter));
    }
    
    return delay;
}

// Execute operation with retry logic
bool execute_with_retry(bool (*operation)(void* context, Event* event, ProcessingResult* result),
                       void* context, Event* event, ProcessingResult* result,
                       const RetryConfig* config, RetryState* state) {
    
    uint64_t current_time = (uint64_t)time(NULL) * 1000;
    
    // Check circuit breaker
    if (config->enable_circuit_breaker && state->circuit_open) {
        if (current_time - state->last_failure_time < state->circuit_open_duration_ms) {
            strcpy(result->error_message, "Circuit breaker open");
            return false;
        } else {
            state->circuit_open = false; // Try to close circuit
        }
    }
    
    for (uint8_t attempt = 0; attempt <= config->max_retries; attempt++) {
        // Apply delay for retries (not for first attempt)
        if (attempt > 0) {
            uint32_t delay = calculate_retry_delay(config, attempt - 1);
            printf("    Retrying in %u ms (attempt %d/%d)\n", delay, attempt + 1, config->max_retries + 1);
            THREAD_SLEEP(delay);
        }
        
        event->retry_count = attempt;
        
        if (operation(context, event, result)) {
            // Success
            state->success_count++;
            state->last_success_time = current_time;
            
            if (state->circuit_open) {
                state->circuit_open = false;
                printf("    Circuit breaker closed after success\n");
            }
            
            return true;
        }
        
        // Failure
        state->failure_count++;
        state->last_failure_time = current_time;
        
        printf("    Attempt %d failed: %s\n", attempt + 1, result->error_message);
        
        // Check if we should open circuit breaker
        if (config->enable_circuit_breaker && 
            state->failure_count >= 5 && 
            (state->failure_count * 100 / (state->failure_count + state->success_count)) > 50) {
            state->circuit_open = true;
            state->circuit_open_duration_ms = 5000; // 5 seconds
            printf("    Circuit breaker opened due to high failure rate\n");
        }
    }
    
    return false; // All retries exhausted
}

// =============================================================================
// BULKHEAD PATTERN IMPLEMENTATION
// =============================================================================

typedef enum {
    BULKHEAD_TYPE_THREAD_POOL,
    BULKHEAD_TYPE_CONNECTION_POOL,
    BULKHEAD_TYPE_MEMORY_POOL,
    BULKHEAD_TYPE_CPU_QUOTA
} BulkheadType;

typedef struct {
    char name[64];
    BulkheadType type;
    uint32_t max_capacity;
    uint32_t current_usage;
    uint32_t queue_size;
    uint32_t timeout_ms;
    bool is_isolated;
    
    // Statistics
    uint32_t total_requests;
    uint32_t successful_requests;
    uint32_t failed_requests;
    uint32_t timeouts;
    uint64_t total_wait_time_ms;
} BulkheadCompartment;

typedef struct {
    BulkheadCompartment compartments[8];
    int compartment_count;
    bool global_circuit_breaker;
} BulkheadManager;

// Create bulkhead compartment
BulkheadCompartment* create_bulkhead_compartment(BulkheadManager* manager, 
                                               const char* name,
                                               BulkheadType type, 
                                               uint32_t capacity) {
    if (manager->compartment_count >= 8) return NULL;
    
    BulkheadCompartment* compartment = &manager->compartments[manager->compartment_count++];
    strncpy(compartment->name, name, sizeof(compartment->name) - 1);
    compartment->type = type;
    compartment->max_capacity = capacity;
    compartment->current_usage = 0;
    compartment->timeout_ms = 5000; // 5 second default timeout
    compartment->is_isolated = true;
    
    return compartment;
}

// Acquire resource from bulkhead compartment
bool acquire_bulkhead_resource(BulkheadCompartment* compartment, uint32_t timeout_ms) {
    uint64_t start_time = (uint64_t)time(NULL) * 1000;
    uint64_t end_time = start_time + timeout_ms;
    
    compartment->total_requests++;
    
    while ((uint64_t)time(NULL) * 1000 < end_time) {
        if (compartment->current_usage < compartment->max_capacity) {
            compartment->current_usage++;
            compartment->successful_requests++;
            compartment->total_wait_time_ms += (uint64_t)time(NULL) * 1000 - start_time;
            return true;
        }
        
        THREAD_SLEEP(10); // Wait 10ms before retry
    }
    
    // Timeout
    compartment->timeouts++;
    compartment->failed_requests++;
    return false;
}

// Release resource back to bulkhead compartment
void release_bulkhead_resource(BulkheadCompartment* compartment) {
    if (compartment->current_usage > 0) {
        compartment->current_usage--;
    }
}

// Check compartment health
bool is_compartment_healthy(const BulkheadCompartment* compartment) {
    if (compartment->total_requests == 0) return true;
    
    float failure_rate = (float)compartment->failed_requests / compartment->total_requests;
    float timeout_rate = (float)compartment->timeouts / compartment->total_requests;
    
    return failure_rate < 0.1f && timeout_rate < 0.05f; // 10% failure, 5% timeout thresholds
}

// =============================================================================
// THROTTLING IMPLEMENTATION
// =============================================================================

typedef enum {
    THROTTLE_ALGORITHM_TOKEN_BUCKET,
    THROTTLE_ALGORITHM_SLIDING_WINDOW,
    THROTTLE_ALGORITHM_FIXED_WINDOW
} ThrottleAlgorithm;

typedef struct {
    ThrottleAlgorithm algorithm;
    uint32_t max_requests;
    uint32_t window_size_ms;
    uint32_t current_tokens;
    uint64_t last_refill_time;
    uint32_t refill_rate; // tokens per second
    
    // Sliding window data
    uint64_t request_timestamps[1000];
    int request_count;
    int request_index;
    
    // Statistics
    uint32_t total_requests;
    uint32_t throttled_requests;
    uint32_t allowed_requests;
} ThrottleManager;

// Initialize throttle manager
void init_throttle_manager(ThrottleManager* throttle, ThrottleAlgorithm algorithm,
                          uint32_t max_requests, uint32_t window_size_ms) {
    memset(throttle, 0, sizeof(ThrottleManager));
    throttle->algorithm = algorithm;
    throttle->max_requests = max_requests;
    throttle->window_size_ms = window_size_ms;
    throttle->current_tokens = max_requests;
    throttle->last_refill_time = (uint64_t)time(NULL) * 1000;
    throttle->refill_rate = max_requests; // Refill completely each window
}

// Token bucket algorithm
bool check_token_bucket(ThrottleManager* throttle) {
    uint64_t current_time = (uint64_t)time(NULL) * 1000;
    uint64_t time_passed = current_time - throttle->last_refill_time;
    
    // Refill tokens based on time passed
    if (time_passed >= throttle->window_size_ms) {
        throttle->current_tokens = throttle->max_requests;
        throttle->last_refill_time = current_time;
    } else {
        uint32_t tokens_to_add = (uint32_t)((time_passed * throttle->refill_rate) / throttle->window_size_ms);
        throttle->current_tokens = (throttle->current_tokens + tokens_to_add > throttle->max_requests) ? 
                                  throttle->max_requests : throttle->current_tokens + tokens_to_add;
    }
    
    if (throttle->current_tokens > 0) {
        throttle->current_tokens--;
        return true;
    }
    
    return false;
}

// Sliding window algorithm
bool check_sliding_window(ThrottleManager* throttle) {
    uint64_t current_time = (uint64_t)time(NULL) * 1000;
    uint64_t window_start = current_time - throttle->window_size_ms;
    
    // Clean old requests
    int valid_requests = 0;
    for (int i = 0; i < throttle->request_count; i++) {
        if (throttle->request_timestamps[i] >= window_start) {
            throttle->request_timestamps[valid_requests++] = throttle->request_timestamps[i];
        }
    }
    throttle->request_count = valid_requests;
    
    // Check if we can accept this request
    if (throttle->request_count < throttle->max_requests) {
        throttle->request_timestamps[throttle->request_count++] = current_time;
        return true;
    }
    
    return false;
}

// Check if request should be throttled
bool should_throttle_request(ThrottleManager* throttle) {
    throttle->total_requests++;
    
    bool allowed = false;
    
    switch (throttle->algorithm) {
        case THROTTLE_ALGORITHM_TOKEN_BUCKET:
            allowed = check_token_bucket(throttle);
            break;
            
        case THROTTLE_ALGORITHM_SLIDING_WINDOW:
            allowed = check_sliding_window(throttle);
            break;
            
        case THROTTLE_ALGORITHM_FIXED_WINDOW:
            // Simple fixed window implementation
            {
                uint64_t current_time = (uint64_t)time(NULL) * 1000;
                uint64_t window_start = (current_time / throttle->window_size_ms) * throttle->window_size_ms;
                
                if (throttle->last_refill_time < window_start) {
                    throttle->current_tokens = throttle->max_requests;
                    throttle->last_refill_time = window_start;
                }
                
                if (throttle->current_tokens > 0) {
                    throttle->current_tokens--;
                    allowed = true;
                }
            }
            break;
    }
    
    if (allowed) {
        throttle->allowed_requests++;
    } else {
        throttle->throttled_requests++;
    }
    
    return !allowed;
}

// =============================================================================
// BACKPRESSURE HANDLING
// =============================================================================

typedef enum {
    BACKPRESSURE_STRATEGY_DROP,
    BACKPRESSURE_STRATEGY_BLOCK,
    BACKPRESSURE_STRATEGY_SAMPLE,
    BACKPRESSURE_STRATEGY_BATCH
} BackpressureStrategy;

typedef struct {
    BackpressureStrategy strategy;
    float high_watermark;
    float low_watermark;
    uint32_t buffer_size;
    uint32_t current_size;
    uint32_t max_block_time_ms;
    
    // Sampling parameters
    uint32_t sample_rate; // 1 in N requests
    uint32_t sample_counter;
    
    // Statistics
    uint32_t total_events;
    uint32_t dropped_events;
    uint32_t blocked_events;
    uint32_t sampled_events;
    uint64_t total_block_time_ms;
} BackpressureManager;

// Initialize backpressure manager
void init_backpressure_manager(BackpressureManager* bp, BackpressureStrategy strategy,
                              uint32_t buffer_size, float high_watermark, float low_watermark) {
    memset(bp, 0, sizeof(BackpressureManager));
    bp->strategy = strategy;
    bp->buffer_size = buffer_size;
    bp->high_watermark = high_watermark;
    bp->low_watermark = low_watermark;
    bp->max_block_time_ms = 5000;
    bp->sample_rate = 10; // Sample 1 in 10 by default
}

// Check if backpressure should be applied
bool should_apply_backpressure(BackpressureManager* bp) {
    float utilization = (float)bp->current_size / bp->buffer_size;
    return utilization >= bp->high_watermark;
}

// Handle backpressure for incoming event
bool handle_backpressure(BackpressureManager* bp, Event* event) {
    bp->total_events++;
    
    if (!should_apply_backpressure(bp)) {
        return true; // No backpressure needed
    }
    
    switch (bp->strategy) {
        case BACKPRESSURE_STRATEGY_DROP:
            bp->dropped_events++;
            printf("    Backpressure: Dropping event %llu (buffer %d%% full)\n", 
                   (unsigned long long)event->id, 
                   (int)((float)bp->current_size / bp->buffer_size * 100));
            return false;
            
        case BACKPRESSURE_STRATEGY_BLOCK:
            {
                uint64_t start_time = (uint64_t)time(NULL) * 1000;
                uint64_t end_time = start_time + bp->max_block_time_ms;
                
                printf("    Backpressure: Blocking event %llu (buffer %d%% full)\n", 
                       (unsigned long long)event->id,
                       (int)((float)bp->current_size / bp->buffer_size * 100));
                
                while ((uint64_t)time(NULL) * 1000 < end_time && should_apply_backpressure(bp)) {
                    THREAD_SLEEP(100);
                }
                
                uint64_t block_time = (uint64_t)time(NULL) * 1000 - start_time;
                bp->total_block_time_ms += block_time;
                
                if (should_apply_backpressure(bp)) {
                    bp->blocked_events++;
                    return false; // Still under pressure after timeout
                }
                
                return true;
            }
            
        case BACKPRESSURE_STRATEGY_SAMPLE:
            bp->sample_counter++;
            if (bp->sample_counter % bp->sample_rate == 0) {
                bp->sampled_events++;
                printf("    Backpressure: Sampling event %llu (1 in %d)\n", 
                       (unsigned long long)event->id, bp->sample_rate);
                return true;
            } else {
                bp->dropped_events++;
                return false;
            }
            
        case BACKPRESSURE_STRATEGY_BATCH:
            // In a real implementation, we would batch multiple events
            // For this demo, we just apply a delay
            THREAD_SLEEP(100);
            return true;
    }
    
    return false;
}

// Update backpressure buffer size
void update_backpressure_buffer(BackpressureManager* bp, int delta) {
    if (delta > 0) {
        bp->current_size += delta;
    } else {
        bp->current_size = (bp->current_size >= (uint32_t)(-delta)) ? 
                          bp->current_size + delta : 0;
    }
}

// =============================================================================
// LAMBDA ARCHITECTURE IMPLEMENTATION
// =============================================================================

// Batch processing layer
typedef struct {
    Event* batch_data;
    uint32_t batch_size;
    uint32_t batch_capacity;
    uint64_t batch_start_time;
    uint32_t batch_interval_ms;
    
    // Results storage
    ProcessingResult* batch_results;
    uint32_t result_count;
    
    // Statistics
    uint32_t batches_processed;
    uint64_t total_processing_time_ms;
} BatchLayer;

// Speed processing layer (real-time)
typedef struct {
    Event* stream_buffer;
    uint32_t buffer_size;
    uint32_t buffer_capacity;
    volatile uint32_t head;
    volatile uint32_t tail;
    
    // Worker threads
    void* worker_threads[MAX_WORKERS];
    int worker_count;
    bool shutdown_requested;
    
    // Results
    ProcessingResult* stream_results;
    uint32_t stream_result_capacity;
    volatile uint32_t stream_result_count;
    
    // Statistics
    uint32_t events_processed;
    uint64_t total_latency_ms;
} SpeedLayer;

// Serving layer (combined results)
typedef struct {
    ProcessingResult* batch_view;
    uint32_t batch_view_size;
    ProcessingResult* real_time_view;
    uint32_t real_time_view_size;
    
    // Query cache
    char query_cache[1000][256];
    char cache_results[1000][512];
    int cache_size;
    
    // Statistics
    uint32_t queries_served;
    uint32_t cache_hits;
    uint32_t cache_misses;
} ServingLayer;

// Lambda architecture coordinator
typedef struct {
    BatchLayer batch_layer;
    SpeedLayer speed_layer;
    ServingLayer serving_layer;
    
    // Resilience components
    RetryConfig retry_config;
    RetryState retry_state;
    BulkheadManager bulkhead_manager;
    ThrottleManager throttle_manager;
    BackpressureManager backpressure_manager;
    
    // Statistics
    uint32_t total_events_received;
    uint32_t events_sent_to_batch;
    uint32_t events_sent_to_speed;
} LambdaArchitecture;

// Simulate event processing (can fail for demonstration)
bool process_event_simulation(void* context, Event* event, ProcessingResult* result) {
    uint64_t start_time = (uint64_t)clock();
    
    result->event_id = event->id;
    result->worker_id = 0; // Simplified
    
    // Simulate processing time
    THREAD_SLEEP(10 + (rand() % 50)); // 10-60ms processing time
    
    // Simulate failure conditions
    if (event->type == EVENT_SENSOR_DATA && (rand() % 10) == 0) {
        strcpy(result->error_message, "Sensor data validation failed");
        result->success = false;
        return false;
    }
    
    if (event->user_id == 12345 && (rand() % 5) == 0) {
        strcpy(result->error_message, "User service temporarily unavailable");
        result->success = false;
        return false;
    }
    
    // Success case
    result->success = true;
    result->processing_time_us = ((uint64_t)clock() - start_time) * 1000;
    strcpy(result->error_message, "Success");
    
    return true;
}

// Initialize lambda architecture
void init_lambda_architecture(LambdaArchitecture* lambda) {
    memset(lambda, 0, sizeof(LambdaArchitecture));
    
    // Initialize batch layer
    lambda->batch_layer.batch_capacity = 1000;
    lambda->batch_layer.batch_data = malloc(lambda->batch_layer.batch_capacity * sizeof(Event));
    lambda->batch_layer.batch_results = malloc(lambda->batch_layer.batch_capacity * sizeof(ProcessingResult));
    lambda->batch_layer.batch_interval_ms = 5000; // 5 second batches
    
    // Initialize speed layer
    lambda->speed_layer.buffer_capacity = 10000;
    lambda->speed_layer.stream_buffer = malloc(lambda->speed_layer.buffer_capacity * sizeof(Event));
    lambda->speed_layer.stream_results = malloc(lambda->speed_layer.buffer_capacity * sizeof(ProcessingResult));
    lambda->speed_layer.stream_result_capacity = lambda->speed_layer.buffer_capacity;
    lambda->speed_layer.worker_count = 4;
    
    // Initialize serving layer  
    lambda->serving_layer.batch_view = malloc(10000 * sizeof(ProcessingResult));
    lambda->serving_layer.real_time_view = malloc(10000 * sizeof(ProcessingResult));
    
    // Initialize resilience patterns
    lambda->retry_config.policy = RETRY_POLICY_EXPONENTIAL;
    lambda->retry_config.max_retries = 3;
    lambda->retry_config.base_delay_ms = 100;
    lambda->retry_config.max_delay_ms = 5000;
    lambda->retry_config.jitter_factor = 0.1f;
    lambda->retry_config.enable_circuit_breaker = true;
    
    create_bulkhead_compartment(&lambda->bulkhead_manager, "BatchProcessing", BULKHEAD_TYPE_THREAD_POOL, 4);
    create_bulkhead_compartment(&lambda->bulkhead_manager, "StreamProcessing", BULKHEAD_TYPE_THREAD_POOL, 8);
    create_bulkhead_compartment(&lambda->bulkhead_manager, "DatabaseAccess", BULKHEAD_TYPE_CONNECTION_POOL, 10);
    
    init_throttle_manager(&lambda->throttle_manager, THROTTLE_ALGORITHM_TOKEN_BUCKET, 100, 1000);
    init_backpressure_manager(&lambda->backpressure_manager, BACKPRESSURE_STRATEGY_SAMPLE, 10000, 0.8f, 0.6f);
}

// Process batch of events
void process_batch(LambdaArchitecture* lambda) {
    if (lambda->batch_layer.batch_size == 0) return;
    
    printf("\n=== Processing Batch of %d Events ===\n", lambda->batch_layer.batch_size);
    
    BulkheadCompartment* compartment = &lambda->bulkhead_manager.compartments[0]; // Batch processing compartment
    
    if (!acquire_bulkhead_resource(compartment, 10000)) {
        printf("Failed to acquire batch processing resources\n");
        return;
    }
    
    uint64_t batch_start = (uint64_t)clock();
    
    for (uint32_t i = 0; i < lambda->batch_layer.batch_size; i++) {
        Event* event = &lambda->batch_layer.batch_data[i];
        ProcessingResult* result = &lambda->batch_layer.batch_results[lambda->batch_layer.result_count];
        
        printf("  Processing batch event %llu (type: %d)\n", (unsigned long long)event->id, event->type);
        
        bool success = execute_with_retry(process_event_simulation, lambda, event, result,
                                        &lambda->retry_config, &lambda->retry_state);
        
        if (success) {
            lambda->batch_layer.result_count++;
            printf("    ✓ Batch processing successful\n");
        } else {
            printf("    ✗ Batch processing failed after retries\n");
        }
    }
    
    uint64_t batch_end = (uint64_t)clock();
    lambda->batch_layer.total_processing_time_ms += (batch_end - batch_start) / (CLOCKS_PER_SEC / 1000);
    lambda->batch_layer.batches_processed++;
    lambda->batch_layer.batch_size = 0; // Reset batch
    
    release_bulkhead_resource(compartment);
    
    printf("=== Batch Processing Complete ===\n");
}

// Add event to lambda architecture
bool add_event_to_lambda(LambdaArchitecture* lambda, const Event* event) {
    lambda->total_events_received++;
    
    // Apply throttling
    if (should_throttle_request(&lambda->throttle_manager)) {
        printf("Event %llu throttled\n", (unsigned long long)event->id);
        return false;
    }
    
    // Apply backpressure handling
    Event event_copy = *event;
    if (!handle_backpressure(&lambda->backpressure_manager, &event_copy)) {
        return false;
    }
    
    // Add to batch layer
    if (lambda->batch_layer.batch_size < lambda->batch_layer.batch_capacity) {
        lambda->batch_layer.batch_data[lambda->batch_layer.batch_size++] = *event;
        lambda->events_sent_to_batch++;
        
        // Check if batch is ready for processing
        uint64_t current_time = (uint64_t)time(NULL) * 1000;
        if (lambda->batch_layer.batch_start_time == 0) {
            lambda->batch_layer.batch_start_time = current_time;
        }
        
        if (current_time - lambda->batch_layer.batch_start_time >= lambda->batch_layer.batch_interval_ms ||
            lambda->batch_layer.batch_size >= lambda->batch_layer.batch_capacity) {
            process_batch(lambda);
            lambda->batch_layer.batch_start_time = 0;
        }
    }
    
    // Add to speed layer (simplified - direct processing)
    printf("Processing real-time event %llu\n", (unsigned long long)event->id);
    
    BulkheadCompartment* stream_compartment = &lambda->bulkhead_manager.compartments[1];
    if (acquire_bulkhead_resource(stream_compartment, 1000)) {
        update_backpressure_buffer(&lambda->backpressure_manager, 1);
        
        ProcessingResult result;
        bool success = execute_with_retry(process_event_simulation, lambda, &event_copy, &result,
                                        &lambda->retry_config, &lambda->retry_state);
        
        if (success) {
            lambda->speed_layer.events_processed++;
            printf("  ✓ Real-time processing successful\n");
        } else {
            printf("  ✗ Real-time processing failed\n");
        }
        
        update_backpressure_buffer(&lambda->backpressure_manager, -1);
        release_bulkhead_resource(stream_compartment);
        lambda->events_sent_to_speed++;
    } else {
        printf("  Failed to acquire stream processing resources\n");
    }
    
    return true;
}

// =============================================================================
// KAPPA ARCHITECTURE IMPLEMENTATION
// =============================================================================

typedef struct {
    Event* stream_buffer;
    uint32_t buffer_capacity;
    volatile uint32_t buffer_size;
    
    // Stream processing stages
    void* processing_threads[MAX_WORKERS];
    int thread_count;
    bool shutdown_requested;
    
    // Reprocessing capability
    Event* replay_buffer;
    uint32_t replay_capacity;
    uint32_t replay_size;
    bool replay_mode;
    
    // Results and views
    ProcessingResult* results;
    uint32_t result_capacity;
    volatile uint32_t result_count;
    
    // Resilience components (shared with Lambda)
    RetryConfig* retry_config;
    RetryState* retry_state;
    BulkheadManager* bulkhead_manager;
    ThrottleManager* throttle_manager;
    BackpressureManager* backpressure_manager;
    
    // Statistics
    uint32_t events_processed;
    uint32_t events_replayed;
    uint64_t total_latency_ms;
} KappaArchitecture;

// Initialize Kappa architecture
void init_kappa_architecture(KappaArchitecture* kappa, LambdaArchitecture* lambda_for_shared_resources) {
    memset(kappa, 0, sizeof(KappaArchitecture));
    
    kappa->buffer_capacity = 20000;
    kappa->stream_buffer = malloc(kappa->buffer_capacity * sizeof(Event));
    kappa->replay_capacity = 50000;
    kappa->replay_buffer = malloc(kappa->replay_capacity * sizeof(Event));
    kappa->result_capacity = 50000;
    kappa->results = malloc(kappa->result_capacity * sizeof(ProcessingResult));
    kappa->thread_count = 6;
    
    // Share resilience components with Lambda architecture
    if (lambda_for_shared_resources) {
        kappa->retry_config = &lambda_for_shared_resources->retry_config;
        kappa->retry_state = &lambda_for_shared_resources->retry_state;
        kappa->bulkhead_manager = &lambda_for_shared_resources->bulkhead_manager;
        kappa->throttle_manager = &lambda_for_shared_resources->throttle_manager;
        kappa->backpressure_manager = &lambda_for_shared_resources->backpressure_manager;
    }
}

// Process event in Kappa architecture
bool process_event_kappa(KappaArchitecture* kappa, const Event* event) {
    // Apply throttling
    if (should_throttle_request(kappa->throttle_manager)) {
        printf("Kappa: Event %llu throttled\n", (unsigned long long)event->id);
        return false;
    }
    
    // Apply backpressure
    Event event_copy = *event;
    if (!handle_backpressure(kappa->backpressure_manager, &event_copy)) {
        return false;
    }
    
    // Store for potential replay
    if (kappa->replay_size < kappa->replay_capacity) {
        kappa->replay_buffer[kappa->replay_size++] = *event;
    }
    
    printf("Kappa: Processing stream event %llu\n", (unsigned long long)event->id);
    
    // Use stream processing bulkhead
    BulkheadCompartment* compartment = &kappa->bulkhead_manager->compartments[1];
    if (!acquire_bulkhead_resource(compartment, 2000)) {
        printf("Kappa: Failed to acquire processing resources\n");
        return false;
    }
    
    update_backpressure_buffer(kappa->backpressure_manager, 1);
    
    ProcessingResult result;
    bool success = execute_with_retry(process_event_simulation, kappa, &event_copy, &result,
                                    kappa->retry_config, kappa->retry_state);
    
    if (success && kappa->result_count < kappa->result_capacity) {
        kappa->results[ATOMIC_INCREMENT(&kappa->result_count) - 1] = result;
        kappa->events_processed++;
        printf("  ✓ Kappa processing successful\n");
    } else if (!success) {
        printf("  ✗ Kappa processing failed\n");
    }
    
    update_backpressure_buffer(kappa->backpressure_manager, -1);
    release_bulkhead_resource(compartment);
    
    return success;
}

// Replay events in Kappa architecture (for reprocessing with updated logic)
void replay_events_kappa(KappaArchitecture* kappa, uint32_t from_index, uint32_t count) {
    if (from_index >= kappa->replay_size) return;
    
    uint32_t end_index = (from_index + count > kappa->replay_size) ? 
                        kappa->replay_size : from_index + count;
    
    printf("\n=== Kappa Replay: Processing %d events from index %d ===\n", 
           end_index - from_index, from_index);
    
    kappa->replay_mode = true;
    
    for (uint32_t i = from_index; i < end_index; i++) {
        Event* event = &kappa->replay_buffer[i];
        
        printf("  Replaying event %llu\n", (unsigned long long)event->id);
        
        ProcessingResult result;
        bool success = execute_with_retry(process_event_simulation, kappa, event, &result,
                                        kappa->retry_config, kappa->retry_state);
        
        if (success) {
            kappa->events_replayed++;
            printf("    ✓ Replay successful\n");
        } else {
            printf("    ✗ Replay failed\n");
        }
    }
    
    kappa->replay_mode = false;
    printf("=== Kappa Replay Complete ===\n");
}

// =============================================================================
// DEMONSTRATION AND TESTING
// =============================================================================

void print_resilience_statistics(const LambdaArchitecture* lambda) {
    printf("\n=== Resilience Pattern Statistics ===\n");
    
    // Retry pattern stats
    printf("Retry Pattern:\n");
    printf("  Success count: %d\n", lambda->retry_state.success_count);
    printf("  Failure count: %d\n", lambda->retry_state.failure_count);
    printf("  Circuit breaker: %s\n", lambda->retry_state.circuit_open ? "OPEN" : "CLOSED");
    
    // Bulkhead pattern stats
    printf("\nBulkhead Pattern:\n");
    for (int i = 0; i < lambda->bulkhead_manager.compartment_count; i++) {
        const BulkheadCompartment* comp = &lambda->bulkhead_manager.compartments[i];
        printf("  %s: %d/%d used, %d success, %d failures, %d timeouts\n",
               comp->name, comp->current_usage, comp->max_capacity,
               comp->successful_requests, comp->failed_requests, comp->timeouts);
    }
    
    // Throttling stats
    printf("\nThrottling:\n");
    printf("  Total requests: %d\n", lambda->throttle_manager.total_requests);
    printf("  Allowed requests: %d\n", lambda->throttle_manager.allowed_requests);
    printf("  Throttled requests: %d\n", lambda->throttle_manager.throttled_requests);
    printf("  Current tokens: %d\n", lambda->throttle_manager.current_tokens);
    
    // Backpressure stats
    printf("\nBackpressure:\n");
    printf("  Total events: %d\n", lambda->backpressure_manager.total_events);
    printf("  Dropped events: %d\n", lambda->backpressure_manager.dropped_events);
    printf("  Blocked events: %d\n", lambda->backpressure_manager.blocked_events);
    printf("  Sampled events: %d\n", lambda->backpressure_manager.sampled_events);
    printf("  Buffer utilization: %.1f%%\n", 
           ((float)lambda->backpressure_manager.current_size / lambda->backpressure_manager.buffer_size) * 100);
}

void print_architecture_statistics(const LambdaArchitecture* lambda, const KappaArchitecture* kappa) {
    printf("\n=== Architecture Statistics ===\n");
    
    printf("Lambda Architecture:\n");
    printf("  Total events received: %d\n", lambda->total_events_received);
    printf("  Events sent to batch: %d\n", lambda->events_sent_to_batch);
    printf("  Events sent to speed: %d\n", lambda->events_sent_to_speed);
    printf("  Batches processed: %d\n", lambda->batch_layer.batches_processed);
    printf("  Speed layer events: %d\n", lambda->speed_layer.events_processed);
    printf("  Batch results: %d\n", lambda->batch_layer.result_count);
    
    printf("\nKappa Architecture:\n");
    printf("  Events processed: %d\n", kappa->events_processed);
    printf("  Events replayed: %d\n", kappa->events_replayed);
    printf("  Replay buffer size: %d\n", kappa->replay_size);
    printf("  Total results: %d\n", kappa->result_count);
}

void demonstrate_lambda_architecture() {
    printf("\n=== Lambda Architecture Demonstration ===\n");
    
    LambdaArchitecture lambda;
    init_lambda_architecture(&lambda);
    
    // Generate test events
    Event test_events[] = {
        {1, EVENT_USER_ACTION, (uint64_t)time(NULL), 1001, "user_login", 10, 1, 0, false},
        {2, EVENT_TRANSACTION, (uint64_t)time(NULL), 1002, "purchase_item", 12, 2, 0, false},
        {3, EVENT_SENSOR_DATA, (uint64_t)time(NULL), 0, "temp_reading_25.6", 15, 3, 0, false},
        {4, EVENT_USER_ACTION, (uint64_t)time(NULL), 12345, "user_logout", 11, 1, 0, false}, // Will fail sometimes
        {5, EVENT_LOG_ENTRY, (uint64_t)time(NULL), 0, "system_startup", 14, 4, 0, false},
        {6, EVENT_TRANSACTION, (uint64_t)time(NULL), 1003, "refund_request", 13, 2, 0, false},
        {7, EVENT_SENSOR_DATA, (uint64_t)time(NULL), 0, "pressure_reading_1.2", 18, 3, 0, false},
        {8, EVENT_HEARTBEAT, (uint64_t)time(NULL), 0, "service_alive", 12, 5, 0, false}
    };
    
    int event_count = sizeof(test_events) / sizeof(test_events[0]);
    
    printf("Processing %d events through Lambda architecture...\n", event_count);
    
    for (int i = 0; i < event_count; i++) {
        printf("\n--- Processing Event %d ---\n", i + 1);
        add_event_to_lambda(&lambda, &test_events[i]);
        THREAD_SLEEP(500); // Simulate real-time arrival
    }
    
    // Force process any remaining batch
    if (lambda.batch_layer.batch_size > 0) {
        process_batch(&lambda);
    }
    
    // Cleanup
    free(lambda.batch_layer.batch_data);
    free(lambda.batch_layer.batch_results);
    free(lambda.speed_layer.stream_buffer);
    free(lambda.speed_layer.stream_results);
    free(lambda.serving_layer.batch_view);
    free(lambda.serving_layer.real_time_view);
}

void demonstrate_kappa_architecture() {
    printf("\n=== Kappa Architecture Demonstration ===\n");
    
    // Create shared lambda for resource sharing
    LambdaArchitecture lambda;
    init_lambda_architecture(&lambda);
    
    KappaArchitecture kappa;
    init_kappa_architecture(&kappa, &lambda);
    
    // Generate test events
    Event stream_events[] = {
        {101, EVENT_USER_ACTION, (uint64_t)time(NULL), 2001, "stream_login", 11, 1, 0, false},
        {102, EVENT_TRANSACTION, (uint64_t)time(NULL), 2002, "stream_purchase", 14, 2, 0, false},
        {103, EVENT_SENSOR_DATA, (uint64_t)time(NULL), 0, "stream_temp_22.1", 16, 3, 0, false},
        {104, EVENT_USER_ACTION, (uint64_t)time(NULL), 12345, "stream_action", 12, 1, 0, false},
        {105, EVENT_LOG_ENTRY, (uint64_t)time(NULL), 0, "stream_log_entry", 15, 4, 0, false}
    };
    
    int stream_count = sizeof(stream_events) / sizeof(stream_events[0]);
    
    printf("Processing %d events through Kappa architecture...\n", stream_count);
    
    for (int i = 0; i < stream_count; i++) {
        printf("\n--- Processing Stream Event %d ---\n", i + 1);
        process_event_kappa(&kappa, &stream_events[i]);
        THREAD_SLEEP(300);
    }
    
    // Demonstrate replay capability
    printf("\nDemonstrating replay capability...\n");
    replay_events_kappa(&kappa, 0, 3); // Replay first 3 events
    
    // Cleanup
    free(kappa.stream_buffer);
    free(kappa.replay_buffer);
    free(kappa.results);
    
    free(lambda.batch_layer.batch_data);
    free(lambda.batch_layer.batch_results);
    free(lambda.speed_layer.stream_buffer);
    free(lambda.speed_layer.stream_results);
    free(lambda.serving_layer.batch_view);
    free(lambda.serving_layer.real_time_view);
}

void demonstrate_resilience_patterns() {
    printf("\n=== Resilience Patterns Demonstration ===\n");
    
    LambdaArchitecture lambda;
    init_lambda_architecture(&lambda);
    
    // Test retry pattern with failing events
    printf("\nTesting Retry Pattern:\n");
    Event failing_event = {999, EVENT_SENSOR_DATA, (uint64_t)time(NULL), 12345, "failing_sensor", 12, 1, 0, false};
    
    ProcessingResult result;
    printf("Attempting to process event that will fail...\n");
    bool success = execute_with_retry(process_event_simulation, &lambda, &failing_event, &result,
                                    &lambda.retry_config, &lambda.retry_state);
    printf("Final result: %s\n", success ? "SUCCESS" : "FAILED");
    
    // Test throttling
    printf("\nTesting Throttling Pattern:\n");
    for (int i = 0; i < 150; i++) { // More than throttle limit
        bool throttled = should_throttle_request(&lambda.throttle_manager);
        if (throttled && i % 10 == 0) {
            printf("Request %d: THROTTLED\n", i + 1);
        }
    }
    
    // Test backpressure
    printf("\nTesting Backpressure Pattern:\n");
    lambda.backpressure_manager.current_size = (uint32_t)(lambda.backpressure_manager.buffer_size * 0.9f); // Simulate high load
    
    Event bp_event = {888, EVENT_USER_ACTION, (uint64_t)time(NULL), 3001, "backpressure_test", 15, 1, 0, false};
    bool accepted = handle_backpressure(&lambda.backpressure_manager, &bp_event);
    printf("Event under backpressure: %s\n", accepted ? "ACCEPTED" : "REJECTED/SAMPLED");
    
    // Cleanup
    free(lambda.batch_layer.batch_data);
    free(lambda.batch_layer.batch_results);
    free(lambda.speed_layer.stream_buffer);
    free(lambda.speed_layer.stream_results);
    free(lambda.serving_layer.batch_view);
    free(lambda.serving_layer.real_time_view);
}

int main() {
    printf("Lambda/Kappa Architecture with Resilience Patterns\n");
    printf("==================================================\n");
    
    srand((unsigned int)time(NULL));
    
    // Demonstrate Lambda Architecture
    demonstrate_lambda_architecture();
    
    // Demonstrate Kappa Architecture  
    demonstrate_kappa_architecture();
    
    // Demonstrate Resilience Patterns
    demonstrate_resilience_patterns();
    
    // Create final combined demonstration
    printf("\n=== Combined Architecture Comparison ===\n");
    
    LambdaArchitecture lambda;
    init_lambda_architecture(&lambda);
    
    KappaArchitecture kappa;
    init_kappa_architecture(&kappa, &lambda);
    
    // Process same events through both architectures
    Event comparison_events[] = {
        {201, EVENT_USER_ACTION, (uint64_t)time(NULL), 4001, "comparison_event_1", 16, 1, 0, false},
        {202, EVENT_TRANSACTION, (uint64_t)time(NULL), 4002, "comparison_event_2", 18, 2, 0, false},
        {203, EVENT_SENSOR_DATA, (uint64_t)time(NULL), 0, "comparison_event_3", 20, 3, 0, false}
    };
    
    printf("Processing events through both architectures...\n");
    
    for (int i = 0; i < 3; i++) {
        printf("\n--- Event %d ---\n", i + 1);
        printf("Lambda: ");
        add_event_to_lambda(&lambda, &comparison_events[i]);
        printf("Kappa:  ");
        process_event_kappa(&kappa, &comparison_events[i]);
    }
    
    // Print final statistics
    print_architecture_statistics(&lambda, &kappa);
    print_resilience_statistics(&lambda);
    
    printf("\n=== Architecture Pattern Summary ===\n");
    
    printf("\nLambda Architecture:\n");
    printf("✓ Handles both batch and real-time processing\n");
    printf("✓ Provides comprehensive historical analysis\n");
    printf("✓ Fault-tolerant through redundant processing paths\n");
    printf("✓ Complex to maintain due to dual processing logic\n");
    printf("✓ Higher latency for batch processing\n");
    
    printf("\nKappa Architecture:\n");
    printf("✓ Simpler single processing pipeline\n");
    printf("✓ Lower latency for all processing\n");
    printf("✓ Easier to maintain and debug\n");
    printf("✓ Replay capability for reprocessing\n");
    printf("✓ May require more sophisticated stream processing\n");
    
    printf("\nResilience Patterns:\n");
    printf("✓ Retry Pattern: Handles transient failures gracefully\n");
    printf("✓ Bulkhead Pattern: Prevents cascading failures\n");
    printf("✓ Throttling: Protects against overload\n");
    printf("✓ Backpressure: Manages flow control under pressure\n");
    printf("✓ Circuit Breaker: Fails fast when services are down\n");
    
    printf("\nReal-World Applications:\n");
    printf("• Lambda: Netflix data processing, AWS analytics\n");
    printf("• Kappa: LinkedIn stream processing, Spotify recommendations\n");
    printf("• Resilience: Microservices, distributed systems, cloud platforms\n");
    
    // Cleanup
    free(lambda.batch_layer.batch_data);
    free(lambda.batch_layer.batch_results);
    free(lambda.speed_layer.stream_buffer);
    free(lambda.speed_layer.stream_results);
    free(lambda.serving_layer.batch_view);
    free(lambda.serving_layer.real_time_view);
    
    free(kappa.stream_buffer);
    free(kappa.replay_buffer);
    free(kappa.results);
    
    printf("\nDistributed systems architecture demonstration completed!\n");
    
    return 0;
}