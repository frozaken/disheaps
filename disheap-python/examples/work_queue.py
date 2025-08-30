#!/usr/bin/env python3
"""
Work queue pattern example for the Disheap Python SDK.

This example demonstrates a realistic work queue pattern where:
- A producer enqueues different types of work items with priorities
- Multiple consumers process work items concurrently
- Work items have different processing requirements
- Error handling with retries and dead letter queue (DLQ)
"""

import asyncio
import json
import logging
import random
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, Any
from enum import Enum

from disheap import DisheapClient, DisheapError, EnqueueRequest


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Types of work tasks."""
    EMAIL_SEND = "email_send"
    IMAGE_RESIZE = "image_resize"
    DATA_EXPORT = "data_export"
    REPORT_GENERATE = "report_generate"
    CLEANUP = "cleanup"


@dataclass
class WorkTask:
    """Represents a work task to be processed."""
    task_id: str
    task_type: TaskType
    priority: int
    parameters: Dict[str, Any]
    created_at: str
    timeout_seconds: int = 300  # 5 minutes default
    
    def to_json(self) -> str:
        """Serialize task to JSON."""
        data = asdict(self)
        data['task_type'] = self.task_type.value
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WorkTask':
        """Deserialize task from JSON."""
        data = json.loads(json_str)
        data['task_type'] = TaskType(data['task_type'])
        return cls(**data)


class TaskProducer:
    """Produces work tasks and enqueues them based on priority."""
    
    def __init__(self, client: DisheapClient):
        self.client = client
    
    async def submit_task(self, task: WorkTask) -> str:
        """
        Submit a work task for processing.
        
        Args:
            task: Work task to submit
            
        Returns:
            Message ID of the enqueued task
        """
        async with self.client.producer() as producer:
            result = await producer.enqueue(
                topic="work-queue",
                payload=task.to_json().encode('utf-8'),
                priority=task.priority,
                partition_key=task.task_type.value,  # Partition by task type
                max_retries=5
            )
            
            logger.info(
                f"Submitted task {task.task_id} ({task.task_type.value}) "
                f"with priority {task.priority} -> message {result.message_id}"
            )
            
            return result.message_id
    
    async def submit_batch_tasks(self, tasks: list[WorkTask]) -> Dict[str, str]:
        """
        Submit multiple tasks as a batch for better performance.
        
        Args:
            tasks: List of work tasks to submit
            
        Returns:
            Mapping of task_id -> message_id
        """
        requests = [
            EnqueueRequest(
                topic="work-queue",
                payload=task.to_json().encode('utf-8'),
                priority=task.priority,
                partition_key=task.task_type.value,
                max_retries=5
            )
            for task in tasks
        ]
        
        async with self.client.producer() as producer:
            batch_result = await producer.enqueue_batch(requests)
            
            task_to_message = {}
            for i, (task, result) in enumerate(zip(tasks, batch_result.results)):
                task_to_message[task.task_id] = result.message_id
                
                logger.info(
                    f"Batch submitted task {task.task_id} -> message {result.message_id}"
                )
            
            logger.info(
                f"Submitted batch of {len(tasks)} tasks "
                f"(transaction: {batch_result.transaction_id})"
            )
            
            return task_to_message


class TaskProcessor:
    """Processes work tasks from the queue."""
    
    def __init__(self, client: DisheapClient, processor_id: str):
        self.client = client
        self.processor_id = processor_id
        self.processed_count = 0
    
    async def process_task(self, task: WorkTask) -> bool:
        """
        Process a single work task.
        
        Args:
            task: Work task to process
            
        Returns:
            True if processing succeeded
            
        Raises:
            Exception: If processing fails and should be retried
        """
        logger.info(
            f"[{self.processor_id}] Processing task {task.task_id} "
            f"({task.task_type.value}, priority={task.priority})"
        )
        
        try:
            # Simulate different processing times based on task type
            processing_time = self._get_processing_time(task.task_type)
            
            # Simulate the actual work
            await asyncio.sleep(processing_time)
            
            # Simulate occasional failures based on task type
            failure_rate = self._get_failure_rate(task.task_type)
            if random.random() < failure_rate:
                raise RuntimeError(f"Simulated failure for task {task.task_id}")
            
            self.processed_count += 1
            
            logger.info(
                f"[{self.processor_id}] Completed task {task.task_id} "
                f"in {processing_time:.1f}s"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"[{self.processor_id}] Failed to process task {task.task_id}: {e}"
            )
            raise
    
    def _get_processing_time(self, task_type: TaskType) -> float:
        """Get simulated processing time for task type."""
        base_times = {
            TaskType.EMAIL_SEND: 1.0,
            TaskType.IMAGE_RESIZE: 3.0,
            TaskType.DATA_EXPORT: 5.0,
            TaskType.REPORT_GENERATE: 8.0,
            TaskType.CLEANUP: 0.5,
        }
        
        base_time = base_times.get(task_type, 2.0)
        # Add some randomness
        return base_time * random.uniform(0.5, 1.5)
    
    def _get_failure_rate(self, task_type: TaskType) -> float:
        """Get simulated failure rate for task type."""
        failure_rates = {
            TaskType.EMAIL_SEND: 0.05,      # 5% failure rate
            TaskType.IMAGE_RESIZE: 0.10,    # 10% failure rate  
            TaskType.DATA_EXPORT: 0.15,     # 15% failure rate
            TaskType.REPORT_GENERATE: 0.20, # 20% failure rate
            TaskType.CLEANUP: 0.02,         # 2% failure rate
        }
        
        return failure_rates.get(task_type, 0.10)
    
    async def start_processing(self, max_tasks: int = None) -> None:
        """
        Start processing tasks from the work queue.
        
        Args:
            max_tasks: Maximum number of tasks to process (None for unlimited)
        """
        logger.info(f"[{self.processor_id}] Starting task processing...")
        
        async with self.client.consumer() as consumer:
            task_count = 0
            
            async for message in consumer.pop_stream(
                topic="work-queue",
                prefetch=3,  # Process up to 3 tasks concurrently
                visibility_timeout=timedelta(minutes=10)
            ):
                if max_tasks and task_count >= max_tasks:
                    logger.info(f"[{self.processor_id}] Reached max tasks limit")
                    break
                
                try:
                    # Parse the task from message payload
                    task = WorkTask.from_json(message.payload_as_text())
                    
                    # Check if task has timed out
                    created_time = datetime.fromisoformat(task.created_at)
                    if datetime.now() - created_time > timedelta(seconds=task.timeout_seconds):
                        logger.warning(
                            f"[{self.processor_id}] Task {task.task_id} has timed out, skipping"
                        )
                        async with message:
                            pass  # Will be auto-acked (removed)
                        continue
                    
                    # Process the task with automatic ack/nack
                    async with message:
                        await self.process_task(task)
                    
                    task_count += 1
                    
                except json.JSONDecodeError as e:
                    logger.error(f"[{self.processor_id}] Invalid task JSON: {e}")
                    async with message:
                        pass  # Auto-ack to remove invalid message
                
                except Exception as e:
                    logger.error(f"[{self.processor_id}] Task processing error: {e}")
                    # Message will be auto-nacked by context manager
        
        logger.info(
            f"[{self.processor_id}] Finished processing. "
            f"Total processed: {self.processed_count}"
        )


async def create_sample_tasks() -> list[WorkTask]:
    """Create sample work tasks for demonstration."""
    tasks = []
    
    # High priority tasks
    tasks.extend([
        WorkTask(
            task_id=f"urgent-email-{i}",
            task_type=TaskType.EMAIL_SEND,
            priority=100,
            parameters={"recipient": f"user{i}@example.com", "template": "urgent"},
            created_at=datetime.now().isoformat()
        )
        for i in range(3)
    ])
    
    # Medium priority tasks
    tasks.extend([
        WorkTask(
            task_id=f"image-resize-{i}",
            task_type=TaskType.IMAGE_RESIZE,
            priority=50,
            parameters={"image_id": f"img_{i}", "width": 800, "height": 600},
            created_at=datetime.now().isoformat()
        )
        for i in range(5)
    ])
    
    # Low priority tasks
    tasks.extend([
        WorkTask(
            task_id=f"cleanup-{i}",
            task_type=TaskType.CLEANUP,
            priority=10,
            parameters={"directory": f"/tmp/cleanup_{i}"},
            created_at=datetime.now().isoformat()
        )
        for i in range(2)
    ])
    
    # Very high priority report
    tasks.append(
        WorkTask(
            task_id="quarterly-report",
            task_type=TaskType.REPORT_GENERATE,
            priority=200,
            parameters={"type": "quarterly", "quarter": "Q4_2024"},
            created_at=datetime.now().isoformat(),
            timeout_seconds=600  # 10 minutes
        )
    )
    
    return tasks


async def main():
    """Main example function."""
    
    # Create client
    client = DisheapClient(
        endpoints=["localhost:9090"],
        api_key="dh_example_key_secret123"
    )
    
    async with client:
        logger.info("Connected to Disheap engine")
        
        # Ensure work queue topic exists
        try:
            await client.create_topic(
                topic="work-queue",
                mode="max",  # Higher priority first
                partitions=4,
                replication_factor=2,
                max_retries=5
            )
            logger.info("Created work-queue topic")
        except DisheapError as e:
            if "already exists" not in str(e).lower():
                raise
        
        # Create task producer
        producer = TaskProducer(client)
        
        # Generate and submit sample tasks
        logger.info("Creating sample tasks...")
        tasks = await create_sample_tasks()
        
        # Submit tasks in batches for better performance
        batch_size = 5
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            await producer.submit_batch_tasks(batch)
        
        logger.info(f"Submitted {len(tasks)} tasks to the work queue")
        
        # Start multiple processors to handle tasks concurrently
        processors = [
            TaskProcessor(client, f"processor-{i}")
            for i in range(3)
        ]
        
        # Run processors concurrently for a limited time
        logger.info("Starting task processors...")
        
        processor_tasks = [
            asyncio.create_task(processor.start_processing(max_tasks=8))
            for processor in processors
        ]
        
        # Wait for all processors to complete
        await asyncio.gather(*processor_tasks, return_exceptions=True)
        
        # Check final stats
        stats = await client.get_stats("work-queue")
        logger.info(f"Final queue stats: {stats}")
        
        total_processed = sum(p.processed_count for p in processors)
        logger.info(f"Total tasks processed across all processors: {total_processed}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Work queue example failed: {e}")
        raise
