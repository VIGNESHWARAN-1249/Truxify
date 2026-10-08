import kafka, { TOPICS, CONSUMER_GROUPS } from '../config/kafka.config.js';
import processedEventRepository from '../repositories/processedEvent.repository.js';
import deadLetterRepository from '../repositories/deadLetter.repository.js';
import orderReadModel from '../cqrs/order.read.model.js';
import logger from '../../api/src/middleware/logger.js';

const ORDER_READ_MODEL_TOPICS = new Set([
  TOPICS.ORDER_CREATED,
  TOPICS.ORDER_UPDATED,
  TOPICS.ORDER_CANCELLED,
  TOPICS.DRIVER_ASSIGNED,
]);

const MAX_REPLAY_ATTEMPTS = 3;

class OrderConsumer {
  constructor({ eventBus: externalEventBus } = {}) {
    this.handlers = new Map();
    this.initialized = false;
    this._eventBus = externalEventBus || null;
    this.createdConsumerGroups = [];
  }

  setEventBus(eventBus) {
    this._eventBus = eventBus;
  }

  /**
   * Get event ID consistently everywhere.
   *
   * Priority:
   * 1. metadata.eventId
   * 2. top-level eventId
   * 3. Kafka message key
   */
  getEventId(message, rawMessage = null) {
    return (
      message?.metadata?.eventId ||
      message?.eventId ||
      rawMessage?.key?.toString() ||
      null
    );
  }

  /**
   * Get order ID consistently everywhere.
   */
  getOrderId(message, rawMessage = null) {
    return (
      message?.aggregateId ||
      message?.orderId ||
      message?.payload?.orderId ||
      rawMessage?.key?.toString() ||
      null
    );
  }

  async initialize() {
    if (this.initialized) return;

    await kafka.createConsumer(
      CONSUMER_GROUPS.ORDER_SERVICE,
      [
        TOPICS.ORDER_CREATED,
        TOPICS.ORDER_UPDATED,
        TOPICS.ORDER_CANCELLED,
        TOPICS.DRIVER_ASSIGNED,
        TOPICS.PAYMENT_CONFIRMED,
        TOPICS.TRIP_STARTED,
        TOPICS.TRIP_COMPLETED,
        TOPICS.ESCROW_CREATED,
        TOPICS.ESCROW_RELEASED,
      ]
    );

    await kafka.createConsumer(
      CONSUMER_GROUPS.NOTIFICATION_SERVICE,
      [
        TOPICS.ORDER_CREATED,
        TOPICS.DRIVER_ASSIGNED,
        TOPICS.PAYMENT_CONFIRMED,
        TOPICS.ESCROW_RELEASED,
        TOPICS.NOTIFICATION_SENT,
      ]
    );

    await kafka.createConsumer(
      CONSUMER_GROUPS.ANALYTICS_SERVICE,
      [
        TOPICS.ORDER_CREATED,
        TOPICS.ORDER_UPDATED,
        TOPICS.ORDER_CANCELLED,
        TOPICS.DRIVER_ASSIGNED,
        TOPICS.PAYMENT_CONFIRMED,
        TOPICS.TRIP_STARTED,
        TOPICS.TRIP_COMPLETED,
        TOPICS.ETA_UPDATED,
        TOPICS.LOCATION_UPDATED,
      ]
    );

    await kafka.createConsumer(
      CONSUMER_GROUPS.FRAUD_SERVICE,
      [
        TOPICS.ORDER_CREATED,
        TOPICS.PAYMENT_CONFIRMED,
        TOPICS.FRAUD_DETECTED,
      ]
    );

    this.createdConsumerGroups = [
      CONSUMER_GROUPS.ORDER_SERVICE,
      CONSUMER_GROUPS.NOTIFICATION_SERVICE,
      CONSUMER_GROUPS.ANALYTICS_SERVICE,
      CONSUMER_GROUPS.FRAUD_SERVICE,
    ];

    this.initialized = true;

    logger.info('✅ Kafka consumers initialized');
  }

  registerHandler(topic, handler) {
    if (!this.handlers.has(topic)) {
      this.handlers.set(topic, []);
    }

    this.handlers.get(topic).push(handler);
  }

  registerHandlerViaEventBus(eventType, handler) {
    if (this._eventBus) {
      this._eventBus.subscribe(eventType, handler);

      logger.info(
        `[OrderConsumer] Registered EventBus handler for "${eventType}"`
      );
    } else {
      logger.warn(
        '[OrderConsumer] No EventBus set, falling back to direct handler registration'
      );

      this.registerHandler(eventType, handler);
    }
  }

  /**
   * Safely persist a dead-letter record.
   *
   * DLQ persistence itself must not crash the consumer.
   */
  async safeStoreDeadLetter(topic, rawMessage, error) {
    try {
      await this.storeDeadLetter(topic, rawMessage, error);
    } catch (dlqError) {
      logger.error(
        `❌ Failed to persist dead letter for ${topic}:`,
        dlqError
      );
    }
  }

  /**
   * Publish to EventBus.
   *
   * Always preserve the original event object when possible so
   * event-id based deduplication can work consistently.
   */
  async publishToEventBus(topic, message, groupId) {
    if (!this._eventBus) {
      return;
    }

    const eventType = topic
      .replace(/\./g, '_')
      .toUpperCase();

    if (
      message &&
      typeof message === 'object'
    ) {
      await this._eventBus.publish(
        message,
        {
          adapters: [],
          source: `kafka:${groupId}`,
        }
      );
    } else {
      await this._eventBus.publish(
        eventType,
        message,
        {
          adapters: [],
          source: `kafka:${groupId}`,
        }
      );
    }
  }

  /**
   * Execute normal registered handlers.
   */
  async executeHandlers(topic, message, rawMessage) {
    const topicHandlers = this.handlers.get(topic) || [];

    for (const handler of topicHandlers) {
      await handler(message, rawMessage);
    }
  }

  /**
   * Apply order read model.
   *
   * Returns false when the event was already applied.
   */
  async applyOrderReadModel(
    topic,
    message,
    rawMessage,
    groupId
  ) {
    const eventId = this.getEventId(
      message,
      rawMessage
    );

    const orderId = this.getOrderId(
      message,
      rawMessage
    );

    return orderReadModel.applyEvent({
      topic,
      eventId,
      orderId,
      eventType: message?.eventType,
      payload: message?.payload,
      version: message?.version,
      consumerGroup: groupId,
    });
  }

  /**
   * Process an incoming Kafka message.
   *
   * This is the single source of truth for processing behavior.
   */
  async processMessage(
    topic,
    message,
    rawMessage,
    groupId
  ) {
    const isReadModelTopic =
      ORDER_READ_MODEL_TOPICS.has(topic);

    /**
     * READ MODEL
     *
     * Projection is atomically idempotent.
     */
    if (isReadModelTopic) {
      const eventId = this.getEventId(
        message,
        rawMessage
      );

      try {
        const applied =
          await this.applyOrderReadModel(
            topic,
            message,
            rawMessage,
            groupId
          );

        if (!applied) {
          logger.info(
            `[OrderConsumer] Duplicate read-model event ${eventId} on ${topic}; skipping`
          );
        }

        /**
         * Important:
         *
         * These topics are projection-only topics.
         * We intentionally do NOT execute side-effect handlers here.
         *
         * This keeps live processing and replay behavior identical.
         */
        return {
          success: true,
          skipped: !applied,
          eventId,
        };
      } catch (error) {
        logger.error(
          `Read-model processing failed for ${topic}:`,
          error
        );

        await this.safeStoreDeadLetter(
          topic,
          rawMessage,
          error
        );

        return {
          success: false,
          eventId,
          error,
        };
      }
    }

    /**
     * SIDE-EFFECT TOPICS
     */
    const eventId = this.getEventId(
      message,
      rawMessage
    );

    const orderId = this.getOrderId(
      message,
      rawMessage
    );

    /**
     * A side-effect event without an ID cannot be safely
     * deduplicated.
     */
    if (!eventId) {
      const error = new Error(
        `Missing eventId for side-effect topic ${topic}`
      );

      logger.error(
        `[OrderConsumer] ${error.message}`
      );

      await this.safeStoreDeadLetter(
        topic,
        rawMessage,
        error
      );

      return {
        success: false,
        eventId: null,
        error,
      };
    }

    let claimed = false;

    try {
      /**
       * Atomically claim the event.
       */
      claimed =
        await processedEventRepository.claimProcessing(
          topic,
          eventId,
          orderId,
          groupId
        );

      if (!claimed) {
        logger.info(
          `[OrderConsumer] Duplicate/active event ${eventId} on ${topic}; skipping`
        );

        return {
          success: true,
          skipped: true,
          eventId,
        };
      }

      /**
       * Execute direct handlers.
       */
      await this.executeHandlers(
        topic,
        message,
        rawMessage
      );

      /**
       * Execute EventBus fan-out.
       *
       * If this fails, the whole event remains retryable.
       */
      await this.publishToEventBus(
        topic,
        message,
        groupId
      );

      /**
       * Only mark completed after ALL side effects succeed.
       */
      const completed =
        await processedEventRepository.markCompleted(
          topic,
          eventId,
          groupId
        );

      if (completed === false) {
        logger.warn(
          `[OrderConsumer] Event ${eventId} on ${topic} was not completed because its claim expired or was superseded`
        );

        return {
          success: false,
          eventId,
        };
      }

      return {
        success: true,
        eventId,
      };
    } catch (error) {
      logger.error(
        `❌ Processing failed for ${topic}, event ${eventId}:`,
        error
      );

      await this.safeStoreDeadLetter(
        topic,
        rawMessage,
        error
      );

      /**
       * Only the worker that successfully claimed the event
       * should transition it to failed.
       */
      if (claimed) {
        try {
          await processedEventRepository.markFailed(
            topic,
            eventId,
            groupId
          );
        } catch (statusError) {
          logger.error(
            `[OrderConsumer] Failed to mark event ${eventId} as failed:`,
            statusError
          );
        }
      }

      return {
        success: false,
        eventId,
        error,
      };
    }
  }

  async startConsuming(groupId) {
    await kafka.getConsumer(groupId);

    const messageHandler = async (
      topic,
      message,
      rawMessage
    ) => {
      await this.processMessage(
        topic,
        message,
        rawMessage,
        groupId
      );
    };

    await kafka.consumeMessages(
      groupId,
      messageHandler,
      async (error, topic, message) => {
        logger.error(
          `Dead letter: ${topic}`,
          {
            error: error?.message,
          }
        );

        await this.safeStoreDeadLetter(
          topic,
          message,
          error
        );
      }
    );
  }

  async storeDeadLetter(
    topic,
    message,
    error
  ) {
    const rawValue = message?.value;

    const serialized =
      Buffer.isBuffer(rawValue)
        ? rawValue.toString()
        : rawValue != null
          ? String(rawValue)
          : null;

    const dlqEntry = {
      topic,
      message: serialized,
      error: error?.message || String(error),
      timestamp: new Date().toISOString(),
      retryCount: 0,
    };

    const stored =
      await deadLetterRepository.store({
        topic,
        message: dlqEntry,
        error: error?.message || String(error),
        retryCount: 0,
      });

    if (stored) {
      logger.info(
        `📦 Dead letter persisted for ${topic} (id: ${stored.id})`
      );
    } else {
      logger.error(
        `📦 Dead letter for ${topic} could NOT be persisted — message dropped`,
        dlqEntry
      );
    }

    return stored;
  }

  /**
   * Parse original message from DLQ.
   */
  parseDeadLetterMessage(entry) {
    const serialized =
      typeof entry.message === 'string'
        ? entry.message
        : entry.message?.message;

    if (!serialized) {
      throw new Error(
        `Dead letter ${entry.id} contains no original message`
      );
    }

    return JSON.parse(serialized);
  }

  async replayDeadLetters({
    topic = null,
    limit = 50,
    consumerGroup = CONSUMER_GROUPS.ORDER_SERVICE,
  } = {}) {
    const pending =
      await deadLetterRepository.listPending({
        topic,
        limit,
      });

    const results = {
      attempted: pending.length,
      succeeded: 0,
      failed: 0,
      skipped: 0,
    };

    const groupId =
      consumerGroup ||
      CONSUMER_GROUPS.ORDER_SERVICE;

    for (const entry of pending) {
      const currentTopic = entry.topic;

      let parsedMessage;

      /**
       * STEP 1:
       * Parse DLQ payload.
       */
      try {
        parsedMessage =
          this.parseDeadLetterMessage(entry);
      } catch (error) {
        logger.error(
          `Replay failed for dead letter ${entry.id}: invalid JSON`,
          error
        );

        await this.handleReplayFailure(
          entry,
          error,
          results
        );

        continue;
      }

      /**
       * STEP 2:
       * READ MODEL
       *
       * Same behavior as normal Kafka processing.
       */
      if (
        ORDER_READ_MODEL_TOPICS.has(
          currentTopic
        )
      ) {
        try {
          const applied =
            await this.applyOrderReadModel(
              currentTopic,
              parsedMessage,
              {
                value: parsedMessage,
              },
              groupId
            );

          if (!applied) {
            results.skipped += 1;

            logger.info(
              `[OrderConsumer] Read-model event already applied during replay for DLQ ${entry.id}`
            );
          }

          await deadLetterRepository.markStatus(
            entry.id,
            'replayed'
          );

          results.succeeded += 1;
        } catch (error) {
          logger.error(
            `Read-model replay failed for DLQ ${entry.id}:`,
            error
          );

          await this.handleReplayFailure(
            entry,
            error,
            results
          );
        }

        continue;
      }

      /**
       * STEP 3:
       * SIDE EFFECT REPLAY
       */
      const eventId =
        this.getEventId(parsedMessage);

      const orderId =
        this.getOrderId(parsedMessage);

      if (!eventId) {
        const error = new Error(
          `Missing eventId during replay for ${currentTopic}`
        );

        await this.handleReplayFailure(
          entry,
          error,
          results
        );

        continue;
      }

      let claimed = false;

      try {
        claimed =
          await processedEventRepository.claimProcessing(
            currentTopic,
            eventId,
            orderId,
            groupId
          );

        if (!claimed) {
          /**
           * Check whether it was already completed.
           */
          let status = 'completed';

          if (
            typeof processedEventRepository.getStatus ===
            'function'
          ) {
            status =
              await processedEventRepository.getStatus(
                currentTopic,
                eventId,
                groupId
              );
          }

          if (status === 'completed') {
            await deadLetterRepository.markStatus(
              entry.id,
              'replayed'
            );

            results.succeeded += 1;
            results.skipped += 1;

            logger.info(
              `[OrderConsumer] Event ${eventId} already completed; DLQ ${entry.id} marked replayed`
            );

            continue;
          }

          /**
           * Another worker currently owns it.
           */
          logger.warn(
            `[OrderConsumer] Event ${eventId} is currently being processed by another worker`
          );

          results.skipped += 1;

          continue;
        }

        /**
         * IMPORTANT:
         * Replay BOTH direct handlers AND EventBus.
         *
         * This fixes the original EventBus-loss bug.
         */
        await this.executeHandlers(
          currentTopic,
          parsedMessage,
          {
            value: parsedMessage,
          }
        );

        await this.publishToEventBus(
          currentTopic,
          parsedMessage,
          groupId
        );

        /**
         * Resolve idempotency FIRST.
         */
        const completed =
          await processedEventRepository.markCompleted(
            currentTopic,
            eventId,
            groupId
          );

        if (completed === false) {
          throw new Error(
            `Processing claim expired or was superseded for event ${eventId}`
          );
        }

        /**
         * Only NOW mark DLQ replayed.
         */
        await deadLetterRepository.markStatus(
          entry.id,
          'replayed'
        );

        results.succeeded += 1;
      } catch (error) {
        logger.error(
          `Replay failed for dead letter ${entry.id} (${currentTopic}):`,
          error
        );

        /**
         * Mark claim failed so a later attempt can reclaim it.
         */
        if (claimed) {
          try {
            await processedEventRepository.markFailed(
              currentTopic,
              eventId,
              groupId
            );
          } catch (statusError) {
            logger.error(
              `Failed to mark replay claim failed for ${eventId}:`,
              statusError
            );
          }
        }

        await this.handleReplayFailure(
          entry,
          error,
          results
        );
      }
    }

    logger.info(
      `♻️ Dead letter replay complete`,
      results
    );

    return results;
  }

  async handleReplayFailure(
    entry,
    error,
    results
  ) {
    const retryCount =
      entry.retry_count ?? 0;

    try {
      if (retryCount >= MAX_REPLAY_ATTEMPTS) {
        await deadLetterRepository.markStatus(
          entry.id,
          'failed'
        );

        logger.error(
          `Dead letter ${entry.id} marked failed after ${retryCount} retries`
        );
      } else {
        await deadLetterRepository.markStatus(
          entry.id,
          'pending',
          {
            incrementRetry: true,
          }
        );
      }
    } catch (statusError) {
      logger.error(
        `Failed to update DLQ status for ${entry.id}:`,
        statusError
      );
    }

    results.failed += 1;
  }

  async startAllConsumers() {
    await this.initialize();

    /**
     * Only start groups actually created above.
     */
    for (const groupId of this.createdConsumerGroups) {
      try {
        await this.startConsuming(groupId);

        logger.info(
          `✅ Consumer ${groupId} started`
        );
      } catch (error) {
        logger.error(
          `❌ Failed to start consumer ${groupId}:`,
          error
        );
      }
    }
  }
}

export default new OrderConsumer();

const TTL = 24 * 60 * 60;

export async function markProcessed(
  redis,
  key
) {
  if (!redis) {
    throw new Error(
      'Redis instance is required'
    );
  }

  if (!key) {
    throw new Error(
      'Deduplication key is required'
    );
  }

  const result = await redis.set(
    `dedup:${key}`,
    '1',
    'EX',
    TTL,
    'NX'
  );

  return result === 'OK';
}