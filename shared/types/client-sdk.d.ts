// Type definitions for distributed-event-stream client SDK

export interface ClientOptions {
  host?: string;
  port?: number;
  logger?: any;
  maxRetries?: number;
  initialRetryDelay?: number;
  maxRetryDelay?: number;
  backoffMultiplier?: number;
  connectTimeout?: number;
  requestTimeout?: number;
  maxInflightRequests?: number;
  offsetCommitInterval?: number;
}

export interface PublishOptions {
  compression?: 'none' | 'gzip' | 'deflate' | 'snappy';
  key?: string;
  partition?: string;
}

export interface PublishResult {
  topic: string;
  offset: number;
  timestamp: number;
  partition?: string;
}

export interface SubscribeOptions {
  offset?: number;
  groupId?: string;
}

export interface MessageEvent {
  topic: string;
  offset: number;
  timestamp: number;
  message: any;
  partition?: string;
}

export interface ClientMetrics {
  messagesPublished: number;
  messagesBytesPublished: number;
  messagesReceived: number;
  messagesBytesReceived: number;
  reconnections: number;
  errors: number;
  offsetCommits: number;
  offsetCommitErrors: number;
  subscriberCount: number;
  inflightRequests: number;
  subscriptionCount: number;
  consumerGroupCount: number;
  uptime: number;
}

export interface ConsumerGroupState {
  groupId: string;
  topic: string;
  committedOffset: number;
  lastUpdate: number;
}

export class Client extends require('events').EventEmitter {
  constructor(options?: ClientOptions);
  connect(): Promise<void>;
  publish(topic: string, message: any, options?: PublishOptions): Promise<PublishResult>;
  subscribe(topic: string, handler: (message: MessageEvent) => void, options?: SubscribeOptions): Promise<string>;
  commitOffset(groupId: string, topic: string, offset: number): Promise<void>;
  pauseConsume(topic: string): void;
  resumeConsume(topic: string): void;
  getConsumerGroupState(groupId: string): ConsumerGroupState | null;
  getMetrics(): ClientMetrics;
  disconnect(): Promise<void>;
}

export interface TcpClientOptions {
  host?: string;
  port?: number;
  logger?: any;
}

export class TcpClient extends require('events').EventEmitter {
  constructor(options?: TcpClientOptions);
  connect(): Promise<void>;
  send(packet: Buffer, options?: { expectResponse?: boolean; timeout?: number }): Promise<Buffer>;
  disconnect(): void;
}

export interface ReconnectionManagerOptions {
  minDelay?: number;
  maxDelay?: number;
  backoffMultiplier?: number;
  maxJitter?: number;
  maxRetries?: number;
  failureThreshold?: number;
  resetTimeout?: number;
  logger?: any;
}

export class ReconnectionManager {
  constructor(options?: ReconnectionManagerOptions);
  getNextDelay(): number;
  recordFailure(): { retryCount: number; delay: number; circuitOpen: boolean };
  recordSuccess(): void;
  shouldGiveUp(): boolean;
  getState(): {
    retryCount: number;
    consecutiveFailures: number;
    currentDelay: number;
    circuitOpen: boolean;
    lastFailureTime: number | null;
  };
  reset(): void;
  destroy(): void;
}

export interface MessageEncoderOptions {
  compression?: 'none' | 'gzip' | 'deflate' | 'snappy';
  key?: string;
  headers?: { [key: string]: string };
}

export class MessageEncoder {
  static encode(message: any, options?: MessageEncoderOptions): Promise<Buffer>;
  static encodeBatch(messages: any[], options?: MessageEncoderOptions): Promise<Buffer>;
}

export interface DecodedMessage {
  message: any;
  key: string | null;
  compression: string;
  decompressed: boolean;
}

export class MessageDecoder {
  static decode(frame: Buffer): Promise<DecodedMessage>;
  static decodeBatch(batch: Buffer): Promise<DecodedMessage[]>;
}

export interface BatchPublisherOptions {
  batchSize?: number;
  batchTimeoutMs?: number;
  compression?: string;
  logger?: any;
}

export interface BatchPublisherMetrics {
  batchesPublished: number;
  messagesPublished: number;
  bytesPublished: number;
  errors: number;
  pendingMessages: number;
}

export class BatchPublisher {
  constructor(client: Client, options?: BatchPublisherOptions);
  add(topic: string, message: any, options?: { key?: string; priority?: number }): Promise<void>;
  drain(): Promise<void>;
  getMetrics(): BatchPublisherMetrics;
  close(): Promise<void>;
}

export interface ConsumerGroupManagerOptions {
  groupId: string;
  enableAutoCommit?: boolean;
  autoCommitIntervalMs?: number;
  autoOffsetReset?: 'earliest' | 'latest' | 'none';
  maxPartitionBehind?: number;
  logger?: any;
}

export interface ConsumerGroupState {
  offset: number;
  lastCommitOffset: number;
  lag: number;
  committed: boolean;
  paused: boolean;
}

export interface ConsumerGroupMetrics {
  offsetsCommitted: number;
  offsetCommitErrors: number;
  messagesProcessed: number;
  rebalances: number;
  topicsCount: number;
  pendingCommits: number;
  rebalancing: boolean;
}

export class ConsumerGroupManager {
  constructor(client: Client, options?: ConsumerGroupManagerOptions);
  registerTopic(topic: string, startOffset?: number): void;
  updateOffset(topic: string, offset: number): void;
  commitOffset(topic: string, offset: number): Promise<void>;
  commitAll(): Promise<void>;
  seek(topic: string, offset: number): void;
  pause(topic: string): void;
  resume(topic: string): void;
  isPaused(topic: string): boolean;
  getOffset(topic: string): number | null;
  getLag(topic: string): number | null;
  getState(): { [topic: string]: ConsumerGroupState };
  getMetrics(): ConsumerGroupMetrics;
  close(): Promise<void>;
}

export interface BrokerConfig {
  host: string;
  port: number;
}

export interface ClientPoolOptions {
  loadBalancingStrategy?: 'round-robin' | 'least-connections' | 'random';
  clientOptions?: ClientOptions;
  logger?: any;
}

export interface HealthStatus {
  [brokerId: string]: {
    connected: boolean;
    inflightRequests: number;
    metrics: ClientMetrics;
  };
}

export interface PoolMetrics {
  routedRequests: number;
  failovers: number;
  clientErrors: number;
  clientMetrics: { [brokerId: string]: ClientMetrics };
  brokerCount: number;
}

export class ClientPool {
  constructor(brokers: BrokerConfig[], options?: ClientPoolOptions);
  connect(): Promise<void>;
  publish(topic: string, message: any, options?: PublishOptions): Promise<PublishResult>;
  subscribe(topic: string, handler: (message: MessageEvent) => void, options?: SubscribeOptions): Promise<string>;
  commitOffset(groupId: string, topic: string, offset: number): Promise<void>;
  getHealthStatus(): HealthStatus;
  getMetrics(): PoolMetrics;
  disconnect(): Promise<void>;
}

export interface RetryPolicyOptions {
  maxAttempts?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  backoffMultiplier?: number;
  jitterFactor?: number;
  retryableErrors?: string[];
  logger?: any;
}

export class RetryPolicy {
  constructor(options?: RetryPolicyOptions);
  isRetryable(error: Error): boolean;
  executeWithRetry(fn: () => Promise<any>, context?: any): Promise<any>;
}

export interface ErrorInfo {
  category: string;
  isRetryable: boolean;
}

export interface ErrorMetrics {
  totalErrors: number;
  errorsByCategory: { [category: string]: number };
  retriedErrors: number;
  retriedSuccessfully: number;
  retrySuccessRate: number;
}

export class ErrorHandler {
  constructor(options?: { logger?: any; retryPolicy?: RetryPolicy } & RetryPolicyOptions);
  categorizeError(error: Error): string;
  recordError(error: Error, context?: any): ErrorInfo;
  handleWithRetry(fn: () => Promise<any>, context?: any): Promise<any>;
  formatError(error: Error, context?: any): any;
  getMetrics(): ErrorMetrics;
  resetMetrics(): void;
}

export function createClient(options?: ClientOptions): Client;
export function createClientPool(brokers: BrokerConfig[], options?: ClientPoolOptions): ClientPool;
export function createBatchPublisher(client: Client, options?: BatchPublisherOptions): BatchPublisher;
export function createConsumerGroup(client: Client, options?: ConsumerGroupManagerOptions): ConsumerGroupManager;
