-- 故障日志库初始化脚本。
-- Docker 首次启动时由 /docker-entrypoint-initdb.d 执行；本地自测也会重复执行，
-- 因此写成可重入（先 DROP 再 CREATE）。
DROP TABLE IF EXISTS legacy_queries CASCADE;
DROP TABLE IF EXISTS fault_logs CASCADE;
DROP TABLE IF EXISTS retirement_state CASCADE;

-- 故障日志表：summary 为新版列，legacy_summary 为待退役旧列。
CREATE TABLE fault_logs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at     timestamptz NOT NULL DEFAULT now(),
    severity       text NOT NULL CHECK (severity IN ('INFO','WARN','ERROR','CRITICAL')),
    service        text NOT NULL,
    message        text NOT NULL,
    summary        text NOT NULL,           -- 新版检索读取
    legacy_summary text NOT NULL            -- 旧版检索读取（待退役）
);

CREATE INDEX idx_fault_logs_severity ON fault_logs (severity);

INSERT INTO fault_logs (created_at, severity, service, message, summary, legacy_summary) VALUES
  (now() - interval '12 days',  'CRITICAL', 'auth-gateway',   'OAuth signing key rotation failed; login error rate spiked to 38%',
   'auth-gateway: OAuth 密钥轮换失败导致登录错误率飙至 38%，已回滚密钥并恢复。',
   'LEGACY|CRITICAL|auth-gateway|oauth-key-rotation-failed|logins-38pct-errors|rolled-back'),
  (now() - interval '11 days',  'ERROR',    'payment-api',    'Connection to acquirer gateway timed out 214 times in 10 minutes',
   'payment-api: 收单网关连接 10 分钟内超时 214 次，切换备用通道后恢复。',
   'LEGACY|ERROR|payment-api|acquirer-timeout|214-timeouts-10min|failover-done'),
  (now() - interval '10 days',  'WARN',     'search-indexer', 'Index lag exceeded 5 minutes on shard 7',
   'search-indexer: 分片 7 索引延迟超 5 分钟，扩容消费者后追平。',
   'LEGACY|WARN|search-indexer|index-lag|shard-7-gt-5min|scaled-consumers'),
  (now() - interval '9 days',   'CRITICAL', 'order-service',  'Primary database failover triggered by replication slot corruption',
   'order-service: 复制槽损坏触发主库切换，RTO 4 分钟，无数据丢失。',
   'LEGACY|CRITICAL|order-service|replication-slot-corrupt|failover-rto-4min|no-data-loss'),
  (now() - interval '8 days',   'INFO',     'cdn-purger',     'Cache purge backlog cleared after 40 minutes',
   'cdn-purger: 缓存刷新积压 40 分钟后清零。',
   'LEGACY|INFO|cdn-purger|purge-backlog|cleared-40min'),
  (now() - interval '7 days',   'ERROR',    'user-profile',   'Redis cluster node OOM killed; 12% of sessions evicted',
   'user-profile: Redis 节点 OOM 被杀，12% 会话被淘汰，用户重新登录即可。',
   'LEGACY|ERROR|user-profile|redis-oom|12pct-sessions-evicted|relogin-required'),
  (now() - interval '6 days',   'WARN',     'billing-worker', 'Invoice generation retries elevated (p95 = 3 attempts)',
   'billing-worker: 发票生成重试率升高（p95=3 次），下游税率服务抖动所致。',
   'LEGACY|WARN|billing-worker|invoice-retry|p95-3-attempts|tax-svc-flapping'),
  (now() - interval '5 days',   'CRITICAL', 'message-broker', 'Kafka partition leader imbalance caused 90s produce latency p99',
   'message-broker: Kafka 分区 leader 不均衡致生产延迟 p99 达 90 秒，重平衡后恢复。',
   'LEGACY|CRITICAL|message-broker|leader-imbalance|p99-90s|rebalanced'),
  (now() - interval '4 days',   'ERROR',    'inventory-sync', 'Deadlock storm between stock reservation and audit writers',
   'inventory-sync: 库存预占与审计写入死锁风暴，调整加锁顺序后消除。',
   'LEGACY|ERROR|inventory-sync|deadlock-storm|lock-order-fixed'),
  (now() - interval '3 days',   'WARN',     'api-gateway',    'TLS certificate for api.internal expires in 72 hours',
   'api-gateway: api.internal 证书 72 小时内到期，已自动续期。',
   'LEGACY|WARN|api-gateway|tls-expiry|72h-left|auto-renewed'),
  (now() - interval '2 days',   'ERROR',    'notification',   'SMS provider webhook endpoint returned 502 for 18 minutes',
   'notification: 短信服务商回调 502 持续 18 分钟，补投完成。',
   'LEGACY|ERROR|notification|sms-webhook-502|18min|replayed'),
  (now() - interval '1 day',    'INFO',     'deploy-agent',   'Canary deploy of recommender v2.14 paused at 5% due to latency budget',
   'deploy-agent: 推荐服务 v2.14 金丝雀发布因延迟预算暂停在 5%。',
   'LEGACY|INFO|deploy-agent|canary-paused|recommender-v2.14|5pct'),
  (now() - interval '12 hours', 'CRITICAL', 'object-storage', 'Erasure coding rebuild fell behind; durability margin at 2x',
   'object-storage: 纠删码重建落后，耐久冗余降至 2 倍，限流后追平。',
   'LEGACY|CRITICAL|object-storage|ec-rebuild-behind|durability-2x|throttled'),
  (now() - interval '6 hours',  'WARN',     'log-pipeline',   'Fluentd buffer near capacity on 3 nodes',
   'log-pipeline: 3 个节点 Fluentd 缓冲接近上限，已清理并扩容。',
   'LEGACY|WARN|log-pipeline|fluentd-buffer|3-nodes-near-cap|expanded');

-- 退役协调状态机（单行）：active -> retiring -> retired。
CREATE TABLE retirement_state (
    id              smallint PRIMARY KEY CHECK (id = 1),
    status          text NOT NULL CHECK (status IN ('active','retiring','retired')),
    request_id      uuid,
    requested_at    timestamptz,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    failed_attempts integer NOT NULL DEFAULT 0,
    last_error      text
);

INSERT INTO retirement_state (id, status) VALUES (1, 'active');

-- 旧版检索查询号登记表（两实例共享）：主键保证同号全局只能接纳一次。
-- 生命周期由应用维护：接纳时 INSERT，取消/完成时 UPDATE 终态，
-- 退役锁内拒单时 DELETE（未真正执行，不留号）。
CREATE TABLE legacy_queries (
    query_id     text PRIMARY KEY
                 CHECK (char_length(query_id) BETWEEN 8 AND 128),
    instance_id  text NOT NULL,              -- 接纳并执行该查询的实例
    backend_pid  integer NOT NULL,           -- 执行后端进程号（pg_cancel_backend 用）
    status       text NOT NULL DEFAULT 'accepted'
                 CHECK (status IN ('accepted', 'cancel_requested',
                                   'cancelled', 'completed', 'failed',
                                   'interrupted')),
    accepted_at  timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

-- 取消轮询/按属主实例排查时的常用索引（表本身很小，主要保证计划稳定）。
CREATE INDEX idx_legacy_queries_status ON legacy_queries (status);
