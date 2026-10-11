-- Run once against an existing MsgMate database, after checking SHOW INDEX.
-- Fresh databases created from msgcenter.sql already have these indexes.
ALTER TABLE t_msg_queue_low ADD INDEX idx_pending_order (status, create_time, id);
ALTER TABLE t_msg_queue_middle ADD INDEX idx_pending_order (status, create_time, id);
ALTER TABLE t_msg_queue_high ADD INDEX idx_pending_order (status, create_time, id);
ALTER TABLE t_msg_queue_retry ADD INDEX idx_pending_order (status, create_time, id);
