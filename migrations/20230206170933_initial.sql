CREATE TABLE IF NOT EXISTS state (
       step_id TEXT,
       state_key TEXT,
       epoch INTEGER,
       snapshot BLOB,
       PRIMARY KEY (step_id, state_key, epoch)
);

CREATE TABLE IF NOT EXISTS progress (
       execution INTEGER,
       worker_index INTEGER,
       frontier INTEGER,
       PRIMARY KEY (execution, worker_index)
);

CREATE TABLE IF NOT EXISTS execution (
       execution INTEGER,
       worker_index INTEGER,
       worker_count INTEGER,
       resume_epoch INTEGER,
       PRIMARY KEY (execution, worker_index)
);
