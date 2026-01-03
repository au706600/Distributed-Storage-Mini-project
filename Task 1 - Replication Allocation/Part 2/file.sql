-- Store representation of stored files
create  table `file` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT, 
    `filename` TEXT, 
    `size` INTEGER, 
    `created` DATETIME DEFAULT CURRENT_TIMESTAMP, 
    `content_type` TEXT
);


-- Store chunk information and replicas
create table `chunk` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `file_id` INTEGER,
    `chunk_name` TEXT,   
    `replica_index` INTEGER,  -- r number of replicas that goes from 0 to r-1
    `chunk_index` INTEGER,   -- k number of chunks that goes from 0 to k-1
    `storage_node_id` INTEGER, -- id of the storage node where the chunk replica is stored
    FOREIGN KEY (file_id) REFERENCES file(id)   
);



-- Store storage node information
create table `storage_node` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT, 
    `status` BOOLEAN DEFAULT 1  -- 1 for active, 0 for inactive
)