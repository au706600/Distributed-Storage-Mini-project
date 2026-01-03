
CREATE TABLE `file`
(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `filename` TEXT,
    `size` INTEGER,
    `content_type` TEXT,
    `created` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `k_fragments` INTEGER, -- k (number of data fragments)
    `node_losses` INTEGER, -- l (number of tolerable node losses)
    `c_fragments` INTEGER -- c (total fragments = k + l)
);

CREATE TABLE `storage_node`
(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT, 
    `status` BOOLEAN DEFAULT 1
);

CREATE TABLE `file_fragment`
(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `file_id` INTEGER,
    `storage_node_id` INTEGER,
    `fragment_name` TEXT,
    `fragment_index` INTEGER,
    `coefficients` BLOB,
    FOREIGN KEY(file_id) REFERENCES file(id),
    FOREIGN KEY(storage_node_id) REFERENCES storage_node(id)
);