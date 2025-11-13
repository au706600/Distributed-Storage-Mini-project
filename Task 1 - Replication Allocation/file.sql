
-- Store representation of stored files
-- Each file can be split into two parts for replication
create table `file` (
    `id` INTEGER PRIMARY KEY AUTOINCREMENT, 
    `filename` TEXT, 
    `size` INTEGER, 
    `created` DATETIME DEFAULT CURRENT_TIMESTAMP, 
    `content_type` TEXT, 
    `part1_filename` TEXT, 
    `part2_filename` TEXT
);