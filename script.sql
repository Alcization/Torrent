CREATE TABLE `peers` (
  `peers_ip` varchar(20) NOT NULL,
  `peers_port` varchar(10) NOT NULL,
  `peers_hostname` varchar(255) NOT NULL,
  `file_name` varchar(255) NOT NULL,
  `file_size` varchar(255) NOT NULL,
  `piece_hash` varchar(255) NOT NULL,
  `piece_size` varchar(10) NOT NULL,
  `num_order_in_file` varchar(10) NOT NULL
);