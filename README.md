# L-Store Database System

## Overview
This project implements **L-Store**, a column-store database system built in Python. It supports **record insertion, updates, queries, indexing, and transactions** while maintaining efficiency through **buffer pooling** and **page-range management**. The system is designed for both durability and performance, with features like persistence and versioning.

## Features
- **Column-Store Architecture**: Records stored in columnar format for efficient analytical queries.  
- **Page & Page-Range Management**: Base and tail pages for handling updates and merges.  
- **Transaction Support**: Commit, rollback, and versioning for consistency and recoverability.  
- **Buffer Pool Manager**: Handles in-memory page caching with LRU eviction, pin/unpin support, and dirty page tracking.  
- **Indexing**: B+Tree (via BTrees) for fast key lookups and range queries across multiple columns.  
- **Persistence**: Records serialized with **MsgPack** for durability and recovery.  

## Tech Stack
- **Language**: Python  
- **Libraries**: BTrees (OOBTree), MsgPack  
- **Concepts**: Database Systems, Transaction Management, Buffer Pooling, Indexing Structures, File I/O  

## Setup & Run
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/l-store-database.git
   cd l-store-database
