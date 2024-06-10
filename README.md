LSM Tree Basic Implementation

- [x] Memtable
    - [x] Implement SkipList Data Structure
    - [x] Flush Memtable data to Disk
- [x] SSTable
    - [x] SSTable Files (*.sst 확장자)
        - [x] Encoding Key/Value
    - [x] Indexing SSTable for Efficient Access
- [ ] Write-Ahead Log
- [ ] Compaction
- [ ] Bloom Filters



```shell
           +-----+-------------+--+----+----------+--...-+-------------+
.sst File  | dataBlock  | dataBlock    | dataBlock | ...  | IndexBlock   |
           +-----+-------------+--+----+----------+--...-+-------------+
```

```shell

           +-----+-------------+--+----+----------+------+--------- ... ----+
dataBlock  | dataEntry  | dataEntry    | dataEntry  | dataEntry   |         |  
           +-----+-------------+--+----+----------+------+--------- ... ----+
           |<---------------- maxBlockSize -------------------------------->|

  maxBlockSize = 4KB
```

```shell
           +-----+-------------+---+----------+------+--  ...   --------+
indexBlock | offset  | offset  | offset  | offset   |     ...   | footer |  
           +-----+-------------+---+----------+------+--  ...   --------+
           
footer: (number of Offsets, index size)
offsets: (last key in dataBlock, offset)
```







[//]: # (---)

[//]: # (```)

[//]: # (aesse: best for sequential search)

[//]: # (magnamet : best for binary search)

[//]: # (voluptatemqui : worst for both search)

[//]: # (```)

[//]: # ()
[//]: # (```shell)

[//]: # (sequential search benchmark)

[//]: # ()
[//]: # (goarch: arm64)

[//]: # (pkg: lsm)

[//]: # (BenchmarkSSTSearch)

[//]: # (BenchmarkSSTSearch/aesse_)

[//]: # (BenchmarkSSTSearch/aesse_-8         	  212092	      5729 ns/op)

[//]: # (BenchmarkSSTSearch/magnamet_)

[//]: # (BenchmarkSSTSearch/magnamet_-8      	  179503	      6652 ns/op)

[//]: # (BenchmarkSSTSearch/voluptatemqui_)

[//]: # (BenchmarkSSTSearch/voluptatemqui_-8 	   75933	     15499 ns/op)

[//]: # (```)

[//]: # ()
[//]: # (```shell)

[//]: # (goarch: arm64)

[//]: # (pkg: lsm)

[//]: # (BenchmarkSSTSearch)

[//]: # (BenchmarkSSTSearch/aesse_)

[//]: # (BenchmarkSSTSearch/aesse_-8         	  189193	      6203 ns/op)

[//]: # (BenchmarkSSTSearch/magnamet_)

[//]: # (BenchmarkSSTSearch/magnamet_-8      	  191581	      6218 ns/op)

[//]: # (BenchmarkSSTSearch/voluptatemqui_)

[//]: # (BenchmarkSSTSearch/voluptatemqui_-8 	   97243	     12283 ns/op)

[//]: # (PASS)

[//]: # (```)

[//]