# Bleve index metrics

## Range of 10K blocks starting at Block 27,700,000

### 100x shards of 100 blocks

#### From Cloud Storage
Processing time: 3m15s (25x realtime speed)
#### From local store
Processing time: 3m15s (25x realtime speed) -- no advantage to not stream

```
2.0G    /tmp/indices-test/100x100/
-----
 20M    /tmp/indices-test/100x100/0027700000.bleve
 19M    /tmp/indices-test/100x100/0027700100.bleve
 21M    /tmp/indices-test/100x100/0027700200.bleve
 21M    /tmp/indices-test/100x100/0027700300.bleve
 20M    /tmp/indices-test/100x100/0027700400.bleve
 20M    /tmp/indices-test/100x100/0027700500.bleve
....... (many more)
```

### 10x shards of 1K blocks

#### From Cloud Storage
Processing time: 3m30s (24x realtime speed)

```
2.1G    /tmp/indices-test/10x1K/
-----
217M    /tmp/indices-test/10x1K/0027700000.bleve
228M    /tmp/indices-test/10x1K/0027701000.bleve
169M    /tmp/indices-test/10x1K/0027702000.bleve
230M    /tmp/indices-test/10x1K/0027703000.bleve
239M    /tmp/indices-test/10x1K/0027704000.bleve
289M    /tmp/indices-test/10x1K/0027705000.bleve
218M    /tmp/indices-test/10x1K/0027706000.bleve
195M    /tmp/indices-test/10x1K/0027707000.bleve
179M    /tmp/indices-test/10x1K/0027708000.bleve
160M    /tmp/indices-test/10x1K/0027709000.bleve
```

### 5x shared of 2K blocks

#### From Cloud Storage
Processing time: 4m5s (20x realtime speed)

```
2.6G    /tmp/indices-test/5x2K/
-----
608M    /tmp/indices-test/5x2K/0027700000.bleve
450M    /tmp/indices-test/5x2K/0027702000.bleve
630M    /tmp/indices-test/5x2K/0027704000.bleve
564M    /tmp/indices-test/5x2K/0027706000.bleve
450M    /tmp/indices-test/5x2K/0027708000.bleve
```

### 4x shares of 2.5K blocks

#### From Cloud Storage
Processing time: 5m3s (16x realtime speed)

```
3.2G    /tmp/indices-test/4x2.5K/
-----
806M    /tmp/indices-test/4x2.5K/0027700000.bleve
706M    /tmp/indices-test/4x2.5K/0027702500.bleve
1.1G    /tmp/indices-test/4x2.5K/0027705000.bleve
640M    /tmp/indices-test/4x2.5K/0027707500.bleve
```

### 2x shards of 5K blocks

#### From Cloud Storage
Processing time: 9m21s (9x realtime speed)

```
7.6G    /tmp/indices-test/2x5K/
-----
4.0G    /tmp/indices-test/2x5K/0027700000.bleve
3.6G    /tmp/indices-test/2x5K/0027705000.bleve
```

### 1x shard of 10K blocks

#### From Cloud Storage
Processing time: 15m50s (5.5x realtime speed)

```
 16G    /tmp/indices-test/1x10K/
----
 16G    /tmp/indices-test/1x10K/0027700000.bleve
```
