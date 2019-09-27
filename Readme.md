## 用于 scrapy 单机节点的 内存型布隆过滤器 和 用于所有节点的 redis 型布隆过滤器

- scrapy 的去重模式, 不是对 url 进行去重, 而是对 url 生成的请求对象进行去重, 这无疑会增大内存的占用
- 使用内存型布隆过滤器 在生成 请求对象之前对 url 进行去重, 如果 url 重复, 就不生成 请求对象, 方便快捷.

- 内存型布隆过滤器: 
  - 在每个节点中使用 基于内存的布隆过滤器, 对本节点的 url 进行过滤
  - 为什么进行此过滤, 对多个新闻站点进行爬取, 新闻 url 的生成是反复调用固定的 api, 通常几分钟到一个小时就要调用一次 api.
  - 很可能会存在大量重复的详情页 url, 在单机节点中使用内存型布隆过滤器进行第一步过滤, 能大大降低 redis 布隆过滤器的压力
- redis型布隆过滤器: 
  - 在 redis 中使用基于 redis 的布隆过滤器, 对所有节点的 url 进行过滤.
- 自动增加过滤器
  - 在一个过滤器中保存的种子数量达到上限时, 能够自动增加一个过滤器
  - 内存型中, 使用 bitarray, 自动增加一个内存块, 实例化 bitarray, 新的数据保存到新的 bitarray 中
  - redis 型, 自动增加一个 redis_key, 新的数据保存到新的 redis_key 中
  - 在判断时, 对所有的过滤器进行判断

## 内存型布隆过滤器 memory based bloom filter

### 综述

- 一般的思路是给定 数据量 和 误判率, 计算所需的 hash 函数数量和 使用的 bit 位的长度
- 但实际中, 1. 实例化 bitarray/bitmap 时, 必须要指定所分配的内存量,
- 2. 向 bitarray/bitmap 中添加数据或判断数据是否存在时, 必须要指定多个 hash 函数
- 所以, 这里思路是从给定的 内存大小 和 hash 函数的数量入手, 计算误判率,
- 保证给出的内存和 hash 函数数量得到的误判率小于指定的的阈值,
- 如果计算得到的 误判率大于指定的误判率的阈值, 就报错, 提醒增加内存或增加 hash 函数数量

### General Intro

- generally, for a bloom filter, capacity and error_rate is given,
- hash_func_num (seeds num) and bit size (memory used) are to be calculated
- however, 1. in order use memory based bloom filter, a bitarray/bitmap must be initialized first. thus, the memory used must be assigned first.
- 2. in order to 'add' data to bitarray or check if a certain data 'exists' in bitarray one or more hash func(s) must be given.
- so in this code, pass memory used, hash_func_num, data_size when init BloomFilter, calculate error_rate,
- if calculated error_rate is smaller than error_rate_threshold, continue.
- otherwise, raise an error to remind user to increase memory_size or increase hash_func_num

## todo list

- [ ] redis 版布隆过滤器增加 redis_lock
- [ ] 英文注释和说明
- [ ] 添加日志 logging

## 参考

- https://hur.st/bloomfilter/
- https://www.jianshu.com/p/214e96e2a781
- https://www.linuxzen.com/understanding-bloom-filter.html
- https://hackernoon.com/probabilistic-data-structures-bloom-filter-5374112a7832
- http://pages.cs.wisc.edu/~cao/papers/summary-cache/node8.html
- https://www.jasondavies.com/bloomfilter/
- https://github.com/jaybaird/python-bloomfilter
- https://github.com/Sssmeb/BloomFilter
