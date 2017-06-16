# MySQL 数据库连接池组件

`pymysqlpool` 是数据库工具包中新成员，目的是能提供一个实用的数据库连接池中间件，从而避免在应用中频繁地创建和释放数据库连接资源，导致在执行 SQL 时效率降低。

# 功能

0. 连接池本身是线程安全的，可在多线程环境下使用，不必担心连接资源被多个线程共享的问题；
1. 提供尽可能紧凑的接口用于数据库操作；
2. 连接池的管理位于包内完成，客户端只需要提交 SQL 请求，并等待结果即可；
3. 将最大程度地与 `dataobj` 等兼容，便于使用；
4. 连接池本身具备动态增加连接数的功能，即 `max_pool_size` 和 `step_size` 会用于控制每次增加的连接数和最大连接数；
5. 连接池最大连接数亦动态增加，需要开启 `enable_auto_resize` 开关，此后当任何一次连接获取超时发生，均记为一次惩罚，并且将 `max_pool_size` 扩大一定倍数。

# 基本工作流程

1. 初始化后优先创建 `step_size` 个连接对象，放在连接池中；
1. 客户端发送 SQL 执行请求，`PoolCursor` 会在执行 SQL 前从连接池中请求一个可用的连接；
1. 当请求连接对象成功后，开始执行 SQL；如果请求失败，则会尝试等待片刻，并根据需要扩展连接池对象；直到排队等到空闲连接对象时，才会执行后续的 SQL；
1. 执行完毕后，cursor 对象会将使用的连接对象返还给连接池；
1. 当进程结束时，连接池会自动释放所有连接对象。

```
new_connection => [connection pool] => old_connection
```

# 参数配置

- pool_name: 连接池的名称，多种连接参数对应多个不同的连接池对象，多单例模式；
- host: 数据库地址
- user: 数据库服务器用户名
- password: 用户密码
- database: 默认选择的数据库
- port: 数据库服务器的端口
- charset: 字符集，默认为 'utf8'
- use_dict_cursor: 使用字典格式或者元组返回数据；
- max_pool_size: 连接池优先最大连接数；
- step_size: 连接池动态增加连接数大小；
- enable_auto_resize: 是否动态扩展连接池，即当超过 `max_pool_size` 时，自动扩展 `max_pool_size`；
- pool_resize_boundary: 该配置为连接池最终可以增加的上上限大小，即时扩展也不可超过该值；
- auto_resize_scale: 自动扩展 `max_pool_size` 的增益，默认为 1.5 倍扩展；
- wait_timeout: 在排队等候连接对象时，最多等待多久，当超时时连接池尝试自动扩展当前连接数；
- kwargs: 其他配置参数将会在创建连接对象时传递给 `pymysql.Connection`

# 使用示例

```python
from pymysqlpool import MySQLConnectionPool

config = {
    'pool_name': 'test',
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'root',
    'database': 'test'
}

conn_pool = MySQLConnectionPool(**config)
conn_pool.connect()
with conn_pool.pool_cursor() as cursor:
    result = cursor.execute_one('INSERT INTO user (name, age) VALUES (%s, %s)', ('test', 20))
    print(result)
    
    users = [(name, age) for name in ['a', 'b', 'c'] for age in range(10, 30)]
    result = cursor.execute_many('INSERT INTO user (name, age) VALUES (%s, %s)', users)
    print(result)
    
    for user in cursor.query('SELECT * FROM user'):
        print(user)
        
# 不使用上下文管理器
cursor = conn_pool.pool_cursor()
result = cursor.execute_one('INSERT INTO user (name, age) VALUES (%s, %s)', ('test', 20))
cursor.close()
```

# 性能测试

1. 相对于旧版每次执行 SQL 都经历连接和关闭过程的方式，基于连接池的机制在多线程执行 SQL 下性能提升一倍；
1. 在单线程下，执行 10000 条 SQL 测试，新版性能提升约 3 倍。

# 依赖
1. `pymysql`：将依赖该工具包完成数据库的连接等操作。

# 日志

## 2017.06.16 周五
1. 完成一个池管理器，使用 FIFO 队列模式管理池中的资源；
1. 提供第一个可供测试的版本，并完成基本的测试。

## 2017.06.15 周四
1. 初步完成连接池的编写