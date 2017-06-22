# MySQL 数据库连接池组件

[pymysqlpool]() 是数据库工具包中新成员，目的是能提供一个实用的数据库连接池中间件，从而避免在应用中频繁地创建和释放数据库连接资源。


# 功能

1. 连接池本身是线程安全的，可在多线程环境下使用，不必担心连接资源被多个线程共享的问题；
2. 提供尽可能紧凑的接口用于数据库操作；
3. 连接池的管理位于包内完成，客户端可以通过接口获取池中的连接资源（返回 `pymysql.Connection`）；
4. 将最大程度地与 dataobj 等兼容，便于使用；
5. 连接池本身具备动态增加连接数的功能，即 `max_pool_size` 和 `step_size` 会用于控制每次增加的连接数和最大连接数；
6. 连接池最大连接数亦动态增加，需要开启 `enable_auto_resize` 开关，此后当任何一次连接获取超时发生，均记为一次惩罚，并且将 `max_pool_size` 扩大一定倍数。

# 基本工作流程

**注意，当多线程同时请求时，若池中没有可用的连接对象，则需要排队等待**


1. 初始化后优先创建一个连接对象，放在连接池中；
1. 客户端请求连接对象，连接池会从中挑选最近没使用的连接对象返回（同时会检查连接是否正常）；
1. 客户端使用连接对象，执行相应操作后，调用接口返回连接对象；
1. 连接池回收连接对象，并将其加入池中的队列，供其它请求使用。


```
|--------|                                |--------------|
|        | <==borrow connection object==  | Pool manager |
| Client |                                |              |
|        | ==return connection object==>  |  FIFO queue  |
|--------|                                |--------------|
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
- enable_auto_resize: 是否动态扩展连接池，即当超过 `max_pool_size` 时，自动扩展 `max_pool_size`；
- pool_resize_boundary: 该配置为连接池最终可以增加的上上限大小，即时扩展也不可超过该值；
- auto_resize_scale: 自动扩展 `max_pool_size` 的增益，默认为 1.5 倍扩展；
- defer_connect_pool: 是否延迟连接到连接池，当该值为 True 时，需要显示调用 `pool.connect` 进行连接；
- kwargs: 其他配置参数将会在创建连接对象时传递给 `pymysql.Connection`。

# 使用示例

1. 使用 `cursor` 上下文管理器（快捷方式，但每次获取都会申请连接对象，多次调用效率不高）：

    ```python
    from pymysqlpool import ConnectionPool
    
    config = {
        'pool_name': 'test',
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'test'
    }
    
    def connection_pool():
        # Return a connection pool instance
        pool = ConnectionPool(**config)
        return pool

    # 直接访问并获取一个 cursor 对象，自动 commit 模式会在这种方式下启用
    with connection_pool().cursor() as cursor:
        print('Truncate table user')
        cursor.execute('TRUNCATE user')

        print('Insert one record')
        result = cursor.execute('INSERT INTO user (name, age) VALUES (%s, %s)', ('Jerry', 20))
        print(result, cursor.lastrowid)

        print('Insert multiple records')
        users = [(name, age) for name in ['Jacky', 'Mary', 'Micheal'] for age in range(10, 15)]
        result = cursor.executemany('INSERT INTO user (name, age) VALUES (%s, %s)', users)
        print(result)

        print('View items in table user')
        cursor.execute('SELECT * FROM user')
        for user in cursor:
            print(user)

        print('Update the name of one user in the table')
        cursor.execute('UPDATE user SET name="Chris", age=29 WHERE id = 16')
        cursor.execute('SELECT * FROM user ORDER BY id DESC LIMIT 1')
        print(cursor.fetchone())

        print('Delete the last record')
        cursor.execute('DELETE FROM user WHERE id = 16')
    ```

1. 使用 `connection` 上下文管理器：


    ```python
    import pandas as pd
    from pymysqlpool import ConnectionPool

    config = {
        'pool_name': 'test',
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'test'
    }


    with connection_pool().connection() as conn:
        pd.read_sql('SELECT * FROM user', conn)


    # 或者
    connection = connection_pool().borrow_connection()
    pd.read_sql('SELECT * FROM user', conn)
    connection_pool().return_connection(connection)
    ```

# 依赖
1. `pymysql`：将依赖该工具包完成数据库的连接等操作；
1. `pandas`：测试时使用了 pandas。

# 安装

下载源码后，使用 `pip` 安装即可：`pip3 setup.py install`，注意需要使用 Python3 环境。

# 日志

## 2017.06.22 周四
1. 更新使用文档和部分问题修复。

## 2017.06.19 周一
1. 重构连接池动态扩展部分。

## 2017.06.18 周日
1. 移除多余的`cursor`模块，充分利用 `pymysql.cursor`；
1. 重构部分模块，同时添加新的测试。

## 2017.06.17 周六
1. 更新连接池工厂函数，替换不正确的命名方式；
1. 添加新的测试和示例。

## 2017.06.16 周五
1. 完成一个池管理器，使用 FIFO 队列模式管理池中的资源；
1. 提供第一个可供测试的版本，并完成基本的测试。

## 2017.06.15 周四
1. 初步完成连接池的编写。