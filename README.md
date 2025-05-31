# Job Hive

[![PyPI version](https://badge.fury.io/py/job-hive.svg)](https://badge.fury.io/py/job-hive)

基于Redis实现的轻量级分布式任务队列系统

## 🚀 功能特性

- 支持任务推送、执行生命周期管理
- 提供Redis队列实现（支持密码认证）
- 上下文管理器简化资源管理
- 支持任务批处理（示例中含单任务推送）

## 📦 安装依赖

> 目前仅支持Python3.10+，于 0.1.3 版本在原来 Redis 的基础上，加入简易版本的 Kafka 支持

使用 redis

```bash
pip install job_hive[redis]
```

使用 kafka

```bash
pip install job_hive[kafka]
```

## 🛠️ 使用示例

```python
from job_hive import HiveWork
from job_hive.queue import RedisQueue

with HiveWork(queue=RedisQueue(
        name="test",
        host="your_redis_host",
        password="your_password"
)) as work:
    # 使用work 对象进行任务推送提交到任务池
    jobs = [work.push(print, f"hello {i}") for i in range(5)]
    for job_id in jobs:
        print(f"Job ID: {job_id}")
    # 启动工作模式接收任务
    work.work(result_ttl=86400)  # result_ttl 参数设置结果保留时间，默认为24小时
```

## ⚙️ 配置说明

```python
from job_hive.queue import RedisQueue, KafkaQueue

RedisQueue(
    name="队列名称",  # 必填
    host="localhost",  # 默认localhost
    port=6379,  # 默认端口
    password=None,  # 密码（可选）
    db=0  # 数据库编号，默认为0
)

KafkaQueue(
    topic_name='test', # 主题
    servers='1Panel-kafka-3wvJ:9092' # Kafka 服务
)
```

## 🤝 贡献指南

1. Fork本仓库
2. 创建特性分支（git checkout -b feature/AmazingFeature）
3. 提交修改（git commit -m 'Add some AmazingFeature'）
4. 推送分支（git push origin feature/AmazingFeature）
5. 发起Pull Request

## 📄 许可证

MIT License