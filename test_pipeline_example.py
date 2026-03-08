from job_hive.queue import RedisQueue
from job_hive import HiveWork, Job, Pipeline


def add_one(x):
    """第一个任务：将输入加 1"""
    return x + 1


def multiply_by_two(x):
    """第二个任务：将输入乘以 2"""
    return x * 2


def add_three(x):
    """第三个任务：将输入加 3"""
    return x + 3


if __name__ == '__main__':
    # 创建 HiveWork 实例
    # 注意：这里的 Redis 连接配置需要根据实际情况修改
    # 你可以使用本地 Redis，或者修改为你自己的 Redis 服务器地址
    work = HiveWork(queue=RedisQueue(name="test_pipeline", host='localhost', port=6379, db=0))
    
    # 创建三个延迟任务
    @work.delay_task()
    def task_add_one(x):
        return add_one(x)
    
    @work.delay_task()
    def task_multiply_by_two(x):
        return multiply_by_two(x)
    
    @work.delay_task()
    def task_add_three(x):
        return add_three(x)
    
    # 创建 Pipeline 并添加任务
    # 初始值为 5，执行顺序：5 → 5+1=6 → 6*2=12 → 12+3=15
    # 最终返回值应为 15
    pipeline = Pipeline(
        task_add_one(5),
        task_multiply_by_two(),  # 注意：这里不需要传参，会自动获取上一个任务的结果
        task_add_three()         # 同上
    )
    
    print("Pipeline 创建成功！")
    print(f"Pipeline 包含 {len(pipeline)} 个任务")
    print(f"第一个任务 ID: {pipeline.first_job.job_id}")
    print(f"最后一个任务 ID: {pipeline.last_job.job_id}")
    
    # 提交 pipeline
    work.pipeline_commit(pipeline)
    print("\nPipeline 已提交到队列！")
    print("现在可以启动 worker 来执行任务...")
    
    # 如果要启动 worker，取消下面这行的注释
    # work.work(result_ttl=30)
