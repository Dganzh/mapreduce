
Master
属性
- 输入文件
- reduce worker数量
- 整个任务的状态

提供的接口
- 登记接口(要返回workerID)
- 分配任务的接口



Worker
属性
workerID (master分配)
task    当前的任务

- 向master注册
- 向master申请任务


Task
属性
taskType    任务类型: map/reduce/done
args        任务参数: fileName/reduceNo/""      

