mysql-dataflowsync(DFS)
===

[English](./README.md)

# DFS 介绍

DFS是一个数据同步框架，通过流式读取mysql binlog实现数据捕获，目标数据库可以是mysql，也可以是其他数据库。这个框架由一个毫秒级的增量数据捕获模块（CDC）、基于sqlite数据库的持久化队列 [persist-queue](https://github.com/peter-wangxu/persist-queue)、自动关系处理模块（rel_mgr）以及符合业务逻辑的、复杂的数据转换模块（DPU）构成，使用 [dfs-entrypoint.py](./dfs-entrypoint.py) 启动，可以自由组合调用。在一个日均7.5万事件负载的集群数据库中，增量数据同步能够在秒级延迟内完成（平均延迟1秒内），同时保持了数据的完整性和一致性。

# DFS 使用场景

  - 用于**数据同步** - 用于源数据库与目标数据库表结构一致情况下的数据同步。

  - 用于**数据管道** - 用于源数据库与目标数据库表结构不一致情况下的数据同步，经过DPU自定义表结构映射与数据加工，可实现数据管道功能。

  - 用于**数据备份** - 不设置目标数据库，使用内置的DFS日志数据库，实现增量数据事件备份功能。


# DFS 工作流

```mermaid
flowchart 
    subgraph "云"
        SDB[("源数据库")]
        BL[["fa:fa-file Binlog 流"]]
        SDB -.- BL
    end
    
    subgraph "fa:fa-cubes DFS 服务"
        subgraph "fa:fa-cube CDC 模块"
            BLS{{"fa:fa-code binlog_stream_reader"}}
            FM1{{"fa:fa-code cdc_field_mapper"}}
            BL ==> BLS
            BLS ==> |"事件"| FM1
        end
        
        
        Q[("fa:fa-database 持久化队列")]
        LDB[("fa:fa-database DFS 数据库")]
        
        FM1 ==> |"推送"| Q
        
        subgraph "fa:fa-cube DPUs"
            RM{{"fa:fa-code relationship_manager"}}
            FM2{{"fa:fa-code dpu_field_mapper"}}
            SG{{"fa:fa-code statement_generator"}}
            FM2 ==> |"事件"| SG
            RM ==> |"事件"| FM2
        end
        
        Q ==> |"获取"| RM
        
        subgraph "fa:fa-cube DFS 监控"
            RTM["fa:fa-window-maximize 前端UI"]
            API{{"fa:fa-code DFS_fastapi_router"}}
            
        end
        
        LDB -.-> |"DFS 信息"| API -.-> |"Websocket"| RTM
        BLS -.-> |"CDC 接收日志"| LDB
        RM <==> |"DPU 关系创建和查询"| LDB
        FM2 -.-> |"DPU 处理日志"| LDB
        SG -.-> |"DML 提交日志"| LDB
        
    end
    
    subgraph "云"
        TDB[("fa:fa-database 目标数据库")]
        SG ==> |"DML 提交"| TDB
    end 
```

在 mermaid.live 上编辑: [DFS workflow](https://mermaid.live/edit#pako:eNqVVe1L20Ac_leO-7RBW5qkrbXMwWpRhBamqQxmpFybaxvWvJCk-FILK8zNbVp1L3bghnMoEwY6kDlWP_jPNKn-F7ska2rTSFk-lOvd89zvuefJ71KDBZnHMAGLFXmpUEaqDjgJkEer5ksqUsqAg532LgedWethU8mFexw02zvmx5_m1qnRfs_B-4t9QDK9sMDBIkoUUbAoVDBIClJFLgHzV4ODi4sDO4FgKEgIzhyWeGcwJMHZrFDNYw2kplhgft4y3hzeVuULBpOpSWCeHBpfWrexjki2VnOxxAOQt1XmNF3FSMyRHx6rHKzXB3lTGcrDK_CFXFHAFT4nIkXx4yTTYGLioVVySIO9sGZ5_LZzecHBNatAH-U60ndlcDRrReGI4ZGO8kjDwNxsdP6sG5t7N58OjI3WYDhpJz0PxfbUP83-iCjryTWbJzfPG5bcWT-kfxipx_OaN4a5jMdNFVeQLsiSVhYUYqiESv4p0B4er1RHpMBOeyiajnQsYknPlbCEVaTL_pWGM2Knvafwy5EemeNsj3bd_G1s71m0ucx_GEpS6-7vms3vQ7ZmM24LLgkSLy8FRbQsiMIqBsbrre6Ps_kZ0ouDpEePZzwOkQK5IiI-KUJOlau6n60jD5l2utw-p6W4c3VoNs6ss5KC7soTnNfkwjOs2yZkM7evE9ZF2f3cPDY_XJitY-OqZYHJ_n0wSeLBP0_J6waM9fPu-aWxsW9cto13m-bB8fXZtyGSlbGr0GIdvejuvLyjAjvdx2bSwNze6bSP7sCOutY8N2vWtze7-6fm11d3dCdR0zuuK8aSke3JIMUBDEARqyISeHLR16x5Dupl8upzMEGGPC6iakW3lNQJFFV1mV2RCjChq1UcgCT4UhkSSRWN_KsqRBhOCYicQXRnMS-Q7sk4nxL7ixKACpJgogaXYYIap0JjYZqmqFg0wkToWDQAV2AiSEVCTHSMYsaYcIRhIky8HoCrsky2DYficZqJxuI0HRkPx8Zpyt7vqb1oyar_BZ9OI2I)

# 已测试环境
- 源数据库: MySQL 5.7.x（打开binlog，且`binlog-format = ROW`）
- 目标数据库: MySQL 8.0.x
- Python 版本: 3.12+
- Python 库: [requirements.txt](./requirements.txt)

# 安装

1. 按需修改 `config.example.yaml` 并重命名为 `config.yaml`
    > 如需指定binlog文件与binlog位置，请在 `config.yaml` 中设置 `binlog_file` 与 `binlog_pos`。

2. 构建并启动容器

    ```bash
    docker build -t mysql-dataflowsync:latest .
    docker compose up -d
    ```

# Performance

> Configuration: Using a 2022 M2 MacBook Pro (16GB), Python 3.12, source database is MySQL 5.7.x (PolarDB), target database is MySQL 8.0.x (PolarDB)

## ![CDC](monitor/static/cdc/favicon.ico "CDC") CDC (Change Data Capture) Unit

A change data capture unit implemented using the Python `mysql-replication` and `persist-queue` libraries.

CDC processing speed: `Maximum` 59 records per second (`MAX 59rps`) on a single node, with an average of `17ms` per record.

## ![DPU](monitor/static/dpu/favicon.ico "DPU") DPU (Data Processing Unit)

Custom Data Processing Unit for unidirectional data synchronisation between two databases.

DPU processing speed: in the case of a single node, the maximum processing `18` records per second (`MAX 18rps`), the average `55ms` processing a record.

# License
This project is licensed under the MIT License.

# References
- [python-mysql-replication](https://github.com/julien-duponchelle/python-mysql-replication) - Apache License, Version 2.0
- [persist-queue](https://github.com/peter-wangxu/persist-queue) - BSD-3-Clause license
