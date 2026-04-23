# 新服务职责边界（T8-001）

> 最后更新：2026-04-23

## 1. 边界原则

服务层（Service）是只读编排层，不直接承担采集、标准化、发布实现。

## 2. 服务层职责

- 接收查询请求并做参数校验
- 从 Served 读取数据
- 提供聚合、分页、排序、过滤
- 输出稳定 API 契约与错误语义

## 3. 非职责

- 不直接调用外部源接口（AkShare/Lixinger/Tushare）
- 不执行 Raw 写入
- 不执行 Standardized 归一化
- 不执行 Served 发布

## 4. 模块边界

- `ingestion/*`：抓取、路由、执行
- `raw/*`：原始落盘与清单
- `standardized/*`：字段映射、归一、合并
- `served/*`：版本发布、读取、回滚
- `api.py` / `service/*`：查询接口

## 5. 依赖方向

允许：`service -> served -> standardized -> raw`

禁止：

- `service -> ingestion`
- `service -> sources`
- `served -> ingestion`
