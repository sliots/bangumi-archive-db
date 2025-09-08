# bangumi-archive-db

**把 [bangumi/Archive] 的部分指标转为可查询的 SQL 数据表，并按快照日期（`data_date`）发布 `.sql` 备份。可便于后续分析与查询。**  

---

## 表结构
当前发布的 `.sql` 中包含以下三张表（PostgreSQL）：

```sql
create table character_stats
(
    id        integer not null,
    comments  integer default 0,
    collects  integer default 0,
    data_date date    not null,
    primary key (id, data_date)
);

create table person_stats
(
    id        integer not null,
    comments  integer default 0,
    collects  integer default 0,
    data_date date    not null,
    primary key (id, data_date)
);

create table subject_stats
(
    id            integer not null,
    score         numeric(3, 1),
    score_details jsonb,
    rank          integer,
    favorite      jsonb,
    data_date     date    not null,
    primary key (id, data_date)
);
```
## 数据范围

- dump-2025-09-02.210328Z.zip
- dump-2025-08-26.210318Z.zip
- dump-2025-08-19.210329Z.zip
- dump-2025-08-12.210319Z.zip
- dump-2025-08-05.210326Z.zip
- dump-2025-07-29.210307Z.zip
- dump-2025-07-22.210307Z.zip
- dump-2025-07-15.210307Z.zip
- dump-2025-07-08.210304Z.zip
- dump-2025-07-01.210306Z.zip
- dump-2025-06-24.210345Z.zip
- dump-2025-06-17.210250Z.zip
- dump-2025-06-10.210258Z.zip
- dump-2025-06-03.210251Z.zip
- dump-2025-05-27.210250Z.zip
- dump-2025-05-20.210251Z.zip
- dump-2025-05-13.210253Z.zip
- dump-2025-05-06.210255Z.zip
- dump-2025-04-29.210251Z.zip
- dump-2025-04-22.210246Z.zip
- dump-2025-04-15.210248Z.zip
- dump-2025-04-08.210252Z.zip
- dump-2025-04-01.210252Z.zip
- dump-2025-03-25.210303Z.zip
- dump-2025-03-18.210318Z.zip
- dump-2025-03-11.210253Z.zip
- dump-2025-03-04.210238Z.zip
- dump-2025-02-25.210247Z.zip
- dump-2025-02-18.210244Z.zip
- dump-2025-02-17.104950Z.zip
- dump-2025-01-28.210228Z.zip
- dump-2025-01-21.210228Z.zip
- dump-2025-01-14.210236Z.zip
- dump-2025-01-07.210239Z.zip
- dump-2024-12-31.210228Z.zip
- dump-2024-12-24.210232Z.zip
- dump-2024-12-17.210236Z.zip
- dump-2024-12-10.210237Z.zip
- dump-2024-12-03.210250Z.zip
- dump-2024-11-26.210256Z.zip
- dump-2024-11-19.210256Z.zip
- dump-2024-11-12.210236Z.zip
- dump-2024-11-05.210237Z.zip
- dump-2024-10-29.210231Z.zip
- dump-2024-10-22.210235Z.zip
- dump-2024-10-15.210236Z.zip
- dump-2024-10-08.210224Z.zip
- dump-2024-10-04.184615Z.zip
