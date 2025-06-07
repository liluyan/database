
## **安装**

composer require godjarvis/database

## **更新**

composer update godjarvis/database

## **使用**

```php
<?php

use Apps\Helper\DiUtils;
use GodJarvis\Database\Query\Builder;
use GodJarvis\Database\Query\JoinClause;

//连表查询
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_list as pal')
    ->join('pigeon_advertiser_to_pitcher as patp', function (JoinClause $join) {
        $join->on('pal.media', '=', 'patp.media')
            ->on('pal.advertiser_id', '=', 'patp.advertiser_id');
    })
    ->select(['patp.id', 'pal.media', 'pal.advertiser_id', 'patp.pitcher_zq_user_id as pitcher_id', 'patp.pitcher_master_zq_user_id as operator_id'])
    ->selectRaw('concat(pal.media,"-",pal.advertiser_id) as mediaAdvConcat')
    ->where('patp.date', '=', '2024-11-04')
    ->whereIn('pal.advertiser_id', ['43525479', '43525477'])
    ->where('pal.media', '=', 2)
    ->groupBy('pal.media', 'pal.advertiser_id', 'patp.date')
    ->orderByRaw('pitcher_id desc, operator_id desc')
    ->toSql();
echo '查询原始SQL：' . $sql . PHP_EOL;
echo '查询绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '查询完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '查询结果：';
var_dump(DiUtils::getDb()->fetchAll($sql, 2, $bindings));

//聚合查询
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_list as pal')
    ->whereIn('pal.advertiser_id', ['43525479', '43525477'])
    ->count('media', 'mediaCount')
    ->toSql();
echo '聚合查询原始SQL：' . $sql . PHP_EOL;
echo '聚合查询绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '聚合查询完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '聚合查询结果：';
var_dump(DiUtils::getDb()->fetchAll($sql, 2, $bindings));

//插入
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_to_pitcher')
    ->insert([
        'media' => 1,
        'advertiser_id' => '1',
        'pitcher_zq_user_id' => 1,
        'pitcher_master_zq_user_id' => 1,
        'date' => '2025-04-25',
        'create_time' => '2025-04-25 00:00:00',
        'update_time' => '2025-04-25 00:00:00',
    ])
    ->toSql();
echo '插入原始SQL：' . $sql . PHP_EOL;
echo '插入绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '插入完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '插入中...' . PHP_EOL;
DiUtils::getDb()->execute($sql, $bindings);

//批量插入
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_to_pitcher')
    ->insert([
        [
            'media' => 2,
            'advertiser_id' => '2',
            'pitcher_zq_user_id' => 2,
            'pitcher_master_zq_user_id' => 2,
            'date' => '2025-04-25',
            'create_time' => '2025-04-25 00:00:00',
            'update_time' => '2025-04-25 00:00:00',
        ],
        [
            'media' => 3,
            'advertiser_id' => '3',
            'pitcher_zq_user_id' => 3,
            'pitcher_master_zq_user_id' => 3,
            'date' => '2025-04-25',
            'create_time' => '2025-04-25 00:00:00',
            'update_time' => '2025-04-25 00:00:00',
        ]
    ])
    ->toSql();
echo '批量插入原始SQL：' . $sql . PHP_EOL;
echo '批量插入绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '批量插入完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '批量插入中...' . PHP_EOL;
DiUtils::getDb()->execute($sql, $bindings);

//upsert
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_to_pitcher')
    ->where('advertiser_id', '=', '9808219')
    ->where('media', '=', 2)
    ->whereIn('date', ['2021-03-24', '2021-03-25'])
    ->upsert([
        ['id' => 1, 'pitcher_zq_user_id' => 444, 'pitcher_master_zq_user_id' => 555],
        ['id' => 2, 'pitcher_zq_user_id' => 666, 'pitcher_master_zq_user_id' => 777]
    ], 'id', ['pitcher_zq_user_id', 'pitcher_master_zq_user_id'])
    ->toSql();
echo 'upsert原始SQL：' . $sql . PHP_EOL;
echo 'upsert绑定参数：';
var_dump($bindings = $builder->getBindings());
echo 'upsert完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo 'upsert中...' . PHP_EOL;
DiUtils::getDb()->execute($sql, $bindings);

//更新
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_to_pitcher')
    ->where('id', '=', 95371238)
    ->update(['create_time' => '2024-11-04 00:10:10'])
    ->toSql();
$beforeUpdateSql = ($beforeUpdateBuilder = new Builder())->from('pigeon_advertiser_to_pitcher')
    ->select(['create_time'])
    ->where('id', '=', 95371238)
    ->limit(1)
    ->toSql();
echo '更新原始SQL：' . $sql . PHP_EOL;
echo '更新绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '更新完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '更新前结果：';
var_dump(DiUtils::getDb()->fetchAll($beforeUpdateSql, 2, $beforeUpdateBuilder->getBindings()));
echo '更新中...' . PHP_EOL;
DiUtils::getDb()->execute($sql, $bindings);
echo '更新后结果：';
var_dump(DiUtils::getDb()->fetchAll($beforeUpdateSql, 2, $beforeUpdateBuilder->getBindings()));

//批量更新（使用switch case形式，适合小数据量更新）
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_to_pitcher')
    ->where('advertiser_id', '=', '9808219')
    ->where('media', '=', 2)
    ->whereIn('date', ['2021-03-24', '2021-03-25'])
    ->batchUpdate([
        ['id' => 1, 'pitcher_zq_user_id' => 111,'pitcher_master_zq_user_id' => 111],
        ['id' => 2, 'pitcher_master_zq_user_id' => 222]
    ], 'id')
    ->toSql();
echo '批量更新原始SQL：' . $sql . PHP_EOL;
echo '批量更新绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '批量更新完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '批量更新中...' . PHP_EOL;
DiUtils::getDb()->execute($sql, $bindings);

//批量更新（使用join形式，适合大批量更新效率更高）
$builder = new Builder();
$sql = $builder->from('pigeon_advertiser_to_pitcher', 'patp')
    ->where('advertiser_id', '=', '9808219')
    ->where('media', '=', 2)
    ->whereIn('date', ['2021-03-24', '2021-03-25'])
    ->batchUpdate([
        ['id' => 1, 'pitcher_zq_user_id' => 7777, 'pitcher_master_zq_user_id' => 7777],
        ['id' => 2, 'pitcher_zq_user_id' => 8888, 'pitcher_master_zq_user_id' => 8888]
    ], 'id', true)
    ->toSql();
echo '批量更新原始SQL：' . $sql . PHP_EOL;
echo '批量更新绑定参数：';
var_dump($bindings = $builder->getBindings());
echo '批量更新完整SQL：' . $builder->toFullSql() . PHP_EOL;
echo '批量更新中...' . PHP_EOL;
DiUtils::getDb()->execute($sql, $bindings);
```

## **备注**

使用方式同 laravel，具体操作细则可查看 laravel 官方操作文档：[Database: Query Builder](https://laravel.com/docs/8.x/queries)

或 laravel 社区中文文档：[查询构造器](https://learnku.com/docs/laravel/8.x/queries/9401) 。