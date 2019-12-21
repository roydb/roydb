<?php

namespace App\services;

use App\components\optimizers\CostBasedOptimizer;
use App\components\optimizers\RulesBasedOptimizer;
use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\pika\Pika;
use SwFwLess\components\http\Response;
use SwFwLess\facades\Log;
use SwFwLess\facades\RedisPool;
use SwFwLess\services\BaseService;

class QueryService extends BaseService
{
    public function select()
    {
//        $redis = RedisPool::pick('pika');
//        try {
//            $redis->hSet('test2.name', 'foo', json_encode([['id' => 1]]));
//            $redis->hSet('test2', 1, json_encode(['id' => 1, 'type' => 1, 'name' => 'foo']));
//            $redis->hSet('test2.name', 'goo', json_encode([['id' => 2]]));
//            $redis->hSet('test2', 2, json_encode(['id' => 2, 'type' => 1, 'name' => 'goo']));
//            $redis->hSet('test2.name', 'zoo', json_encode([['id' => 3]]));
//            $redis->hSet('test2', 3, json_encode(['id' => 3, 'type' => 1, 'name' => 'zoo']));
//            $redis->hSet('test2.name', 'doo', json_encode([['id' => 5]]));
//            $redis->hSet('test2', 5, json_encode(['id' => 5, 'type' => 1, 'name' => 'doo']));
//            $redis->hSet('test2.name', 'boo', json_encode([['id' => 4]]));
//            $redis->hSet('test2', 4, json_encode(['id' => 4, 'type' => 1, 'name' => 'boo']));
//            var_dump($redis->hGet('test2.name', 'foo'));
//            var_dump($redis->hGet('test2.name', 'haha'));
//            $redis->hSet('test1.name', 'foo', json_encode([['id' => 1]]));
//            $redis->hSet('test1', 1, json_encode(['id' => 1, 'type' => 1, 'name' => 'foo']));
//            $redis->hSet('test1.name', 'goo', json_encode([['id' => 2]]));
//            $redis->hSet('test1', 2, json_encode(['id' => 2, 'type' => 1, 'name' => 'goo']));
//            $redis->hSet('test1.name', 'zoo', json_encode([['id' => 3]]));
//            $redis->hSet('test1', 3, json_encode(['id' => 3, 'type' => 1, 'name' => 'zoo']));
//            $redis->hSet('test1.name', 'doo', json_encode([['id' => 5]]));
//            $redis->hSet('test1', 5, json_encode(['id' => 5, 'type' => 1, 'name' => 'doo']));
//            $redis->hSet('test1.name', 'boo', json_encode([['id' => 4]]));
//            $redis->hSet('test1', 4, json_encode(['id' => 4, 'type' => 1, 'name' => 'boo']));
//            var_dump($redis->hGet('test1.name', 'foo'));
//            var_dump($redis->hGet('test1.name', 'haha'));
//
//            $redis->hSet('meta.schema', 'test1', json_encode([
//                'pk' => 'id',
//                'columns' => [
//                    [
//                        'name' => 'id',
//                        'type' => 'int',
//                        'length' => 11,
//                        'default' => null,
//                        'allow_null' => false,
//                    ],
//                    [
//                        'name' => 'type',
//                        'type' => 'int',
//                        'length' => 11,
//                        'default' => 0,
//                        'allow_null' => false,
//                    ],
//                    [
//                        'name' => 'name',
//                        'type' => 'varchar',
//                        'length' => 255,
//                        'default' => '',
//                        'allow_null' => false,
//                    ],
//                ],
//                'index' => [
//                    [
//                        'name' => 'name',
//                        'columns' => ['name'],
//                        'unique' => false,
//                    ],
//                ],
//            ]));
//            $redis->hSet('meta.schema', 'test2', json_encode([
//                'pk' => 'id',
//                'columns' => [
//                    [
//                        'name' => 'id',
//                        'type' => 'int',
//                        'length' => 11,
//                        'default' => null,
//                        'allow_null' => false,
//                    ],
//                    [
//                        'name' => 'type',
//                        'type' => 'int',
//                        'length' => 11,
//                        'default' => 0,
//                        'allow_null' => false,
//                    ],
//                    [
//                        'name' => 'name',
//                        'type' => 'varchar',
//                        'length' => 255,
//                        'default' => '',
//                        'allow_null' => false,
//                    ],
//                ],
//                'index' => [
//                    [
//                        'name' => 'name',
//                        'columns' => ['name'],
//                        'unique' => false,
//                    ],
//                ],
//            ]));
//            var_dump($redis->hGet('meta.schema', 'test1'));
//            var_dump($redis->hGet('meta.schema', 'test2'));
//
//            return [
//                'code' => 0,
//                'msg' => 'ok',
//                'data' => [
//                    'result_set' => [],
//                ],
//            ];
//        } catch (\Throwable $e) {
//            throw $e;
//        } finally {
//            RedisPool::release($redis);
//        }
//        return;


//        $index = RedisPool::pick('pika');
//        $result = $index->rawCommand(
//            'pkhscanrange',
//            $index->_prefix('test1'),
//            '',
//            '',
//            'MATCH',
//            '*',
//            'LIMIT',
//            100
//        );
//        foreach ($result[1] as $key => $data) {
//            if ($key % 2 != 0) {
//                var_dump($data);
//            }
//        }
//        RedisPool::release($index);
//        return [
//            'code' => 0,
//            'msg' => 'ok',
//            'data' => $result,
//        ];

        $start = microtime(true);
        $sql = $this->request->post('sql');
        $ast = Parser::fromSql($sql)->parseAst();
        $plan = Plan::create($ast, new Pika());
        $plan = RulesBasedOptimizer::fromPlan($plan)->optimize();
        $plan = CostBasedOptimizer::fromPlan($plan)->optimize();
        $resultSet = $plan->execute();

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'result_set' => $resultSet,
                'time_usage' => microtime(true) - $start,
            ],
        ];
    }
}
