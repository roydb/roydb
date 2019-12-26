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
        $redis = RedisPool::pick('pika');
        try {
            for ($i = 0; $i < 1000000; ++$i) {
                $firstAlphabet = chr(ord('a') + ($i % 25));
                $redis->hSet('test2.name', $firstAlphabet . 'oo', json_encode([['id' => $i]]));
                $redis->hSet('test2', $i, json_encode(['id' => $i, 'type' => 1, 'name' => $firstAlphabet . 'oo']));
                $redis->hSet('test1.name', $firstAlphabet . 'oo', json_encode([['id' => $i]]));
                $redis->hSet('test1', $i, json_encode(['id' => $i, 'type' => 1, 'name' => $firstAlphabet . 'oo']));
            }

            $redis->hSet('meta.schema', 'test1', json_encode([
                'pk' => 'id',
                'columns' => [
                    [
                        'name' => 'id',
                        'type' => 'int',
                        'length' => 11,
                        'default' => null,
                        'allow_null' => false,
                    ],
                    [
                        'name' => 'type',
                        'type' => 'int',
                        'length' => 11,
                        'default' => 0,
                        'allow_null' => false,
                    ],
                    [
                        'name' => 'name',
                        'type' => 'varchar',
                        'length' => 255,
                        'default' => '',
                        'allow_null' => false,
                    ],
                ],
                'index' => [
                    [
                        'name' => 'name',
                        'columns' => ['name'],
                        'unique' => false,
                    ],
                ],
                'partition' => [
                    'key' => 'id',
                    'range' => [
                        [
                            'lower' => '',
                            'upper' => 10000,
                        ],
                        [
                            'lower' => 10001,
                            'upper' => 100000,
                        ],
                        [
                            'lower' => 100000,
                            'upper' => '',
                        ],
                    ]
                ],
            ]));
            $redis->hSet('meta.schema', 'test2', json_encode([
                'pk' => 'id',
                'columns' => [
                    [
                        'name' => 'id',
                        'type' => 'int',
                        'length' => 11,
                        'default' => null,
                        'allow_null' => false,
                    ],
                    [
                        'name' => 'type',
                        'type' => 'int',
                        'length' => 11,
                        'default' => 0,
                        'allow_null' => false,
                    ],
                    [
                        'name' => 'name',
                        'type' => 'varchar',
                        'length' => 255,
                        'default' => '',
                        'allow_null' => false,
                    ],
                ],
                'index' => [
                    [
                        'name' => 'name',
                        'columns' => ['name'],
                        'unique' => false,
                    ],
                ],
                'partition' => [
                    'key' => 'id',
                    'range' => [
                        [
                            'lower' => '',
                            'upper' => 10000,
                        ],
                        [
                            'lower' => 10001,
                            'upper' => 100000,
                        ],
                        [
                            'lower' => 100000,
                            'upper' => '',
                        ],
                    ]
                ],
            ]));
            var_dump($redis->hGet('meta.schema', 'test1'));
            var_dump($redis->hGet('meta.schema', 'test2'));

            return [
                'code' => 0,
                'msg' => 'ok',
                'data' => [
                    'result_set' => [],
                ],
            ];
        } catch (\Throwable $e) {
            throw $e;
        } finally {
            RedisPool::release($redis);
        }
        return;


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
