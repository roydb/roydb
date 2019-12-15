<?php

namespace App\services;

use App\components\optimizers\CostBasedOptimizer;
use App\components\optimizers\RulesBasedOptimizer;
use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\leveldb\LevelDB;
use SwFwLess\components\http\Response;
use SwFwLess\facades\Log;
use SwFwLess\facades\RedisPool;
use SwFwLess\services\BaseService;

class QueryService extends BaseService
{
    protected function openBtree($name, $new = false)
    {
        $btreePath = \SwFwLess\facades\File::storagePath() . '/btree/' . $name;
        if ((!$new) && (!is_dir($btreePath))) {
            return false;
        }

        /* default open options */
        $options = array(
            'create_if_missing' => true,	// if the specified database didn't exist will create a new one
            'error_if_exists'	=> false,	// if the opened database exsits will throw exception
            'paranoid_checks'	=> false,
            'block_cache_size'	=> 8 * (2 << 20),
            'write_buffer_size' => 4<<20,
            'block_size'		=> 4096,
            'max_open_files'	=> 1000,
            'block_restart_interval' => 16,
            'compression'		=> LEVELDB_SNAPPY_COMPRESSION,
            'comparator'		=> NULL,   // any callable parameter which returns 0, -1, 1
        );
        /* default readoptions */
        $readoptions = array(
            'verify_check_sum'	=> false,
            'fill_cache'		=> true,
            'snapshot'			=> null
        );

        /* default write options */
        $writeoptions = array(
            'sync' => false
        );

        return new \LevelDB($btreePath, $options, $readoptions, $writeoptions);
    }

    public function select()
    {
//        if (!is_dir(\SwFwLess\facades\File::storagePath() . '/btree')) {
//            mkdir(\SwFwLess\facades\File::storagePath() . '/btree', 0777, true);
//        }
//        $primaryIdx = $this->openBtree('test2', true);
//        $fieldIdx = $this->openBtree('test2.name', true);
//        $fieldIdx->set('foo', json_encode([['id' => 1]]));
//        $primaryIdx->set(1, json_encode(['id' => 1, 'type' => 1, 'name' => 'foo']));
//        $fieldIdx->set('goo', json_encode([['id' => 2]]));
//        $primaryIdx->set(2, json_encode(['id' => 2, 'type' => 1, 'name' => 'goo']));
//        $fieldIdx->set('zoo', json_encode([['id' => 3]]));
//        $primaryIdx->set(3, json_encode(['id' => 3, 'type' => 1, 'name' => 'zoo']));
//        $fieldIdx->set('doo', json_encode([['id' => 5]]));
//        $primaryIdx->set(5, json_encode(['id' => 5, 'type' => 1, 'name' => 'doo']));
//        $fieldIdx->set('boo', json_encode([['id' => 4]]));
//        $primaryIdx->set(4, json_encode(['id' => 4, 'type' => 1, 'name' => 'boo']));
//        var_dump($fieldIdx->get('foo'));
//        var_dump($fieldIdx->get('haha'));
//        $primaryIdx = $this->openBtree('test1', true);
//        $fieldIdx = $this->openBtree('test1.name', true);
//        $fieldIdx->set('foo', json_encode([['id' => 1]]));
//        $primaryIdx->set(1, json_encode(['id' => 1, 'type' => 1, 'name' => 'foo']));
//        $fieldIdx->set('goo', json_encode([['id' => 2]]));
//        $primaryIdx->set(2, json_encode(['id' => 2, 'type' => 1, 'name' => 'goo']));
//        $fieldIdx->set('zoo', json_encode([['id' => 3]]));
//        $primaryIdx->set(3, json_encode(['id' => 3, 'type' => 1, 'name' => 'zoo']));
//        $fieldIdx->set('doo', json_encode([['id' => 5]]));
//        $primaryIdx->set(5, json_encode(['id' => 5, 'type' => 1, 'name' => 'doo']));
//        $fieldIdx->set('boo', json_encode([['id' => 4]]));
//        $primaryIdx->set(4, json_encode(['id' => 4, 'type' => 1, 'name' => 'boo']));
//        var_dump($fieldIdx->get('foo'));
//        var_dump($fieldIdx->get('haha'));
//        return [
//            'code' => 0,
//            'msg' => 'ok',
//            'data' => [
//                'result_set' => [],
//            ],
//        ];

//        if (!is_dir(\SwFwLess\facades\File::storagePath() . '/btree')) {
//            mkdir(\SwFwLess\facades\File::storagePath() . '/btree', 0777, true);
//        }
//
//        $schemaData = $this->openBtree('meta.schema', true);
//        $schemaData->set('test', json_encode([
//            'pk' => 'id',
//            'columns' => [
//                [
//                    'name' => 'id',
//                    'type' => 'int',
//                    'length' => 11,
//                    'default' => null,
//                    'allow_null' => false,
//                ],
//                [
//                    'name' => 'type',
//                    'type' => 'int',
//                    'length' => 11,
//                    'default' => 0,
//                    'allow_null' => false,
//                ],
//                [
//                    'name' => 'name',
//                    'type' => 'varchar',
//                    'length' => 255,
//                    'default' => '',
//                    'allow_null' => false,
//                ],
//            ],
//            'index' => [
//                [
//                    'name' => 'name',
//                    'columns' => ['name'],
//                    'unique' => false,
//                ],
//            ],
//        ]));
//        $schemaData->set('test2', json_encode([
//            'pk' => 'id',
//            'columns' => [
//                [
//                    'name' => 'id',
//                    'type' => 'int',
//                    'length' => 11,
//                    'default' => null,
//                    'allow_null' => false,
//                ],
//                [
//                    'name' => 'type',
//                    'type' => 'int',
//                    'length' => 11,
//                    'default' => 0,
//                    'allow_null' => false,
//                ],
//                [
//                    'name' => 'name',
//                    'type' => 'varchar',
//                    'length' => 255,
//                    'default' => '',
//                    'allow_null' => false,
//                ],
//            ],
//            'index' => [
//                [
//                    'name' => 'name',
//                    'columns' => ['name'],
//                    'unique' => false,
//                ],
//            ],
//        ]));
//        var_dump($schemaData->get('test'));
//        var_dump($schemaData->get('test2'));
//        return [
//            'code' => 0,
//            'msg' => 'ok',
//            'data' => [
//                'result_set' => [],
//            ],
//        ];

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
//            $redis->hSet('meta.schema', 'test', json_encode([
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
//            var_dump($redis->hGet('meta.schema', 'test'));
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


        $index = RedisPool::pick('pika');
        $result = $index->rawCommand(
            'pkhscanrange',
            $index->_prefix('test1'),
            '',
            '',
            'MATCH',
            '*',
            'LIMIT',
            100
        );
        foreach ($result[1] as $key => $data) {
            if ($key % 2 != 0) {
                var_dump($data);
            }
        }
        RedisPool::release($index);
        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => $result,
        ];

        $start = microtime(true);
        $sql = $this->request->post('sql');
        $ast = Parser::fromSql($sql)->parseAst();
        $plan = Plan::create($ast, LevelDB::create());
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
