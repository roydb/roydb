<?php

namespace App\services;

use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\leveldb\LevelDB;
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

        $start = microtime(true);
        $sql = $this->request->post('sql');
        $ast = Parser::fromSql($sql);
        $plan = Plan::create($ast, LevelDB::create());
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
