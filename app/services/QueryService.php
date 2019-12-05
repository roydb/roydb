<?php

namespace App\services;

use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\LevelDB;
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
//        $fieldIdx = $this->openBtree('test.name', true);
//        $fieldIdx->set('foo', json_encode(['id' => 1]));
//        $primaryIdx = $this->openBtree('test', true);
//        $primaryIdx->set(1, json_encode(['id' => 1, 'type' => 1, 'name' => 'foo']));
//        var_dump($fieldIdx->get('foo'));
//        var_dump($fieldIdx->get('haha'));
//        return [
//            'code' => 0,
//            'msg' => 'ok',
//            'data' => [
//                'result_set' => [],
//            ],
//        ];

        $sql = $this->request->post('sql');
        $ast = Parser::fromSql($sql);
        $plan = Plan::create($ast, new LevelDB());
        $resultSet = $plan->execute();

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'result_set' => $resultSet,
            ],
        ];
    }
}
