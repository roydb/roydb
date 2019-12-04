<?php

namespace App\services;

use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\File;
use SwFwLess\services\BaseService;

class QueryService extends BaseService
{
    public function select()
    {
//        if (!is_dir(\SwFwLess\facades\File::storagePath() . '/btree')) {
//            mkdir(\SwFwLess\facades\File::storagePath() . '/btree', 0777, true);
//        }
//        $fieldIdx = \btree::open(
//            \SwFwLess\facades\File::storagePath() . '/btree/test.name'
//        );
//        $fieldIdx->set('foo', json_encode(['id' => 1]));
//        $primaryIdx = \btree::open(
//            \SwFwLess\facades\File::storagePath() . '/btree/test'
//        );
//        $primaryIdx->set(1, json_encode(['id' => 1, 'type' => 1, 'name' => 'foo']));
//        return [
//            'code' => 0,
//            'msg' => 'ok',
//            'data' => [
//                'result_set' => [],
//            ],
//        ];

        $sql = $this->request->post('sql');
        $ast = Parser::fromSql($sql);
        $plan = Plan::create($ast, new File());
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
