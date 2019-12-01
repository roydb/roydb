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
        $sql = $this->request->post('sql');
        $ast = Parser::fromSql($sql);
        $plan = Plan::fromAst($ast);
        $resultSet = $plan->execute(new File());

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'result_set' => $resultSet,
            ],
        ];
    }
}
