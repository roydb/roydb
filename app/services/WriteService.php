<?php

namespace App\services;

use App\services\roydb\WriteClient;
use Roydb\InsertRequest;
use SwFwLess\services\BaseService;

class WriteService extends BaseService
{
    public function insert()
    {
        $start = microtime(true);
        $sql = $this->request->post('sql');
        $insertResponse = (new WriteClient())->Insert(
            (new InsertRequest())->setSql($sql)
        );

        if (!$insertResponse) {
            return [
                'code' => -1,
                'msg' => 'failed',
                'data' => [],
            ];
        }

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'result_set' => $insertResponse->getAffectedRows(),
                'time_usage' => microtime(true) - $start,
            ],
        ];
    }
}
