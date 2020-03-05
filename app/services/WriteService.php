<?php

namespace App\services;

use App\services\roydb\WriteClient;
use Roydb\DeleteRequest;
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
                'affected_rows' => $insertResponse->getAffectedRows(),
                'time_usage' => microtime(true) - $start,
            ],
        ];
    }

    public function update()
    {
        //todo
    }

    public function delete()
    {
        $start = microtime(true);
        $sql = $this->request->post('sql');
        $deleteResponse = (new WriteClient())->Delete(
            (new DeleteRequest())->setSql($sql)
        );

        if (!$deleteResponse) {
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
                'affected_rows' => $deleteResponse->getAffectedRows(),
                'time_usage' => microtime(true) - $start,
            ],
        ];
    }
}
