<?php

namespace App\services;

use PHPSQLParser\PHPSQLParser;
use SwFwLess\services\BaseService;

class QueryService extends BaseService
{
    public function select()
    {
        $originData = [
            1 => ['name' => 'foo', 'type' => 1]
        ];
        $idxSet = [
            'name' => [
                'foo' => ['id' => 1],
            ]
        ];

        $resultSet = [];

        $sql = $this->request->post('sql');
        $parser = new PHPSQLParser();
        $parsed = $parser->parse($sql);
        $conditions = $parsed['WHERE'];

        $idxField = $conditions[0]['base_expr'];
        $idxValue = $conditions[2]['base_expr'];
        if (array_key_exists($idxField, $idxSet)) {
            $idxData = $idxSet[$idxField];
            if (array_key_exists($idxValue, $idxData)) {
                $index = $idxData[$idxValue];
                $resultSet = $originData[$index['id']];
            }
        }

        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
                'result_set' => $resultSet,
            ],
        ];
    }
}
