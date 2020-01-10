<?php

namespace App\services;

use App\components\optimizers\CostBasedOptimizer;
use App\components\optimizers\RulesBasedOptimizer;
use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\pika\Pika;
use App\components\storage\pika\Roykv;
use Roykv\BeginRequest;
use Roykv\SetRequest;
use SwFwLess\components\http\Response;
use SwFwLess\facades\Log;
use SwFwLess\facades\RedisPool;
use SwFwLess\services\BaseService;

class QueryService extends BaseService
{
    public function select()
    {
        $client = new TxnClient();
        $reply = $client->Begin(new BeginRequest());
        var_dump($reply->getTxnId());
        return [
            'code' => 0,
            'msg' => 'ok',
            'data' => [
            ],
        ];

//        $partitions = [];
//        $partitions[] = [
//            'lower' => '',
//            'upper' => 0,
//        ];
//        for ($start = 1, $end = 10000; $end <= 100000; $start = $start + 10000, $end = $end + 10000) {
//            $partitions[] = [
//                'lower' => $start,
//                'upper' => $end,
//            ];
//        }
//        $partitions[] = [
//            'lower' => 100001,
//            'upper' => '',
//        ];
//
//        $kvClient = new KvClient();
//            for ($i = 1; $i < 100000; ++$i) {
//                $targetPartitionIndex = null;
//                foreach ($partitions as $partitionIndex => $partition) {
//                    if ((($partition['lower'] === '') || ($i >= $partition['lower'])) &&
//                        (($partition['upper'] === '') || ($i <= $partition['upper']))
//                    ) {
//                        $targetPartitionIndex = $partitionIndex;
//                        break;
//                    }
//                }
//
//                $firstAlphabet = chr(ord('a') + ($i % 25));
//                $kvClient->Set(
//                    (new SetRequest())->setKey('data.schema.test2.name::' . $firstAlphabet . 'oo')
//                        ->setValue(json_encode([['id' => $i]]))
//                );
//                $kvClient->Set(
//                    (new SetRequest())->setKey('data.schema.test2::' . ((string)$i))
//                        ->setValue(json_encode(['id' => $i, 'type' => 1, 'name' => $firstAlphabet . 'oo']))
//                );
//                $kvClient->Set(
//                    (new SetRequest())->setKey(
//                        'data.schema.test2.partition.' . ((string)$targetPartitionIndex) . '::' . ((string)$i))
//                        ->setValue(json_encode(['id' => $i, 'type' => 1, 'name' => $firstAlphabet . 'oo']))
//                );
//                $kvClient->Set(
//                    (new SetRequest())->setKey(
//                        'data.schema.test1.name' . '::' . $firstAlphabet . 'oo')
//                        ->setValue(json_encode([['id' => $i]]))
//                );
//                $kvClient->Set(
//                    (new SetRequest())->setKey(
//                        'data.schema.test1' . '::' . ((string)$i))
//                        ->setValue(json_encode(['id' => $i, 'type' => 1, 'name' => $firstAlphabet . 'oo']))
//                );
//                $kvClient->Set(
//                    (new SetRequest())->setKey(
//                        'data.schema.test1.partition.' . ((string)$targetPartitionIndex) . '::' . ((string)$i))
//                        ->setValue(json_encode(['id' => $i, 'type' => 1, 'name' => $firstAlphabet . 'oo']))
//                );
//            }
//
//            $kvClient->Set(
//                (new SetRequest())->setKey(
//                    'meta.schema::test1'
//                )->setValue(
//                    json_encode([
//                        'pk' => 'id',
//                        'columns' => [
//                            [
//                                'name' => 'id',
//                                'type' => 'int',
//                                'length' => 11,
//                                'default' => null,
//                                'allow_null' => false,
//                            ],
//                            [
//                                'name' => 'type',
//                                'type' => 'int',
//                                'length' => 11,
//                                'default' => 0,
//                                'allow_null' => false,
//                            ],
//                            [
//                                'name' => 'name',
//                                'type' => 'varchar',
//                                'length' => 255,
//                                'default' => '',
//                                'allow_null' => false,
//                            ],
//                        ],
//                        'index' => [
//                            [
//                                'name' => 'name',
//                                'columns' => ['name'],
//                                'unique' => false,
//                            ],
//                        ],
//                        'partition' => [
//                            'key' => 'id',
//                            'range' => $partitions,
//                        ],
//                    ])
//                )
//            );
//
//            $kvClient->Set(
//                (new SetRequest())->setKey(
//                    'meta.schema::test2'
//                )->setValue(
//                    json_encode([
//                        'pk' => 'id',
//                        'columns' => [
//                            [
//                                'name' => 'id',
//                                'type' => 'int',
//                                'length' => 11,
//                                'default' => null,
//                                'allow_null' => false,
//                            ],
//                            [
//                                'name' => 'type',
//                                'type' => 'int',
//                                'length' => 11,
//                                'default' => 0,
//                                'allow_null' => false,
//                            ],
//                            [
//                                'name' => 'name',
//                                'type' => 'varchar',
//                                'length' => 255,
//                                'default' => '',
//                                'allow_null' => false,
//                            ],
//                        ],
//                        'index' => [
//                            [
//                                'name' => 'name',
//                                'columns' => ['name'],
//                                'unique' => false,
//                            ],
//                        ],
//                        'partition' => [
//                            'key' => 'id',
//                            'range' => $partitions,
//                        ],
//                    ])
//                )
//            );
//
//            return [
//                'code' => 0,
//                'msg' => 'ok',
//                'data' => [
//                    'result_set' => [],
//                ],
//            ];


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
        $plan = Plan::create($ast, new Roykv());
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
