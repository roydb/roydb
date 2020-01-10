<?php

namespace App\components\storage\pika;

use App\components\storage\KvStorage;
use SwFwLess\components\redis\RedisWrapper;
use SwFwLess\facades\RedisPool;

class Pika extends KvStorage
{
    protected $indexExistsCache = [];

    /**
     * @param $index
     * @param $callback
     * @return mixed
     * @throws \Throwable
     */
    protected function safeUseIndex($index, $callback) {
        try {
            return call_user_func_array($callback, [$index]);
        } catch (\Throwable $e) {
            throw $e;
        } finally {
            RedisPool::release($index);
        }
    }

    /**
     * @param RedisWrapper $btree
     * @param $schemaName
     * @return mixed
     * @throws \Throwable
     */
    protected function metaSchemaGet($btree, $schemaName)
    {
        return $this->safeUseIndex($btree, function (RedisWrapper $metaSchema) use ($schemaName) {
            return $metaSchema->hGet('meta.schema', $schemaName) ?: null;
        });
    }

    /**
     * @param RedisWrapper $btree
     * @param $indexName
     * @return mixed
     * @throws \Throwable
     */
    protected function dataSchemaGetAll($btree, $indexName)
    {
        return $this->safeUseIndex($btree, function (RedisWrapper $index) use ($indexName) {
            return $index->hVals($indexName);
        });
    }

    /**
     * @param RedisWrapper $btree
     * @param $id
     * @param $schema
     * @return mixed
     * @throws \Throwable
     */
    protected function dataSchemaGetById($btree, $id, $schema)
    {
        return $this->safeUseIndex($btree, function (RedisWrapper $index) use ($id, $schema) {
            return $index->hGet($schema, $id) ?: null;
        });
    }

    /**
     * @param RedisWrapper $btree
     * @param $indexName
     * @param $startKey
     * @param $endKey
     * @param $limit
     * @param callable $callback
     * @param bool $skipFirst
     * @throws \Throwable
     */
    protected function dataSchemaScan($btree, $indexName, &$startKey, &$endKey, $limit, $callback, &$skipFirst = false)
    {
        $this->safeUseIndex($btree, function (RedisWrapper $index) use (
            $indexName, &$startKey, &$endKey, $limit, &$skipFirst, $callback
        ) {
            while (($result = $index->rawCommand(
                'pkhscanrange',
                $index->_prefix($indexName),
                $startKey,
                $endKey,
                'MATCH',
                '*',
                'LIMIT',
                $limit
            )) && isset($result[1])) {
                $formattedResult = [];
                $resultCount = 0;
                foreach ($result[1] as $key => $data) {
                    if ($key % 2 == 0) {
                        ++$resultCount;
                    }

                    if ($skipFirst) {
                        if (in_array($key, [0, 1])) {
                            continue;
                        }
                    }

                    if ($key % 2 == 0) {
                        $formattedResult[$data] = $result[1][$key + 1];
                    }
                }

                if (call_user_func_array($callback, [$formattedResult, $resultCount]) === false) {
                    break;
                }
            }
        });
    }

    /**
     * @param RedisWrapper $btree
     * @param $schema
     * @param $idList
     * @return mixed
     * @throws \Throwable
     */
    protected function dataSchemaMGet($btree, $schema, $idList)
    {
        return $this->safeUseIndex($btree, function (RedisWrapper $index) use ($idList, $schema) {
            return $index->hMGet($schema, $idList);
        });
    }

    /**
     * @param $name
     * @param bool $new
     * @return bool|RedisWrapper
     * @throws \Throwable
     */
    protected function openBtree($name, $new = false)
    {
        $redis = RedisPool::pick('pika');
        try {
            if (!$new) {
                //todo optimization exists result cache
                if (!$redis->exists($name)) {
                    return false;
                }
            }

            return $redis;
        } catch (\Throwable $e) {
            RedisPool::release($redis);
            throw $e;
        }
    }
}
