<?php

namespace App\components\storage\pika;

use App\components\storage\KvStorage;
use SwFwLess\components\redis\RedisWrapper;
use SwFwLess\facades\RedisPool;

class Pika extends KvStorage
{
    /**
     * @param RedisWrapper $btree
     * @param $schemaName
     */
    protected function metaSchemaGet($btree, $schemaName)
    {
        //todo
    }

    /**
     * @param RedisWrapper $btree
     * @param $indexName
     */
    protected function dataSchemaGetAll($btree, $indexName)
    {
        //todo
    }

    /**
     * @param RedisWrapper $btree
     * @param $id
     * @param $schema
     */
    protected function dataSchemaGetById($btree, $id, $schema)
    {
        //todo
    }

    /**
     * @param RedisWrapper $btree
     * @param $indexName
     * @param $startKey
     * @param $endKey
     * @param $limit
     * @param bool $skipFirst
     */
    protected function dataSchemaScan($btree, $indexName, $startKey, $endKey, $limit, $skipFirst = false)
    {
        //todo
    }

    /**
     * @param RedisWrapper $btree
     * @param $schema
     * @param $idList
     */
    protected function dataSchemaMGet($btree, $schema, $idList)
    {
        //todo
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
