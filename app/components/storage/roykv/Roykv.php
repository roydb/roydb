<?php

namespace App\components\storage\pika;

use App\components\storage\KvStorage;
use App\services\roykv\KvClient;
use Roykv\CountRequest;
use Roykv\GetAllRequest;
use Roykv\GetRequest;
use Roykv\MGetRequest;
use Roykv\ScanRequest;
use Roykv\SetRequest;

class Roykv extends KvStorage
{
    /**
     * @return KvClient
     */
    protected function getKvClient()
    {
        return new KvClient();
    }

    /**
     * @param $name
     * @param bool $new
     * @return KvClient|bool
     */
    protected function openBtree($name, $new = false)
    {
        //todo 判断索引是否存在
        return $this->getKvClient();
    }

    /**
     * @param KvClient $btree
     * @param $schemaName
     * @return null|string
     */
    protected function metaSchemaGet($btree, $schemaName)
    {
        $metaSchema = null;

        $getReply = $btree->Get((new GetRequest())->setKey('meta.schema::' . $schemaName));
        if ($getReply) {
            $metaSchema = $getReply->getValue() ?: null;
        }

        return $metaSchema;
    }

    /**
     * @param KvClient $btree
     * @param $indexName
     * @return array
     */
    protected function dataSchemaGetAll($btree, $indexName)
    {
        $values = [];
        $getAllReply = $btree->GetAll((new GetAllRequest())->setKeyPrefix('data.schema.' . $indexName . '::'));
        if ($getAllReply) {
            $data = $getAllReply->getData();
            foreach ($data as $key => $value) {
                $values[] = $value;
            }
        }

        return $values;
    }

    /**
     * @param KvClient $btree
     * @param $id
     * @param $schema
     * @return null|string
     */
    protected function dataSchemaGetById($btree, $id, $schema)
    {
        $data = null;
        if (is_int($id)) {
            $id = (string)$id;
        }

        $getReply = $btree->Get((new GetRequest())->setKey('data.schema.' . $schema . '::' . $id));
        if ($getReply) {
            $data = $getReply->getValue() ?: null;
        }

        return $data;
    }

    /**
     * @param KvClient $btree
     * @param $indexName
     * @param $startKey
     * @param $endKey
     * @param $limit
     * @param callable $callback
     * @param bool $skipFirst
     */
    protected function dataSchemaScan($btree, $indexName, &$startKey, &$endKey, $limit, $callback, &$skipFirst = false)
    {
        while (($scanReply = $btree->Scan(
            (new ScanRequest())->setStartKey('data.schema.' . $indexName . '::' . $startKey)
                ->setEndKey(($endKey === '') ? '' : ('data.schema' . $indexName . '::' . $endKey))
                ->setKeyPrefix('data.schema.' . $indexName . '::')
                ->setLimit($limit)
        ))) {
            $data = [];
            $resultCount = 0;
            foreach ($scanReply->getData() as $i => $item) {
                ++$resultCount;

                if ($skipFirst && ($i === 0)) {
                    continue;
                }

                $key = substr($item->getKey(), strlen('data.schema.' . $indexName . '::'));
                $data[$key] = $item->getValue();
            }

            if (call_user_func_array($callback, [$data, $resultCount]) === false) {
                break;
            }
        }
    }

    /**
     * @param KvClient $btree
     * @param $schema
     * @param $idList
     * @return array
     */
    protected function dataSchemaMGet($btree, $schema, $idList)
    {
        $values = [];

        array_walk($idList, function (&$val) use ($schema) {
            $val = 'data.schema.' . $schema . '::' . ((string)$val);
        });

        $mGetReply = $btree->MGet((new MGetRequest())->setKeys($idList));
        if ($mGetReply) {
            $data = $mGetReply->getData();
            foreach ($data as $key => $value) {
                $values[] = $value;
            }
        }

        return $values;
    }

    /**
     * @param KvClient $btree
     * @param $schema
     * @return mixed
     */
    protected function dataSchemaCountAll($btree, $schema)
    {
        $countReply = $btree->Count(
            (new CountRequest())->setStartKey('data.schema.' . $schema . '::')
                ->setEndKey('')
                ->setKeyPrefix('data.schema.' . $schema . '::')
        );

        if ($countReply) {
            return $countReply->getCount();
        }

        return 0;
    }

    /**
     * @param KvClient $btree
     * @param $indexName
     * @param $id
     * @param $value
     * @return bool
     */
    protected function dataSchemaSet($btree, $indexName, $id, $value)
    {
        $setReply = $btree->Set(
            (new SetRequest())->setKey('data.schema.' . $indexName . '::' . $id)
                ->setValue($value)
        );

        if ($setReply) {
            return $setReply->getResult();
        }

        return false;
    }
}
