<?php

namespace App\components\storage\pika;

use App\services\KvClient;
use Roykv\GetAllRequest;
use Roykv\GetRequest;
use Roykv\MGetRequest;
use Roykv\ScanRequest;

class Roykv extends Pika
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
        return $this->getKvClient();
    }

    protected function metaSchemaGet(KvClient $btree, $schemaName)
    {
        $metaSchema = null;

        $getReply = $btree->Get((new GetRequest())->setKey('meta.schema::' . $schemaName));
        if ($getReply) {
            $metaSchema = $getReply->getValue();
        }

        return $metaSchema;
    }

    protected function dataSchemaGetAll(KvClient $btree, $indexName)
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

    protected function dataSchemaGetById(KvClient $btree, $id, $schema)
    {
        $data = null;
        if (is_int($id)) {
            $id = (string)$id;
        }

        $getReply = $btree->Get((new GetRequest())->setKey('data.schema.' . $schema . '::' . $id));
        if ($getReply) {
            $data = $getReply->getValue();
        }

        return $data;
    }

    protected function dataSchemaScan(KvClient $btree, $indexName, $startKey, $endKey, $limit, $skipFirst = false)
    {
        $data = [];

        $scanReply = $btree->Scan(
            (new ScanRequest())->setStartKey('data.schema.' . $indexName . '::' . $startKey)
                ->setEndKey(($endKey === '') ? '' : ('data.schema' . $indexName . '::' . $endKey))
                ->setKeyPrefix('data.schema.' . $indexName . '::')
                ->setLimit($limit)
        );
        if ($scanReply) {
            foreach ($scanReply->getData() as $i => $item) {
                if ($skipFirst && ($i === 0)) {
                    continue;
                }
                $data[$item->getKey()] = $item->getValue();
            }
        }

        return $data;
    }

    protected function dataSchemaMGet(KvClient $btree, $schema, $idList)
    {
        $values = [];

        array_walk($idList, function (&$val) {
            if (!is_int($val)) {
                $val = (string) $val;
            }
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

    //todo
}
