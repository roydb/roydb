<?php

namespace App\components\storage;

abstract class AbstractStorage
{
    abstract public function get($schema, $condition, $limit, $indexSuggestions, $usedColumns);

    abstract public function getSchemaMetaData($schema);

    abstract public function countAll($schema);

    abstract public function set($schema, $rows);

    abstract public function del($schema, $pkList);
}
