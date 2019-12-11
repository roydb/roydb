<?php

namespace App\components\storage;

abstract class AbstractStorage
{
    abstract public function get($schema, $condition, $limit);

    abstract public function getSchemaMetaData($schema);
}
