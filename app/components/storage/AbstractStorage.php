<?php

namespace App\components\storage;

abstract class AbstractStorage
{
    abstract public function get($schema, $condition);

    abstract public function getSchemaMetaData($schema);
}
