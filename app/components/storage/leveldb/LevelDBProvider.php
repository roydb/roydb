<?php

namespace App\components\storage\leveldb;

use SwFwLess\components\provider\AbstractProvider;
use SwFwLess\components\provider\AppProvider;

class LevelDBProvider extends AbstractProvider implements AppProvider
{
    public static function bootApp()
    {
        parent::bootApp();

        LevelDB::create();
    }
}
