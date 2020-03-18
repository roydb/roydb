<?php

namespace App\components\storage\tikv;

use App\components\storage\roykv\Roykv;
use App\services\tikv\TiKVClient;

class TiKV extends Roykv
{
    /**
     * @return TiKVClient
     */
    protected function getKvClient()
    {
        return new TiKVClient();
    }
}
