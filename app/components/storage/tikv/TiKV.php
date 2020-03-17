<?php

namespace App\components\storage\tikv;

use App\components\storage\roykv\Roykv;
use App\services\roykv\KvClient;

class TiKV extends Roykv
{
    /**
     * @return KvClient
     */
    protected function getKvClient()
    {
        return new KvClient();
    }
}
