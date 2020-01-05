<?php

namespace App\components\storage\pika;

use App\services\KvClient;
use Roykv\ExistRequest;

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
        $kvClient = $this->getKvClient();

        $existReply = $kvClient->Exist((new ExistRequest())->setKey($name));
        if ($existReply) {
            if ($existReply->getExisted()) {
                return $kvClient;
            }
        }

        return false;
    }

    //todo
}
