<?php

namespace App\services\tikv;

/**
 *
 * @mixin \Roykv\TiKVClient
 */
class TiKVClient  extends \Grpc\ClientStub
{

    protected $grpc_client = \Roykv\TiKVClient::class;

    protected $endpoint = '127.0.0.1:50054';

}
