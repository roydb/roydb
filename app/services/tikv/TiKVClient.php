<?php

namespace App\services\tikv;

/**
 *
 * @mixin \Roykvtikv\TiKVClient
 */
class TiKVClient  extends \Grpc\ClientStub
{

    protected $grpc_client = \Roykvtikv\TiKVClient::class;

    protected $endpoint = '127.0.0.1:50055';

}
