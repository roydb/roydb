<?php

namespace App\services\roykv;

/**
 *
 * @mixin \Roykv\KvClient
 */
class KvClient  extends \Grpc\ClientStub
{

    use \SwFwLess\components\traits\Singleton;

    protected $grpc_client = \Roykv\KvClient::class;

    protected $endpoint = '127.0.0.1:9999';

}
