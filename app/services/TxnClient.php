<?php

namespace App\services;

/**
 *
 * @mixin \Roykv\TxnClient
 */
class TxnClient  extends \Grpc\ClientStub
{

    use \SwFwLess\components\traits\Singleton;

    protected $grpc_client = \Roykv\TxnClient::class;

    protected $endpoint = '127.0.0.1:9999';

}
