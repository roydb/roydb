<?php

namespace App\services\roykv;

/**
 *
 * @mixin \Roykv\TxnClient
 */
class TxnClient  extends \Grpc\ClientStub
{

    protected $grpc_client = \Roykv\TxnClient::class;

    protected $endpoint = '127.0.0.1:9999';

}
