<?php

namespace App\services\roydb;

/**
 *
 * @mixin \Roydbudf\MathClient
 */
class MathClient  extends \Grpc\ClientStub
{

    protected $grpc_client = \Roydbudf\MathClient::class;

    protected $endpoint = '127.0.0.1:50052';

}
