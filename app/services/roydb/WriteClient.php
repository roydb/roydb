<?php

namespace App\services\roydb;

/**
 *
 * @mixin \Roydb\WriteClient
 */
class WriteClient  extends \Grpc\ClientStub
{

    protected $grpc_client = \Roydb\WriteClient::class;

    protected $endpoint = '127.0.0.1:50051';

}
