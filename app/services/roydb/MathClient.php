<?php

namespace App\services;

/**
 *
 * @mixin \Roydbudf\MathClient
 */
class MathClient  extends \Grpc\ClientStub
{

    use \SwFwLess\components\traits\Singleton;

    protected $grpc_client = \Roydbudf\MathClient::class;

}
