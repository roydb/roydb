<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Roydbudf;

/**
 */
class MathClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts = []) {
        parent::__construct($hostname, $opts);
    }

    /**
     * @param \Roydbudf\SinRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roydbudf\SinResponse[]|\Roydbudf\SinResponse|\Grpc\StringifyAble[]
     */
    public function Sin(\Roydbudf\SinRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roydbudf.Math/Sin',
        $argument,
        ['\Roydbudf\SinResponse', 'decode'],
        $metadata, $options);
    }

}
