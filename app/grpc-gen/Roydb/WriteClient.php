<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Roydb;

/**
 */
class WriteClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts = []) {
        parent::__construct($hostname, $opts);
    }

    /**
     * @param \Roydb\InsertRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roydb\InsertResponse[]|\Roydb\InsertResponse|\Grpc\StringifyAble[]
     */
    public function Insert(\Roydb\InsertRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roydb.Write/Insert',
        $argument,
        ['\Roydb\InsertResponse', 'decode'],
        $metadata, $options);
    }

}
