<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Roykv;

/**
 */
class KvClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts = []) {
        parent::__construct($hostname, $opts);
    }

    /**
     * @param \Roykv\SetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\SetReply[]|\Roykv\SetReply|\Grpc\StringifyAble[]
     */
    public function Set(\Roykv\SetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Kv/Set',
        $argument,
        ['\Roykv\SetReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\GetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\GetReply[]|\Roykv\GetReply|\Grpc\StringifyAble[]
     */
    public function Get(\Roykv\GetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Kv/Get',
        $argument,
        ['\Roykv\GetReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\ExistRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\ExistReply[]|\Roykv\ExistReply|\Grpc\StringifyAble[]
     */
    public function Exist(\Roykv\ExistRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Kv/Exist',
        $argument,
        ['\Roykv\ExistReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\ScanRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\ScanReply[]|\Roykv\ScanReply|\Grpc\StringifyAble[]
     */
    public function Scan(\Roykv\ScanRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Kv/Scan',
        $argument,
        ['\Roykv\ScanReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\MGetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\MGetReply[]|\Roykv\MGetReply|\Grpc\StringifyAble[]
     */
    public function MGet(\Roykv\MGetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Kv/MGet',
        $argument,
        ['\Roykv\MGetReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\GetAllRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\GetAllReply[]|\Roykv\GetAllReply|\Grpc\StringifyAble[]
     */
    public function GetAll(\Roykv\GetAllRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Kv/GetAll',
        $argument,
        ['\Roykv\GetAllReply', 'decode'],
        $metadata, $options);
    }

}
