<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Roykvtikv;

/**
 */
class TiKVClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts = []) {
        parent::__construct($hostname, $opts);
    }

    /**
     * @param \Roykvtikv\SetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\SetReply[]|\Roykvtikv\SetReply|\Grpc\StringifyAble[]
     */
    public function Set(\Roykvtikv\SetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/Set',
        $argument,
        ['\Roykvtikv\SetReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\GetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\GetReply[]|\Roykvtikv\GetReply|\Grpc\StringifyAble[]
     */
    public function Get(\Roykvtikv\GetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/Get',
        $argument,
        ['\Roykvtikv\GetReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\ExistRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\ExistReply[]|\Roykvtikv\ExistReply|\Grpc\StringifyAble[]
     */
    public function Exist(\Roykvtikv\ExistRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/Exist',
        $argument,
        ['\Roykvtikv\ExistReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\ScanRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\ScanReply[]|\Roykvtikv\ScanReply|\Grpc\StringifyAble[]
     */
    public function Scan(\Roykvtikv\ScanRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/Scan',
        $argument,
        ['\Roykvtikv\ScanReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\MGetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\MGetReply[]|\Roykvtikv\MGetReply|\Grpc\StringifyAble[]
     */
    public function MGet(\Roykvtikv\MGetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/MGet',
        $argument,
        ['\Roykvtikv\MGetReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\GetAllRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\GetAllReply[]|\Roykvtikv\GetAllReply|\Grpc\StringifyAble[]
     */
    public function GetAll(\Roykvtikv\GetAllRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/GetAll',
        $argument,
        ['\Roykvtikv\GetAllReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\CountRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\CountReply[]|\Roykvtikv\CountReply|\Grpc\StringifyAble[]
     */
    public function Count(\Roykvtikv\CountRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/Count',
        $argument,
        ['\Roykvtikv\CountReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykvtikv\DelRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykvtikv\DelReply[]|\Roykvtikv\DelReply|\Grpc\StringifyAble[]
     */
    public function Del(\Roykvtikv\DelRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykvtikv.TiKV/Del',
        $argument,
        ['\Roykvtikv\DelReply', 'decode'],
        $metadata, $options);
    }

}
