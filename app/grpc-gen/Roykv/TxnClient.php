<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Roykv;

/**
 */
class TxnClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts = []) {
        parent::__construct($hostname, $opts);
    }

    /**
     * @param \Roykv\BeginRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\BeginReply[]|\Roykv\BeginReply|\Grpc\StringifyAble[]
     */
    public function Begin(\Roykv\BeginRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Txn/Begin',
        $argument,
        ['\Roykv\BeginReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\CommitRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\CommitReply[]|\Roykv\CommitReply|\Grpc\StringifyAble[]
     */
    public function Commit(\Roykv\CommitRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Txn/Commit',
        $argument,
        ['\Roykv\CommitReply', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Roykv\RollbackRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Roykv\RollbackReply[]|\Roykv\RollbackReply|\Grpc\StringifyAble[]
     */
    public function Rollback(\Roykv\RollbackRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/roykv.Txn/Rollback',
        $argument,
        ['\Roykv\RollbackReply', 'decode'],
        $metadata, $options);
    }

}
