<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: roykv-for-tikv.proto

namespace Roykvtikv;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>roykvtikv.MGetRequest</code>
 */
class MGetRequest extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>repeated string keys = 1;</code>
     */
    private $keys;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string[]|\Google\Protobuf\Internal\RepeatedField $keys
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\RoykvForTikv::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>repeated string keys = 1;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getKeys()
    {
        return $this->keys;
    }

    /**
     * Generated from protobuf field <code>repeated string keys = 1;</code>
     * @param string[]|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setKeys($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::STRING);
        $this->keys = $arr;

        return $this;
    }

}

