<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: roydb.proto

namespace Roydb;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>roydb.InsertRequest</code>
 */
class InsertRequest extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string sql = 1;</code>
     */
    private $sql = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $sql
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Roydb::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string sql = 1;</code>
     * @return string
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * Generated from protobuf field <code>string sql = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setSql($var)
    {
        GPBUtil::checkString($var, True);
        $this->sql = $var;

        return $this;
    }

}

