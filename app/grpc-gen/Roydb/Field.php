<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: roydb.proto

namespace Roydb;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>roydb.Field</code>
 */
class Field extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string key = 1;</code>
     */
    private $key = '';
    /**
     * Generated from protobuf field <code>string valueType = 2;</code>
     */
    private $valueType = '';
    /**
     * Generated from protobuf field <code>int64 intValue = 3;</code>
     */
    private $intValue = 0;
    /**
     * Generated from protobuf field <code>double doubleValue = 4;</code>
     */
    private $doubleValue = 0.0;
    /**
     * Generated from protobuf field <code>string strValue = 5;</code>
     */
    private $strValue = '';
    /**
     * Generated from protobuf field <code>string charset = 6;</code>
     */
    private $charset = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $key
     *     @type string $valueType
     *     @type int|string $intValue
     *     @type float $doubleValue
     *     @type string $strValue
     *     @type string $charset
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Roydb::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string key = 1;</code>
     * @return string
     */
    public function getKey()
    {
        return $this->key;
    }

    /**
     * Generated from protobuf field <code>string key = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setKey($var)
    {
        GPBUtil::checkString($var, True);
        $this->key = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string valueType = 2;</code>
     * @return string
     */
    public function getValueType()
    {
        return $this->valueType;
    }

    /**
     * Generated from protobuf field <code>string valueType = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setValueType($var)
    {
        GPBUtil::checkString($var, True);
        $this->valueType = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>int64 intValue = 3;</code>
     * @return int|string
     */
    public function getIntValue()
    {
        return $this->intValue;
    }

    /**
     * Generated from protobuf field <code>int64 intValue = 3;</code>
     * @param int|string $var
     * @return $this
     */
    public function setIntValue($var)
    {
        GPBUtil::checkInt64($var);
        $this->intValue = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>double doubleValue = 4;</code>
     * @return float
     */
    public function getDoubleValue()
    {
        return $this->doubleValue;
    }

    /**
     * Generated from protobuf field <code>double doubleValue = 4;</code>
     * @param float $var
     * @return $this
     */
    public function setDoubleValue($var)
    {
        GPBUtil::checkDouble($var);
        $this->doubleValue = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string strValue = 5;</code>
     * @return string
     */
    public function getStrValue()
    {
        return $this->strValue;
    }

    /**
     * Generated from protobuf field <code>string strValue = 5;</code>
     * @param string $var
     * @return $this
     */
    public function setStrValue($var)
    {
        GPBUtil::checkString($var, True);
        $this->strValue = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string charset = 6;</code>
     * @return string
     */
    public function getCharset()
    {
        return $this->charset;
    }

    /**
     * Generated from protobuf field <code>string charset = 6;</code>
     * @param string $var
     * @return $this
     */
    public function setCharset($var)
    {
        GPBUtil::checkString($var, True);
        $this->charset = $var;

        return $this;
    }

}

