<?php

namespace App\components\elements\condition;

class Condition
{
    protected $operator;

    protected $operands = [];

    /**
     * @param $operator
     * @return $this
     */
    public function setOperator($operator)
    {
        $this->operator = $operator;
        return $this;
    }

    /**
     * @param $operands
     * @return $this
     */
    public function setOperands($operands)
    {
        $this->operands = $operands;
        return $this;
    }

    /**
     * @param $operand
     * @return $this
     */
    public function addOperands($operand)
    {
        $this->operands[] = $operand;
        return $this;
    }
}
