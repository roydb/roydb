<?php

namespace App\components\math;

class OperatorHandler
{
    protected $isNot = false;

    protected $operatorHandlers = [];

    public function __construct($isNot = false)
    {
        $this->isNot = $isNot;

        $this->operatorHandlers['='] = function ($operand1, $operand2) {
            if ($this->isNot) {
                return $operand1 !== $operand2;
            } else {
                return $operand1 === $operand2;
            }
        };
        $this->operatorHandlers['<'] = function ($operand1, $operand2) {
            if ($this->isNot) {
                return $operand1 >= $operand2;
            } else {
                return $operand1 < $operand2;
            }
        };
        $this->operatorHandlers['<='] = function ($operand1, $operand2) {
            if ($this->isNot) {
                return $operand1 > $operand2;
            } else {
                return $operand1 <= $operand2;
            }
        };
        $this->operatorHandlers['>'] = function ($operand1, $operand2) {
            if ($this->isNot) {
                return $operand1 <= $operand2;
            } else {
                return $operand1 > $operand2;
            }
        };
        $this->operatorHandlers['>='] = function ($operand1, $operand2) {
            if ($this->isNot) {
                return $operand1 < $operand2;
            } else {
                return $operand1 >= $operand2;
            }
        };
        $this->operatorHandlers['between'] = function ($operand1, $operand2, $operand3) {
            if ($this->isNot) {
                return ($operand1 < $operand2) || ($operand1 > $operand3);
            } else {
                return ($operand1 >= $operand2) && ($operand1 <= $operand3);
            }
        };
    }

    public function calculateOperatorExpr($operator, ...$args)
    {
        return $this->operatorHandlers[$operator](...$args);
    }
}
