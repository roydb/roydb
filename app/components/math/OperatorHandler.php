<?php

namespace App\components\math;

class OperatorHandler
{
    protected $operatorHandlers = [];

    public function __construct()
    {
        $this->operatorHandlers['='] = function ($operand1, $operand2) {
            return $operand1 === $operand2;
        };
        $this->operatorHandlers['<'] = function ($operand1, $operand2) {
            return $operand1 < $operand2;
        };
        $this->operatorHandlers['<='] = function ($operand1, $operand2) {
            return $operand1 <= $operand2;
        };
        $this->operatorHandlers['>'] = function ($operand1, $operand2) {
            return $operand1 > $operand2;
        };
        $this->operatorHandlers['>='] = function ($operand1, $operand2) {
            return $operand1 >= $operand2;
        };
        $this->operatorHandlers['between'] = function ($operand1, $operand2, $operand3) {
            return ($operand1 >= $operand2) && ($operand1 <= $operand3);
        };
    }

    public function calculateOperatorExpr($operator, ...$args)
    {
        return $this->operatorHandlers[$operator](...$args);
    }
}
