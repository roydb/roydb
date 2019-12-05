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
    }

    public function calculateOperatorExpr($operator, ...$args)
    {
        return $this->operatorHandlers[$operator](...$args);
    }
}
