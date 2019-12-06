<?php

namespace App\components\elements\condition;

class ConditionTree
{
    /** @var Condition[]|ConditionTree[] */
    protected $subConditions = [];

    protected $logicOperator;

    /**
     * @param $subConditions
     * @return $this
     */
    public function setSubConditions($subConditions)
    {
        $this->subConditions = $subConditions;
        return $this;
    }

    /**
     * @param $subCondition
     * @return $this
     */
    public function addSubConditions($subCondition)
    {
        $this->subConditions[] = $subCondition;
        return $this;
    }

    /**
     * @return mixed
     */
    public function popSubCondition()
    {
        return array_pop($this->subConditions);
    }

    /**
     * @param $logicOperator
     * @return $this
     */
    public function setLogicOperator($logicOperator)
    {
        $this->logicOperator = $logicOperator;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getLogicOperator()
    {
        return $this->logicOperator;
    }

    /**
     * @return Condition[]|ConditionTree[]
     */
    public function getSubConditions() : array
    {
        return $this->subConditions;
    }
}
