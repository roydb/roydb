<?php

namespace App\components\optimizers;

use App\components\elements\condition\ConditionTree;
use App\components\plans\Plan;
use App\components\plans\QueryPlan;

class CostBasedOptimizer
{
    /** @var Plan */
    protected $plan;

    public static function fromPlan(Plan $plan)
    {
        return new static($plan);
    }

    public function __construct(Plan $plan)
    {
        $this->plan = $plan;
    }

    public function optimize()
    {
        if (!($this->plan instanceof QueryPlan)) {
            return $this->plan;
        }

        //todo

        $this->setIndexSuggestion();

        return $this->plan;
    }

    protected function setIndexSuggestion()
    {
        /** @var QueryPlan $queryPlan */
        $queryPlan = $this->plan->getExecutePlan();
        $condition = $queryPlan->getCondition();
        if (is_null($condition)) {
            return;
        }

        $this->setIndexSuggestionByCondition($condition);
    }

    protected function setIndexSuggestionByCondition($condition)
    {
        if ($condition instanceof ConditionTree) {
            foreach ($condition->getSubConditions() as $subCondition) {
                $this->setIndexSuggestionByCondition($subCondition);
            }
        } else {
            //todo
        }
    }
}
