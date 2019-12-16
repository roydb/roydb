<?php

namespace App\components\optimizers;

use App\components\elements\condition\Condition;
use App\components\plans\Plan;
use App\components\plans\QueryPlan;

class RulesBasedOptimizer
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
        if (!($this->plan->getExecutePlan() instanceof QueryPlan)) {
            return $this->plan;
        }

        $this->setStorageGetLimit();
        $this->setNativeOrderBy();

        return $this->plan;
    }

    protected function setStorageGetLimit()
    {
        /** @var QueryPlan $queryPlan */
        $queryPlan = $this->plan->getExecutePlan();
        $limit = $queryPlan->getLimit();
        if (is_null($limit)) {
            return;
        }
        if (!is_null($queryPlan->getOrders())) {
            return;
        }
        if (!is_null($queryPlan->getGroups())) {
            return;
        }
        if (count($queryPlan->getSchemas()) > 0) {
            return;
        }
        $condition = $queryPlan->getCondition();
        if (is_null($condition)) {
            $queryPlan->setStorageGetLimit($limit);
        } else {
            if ($condition instanceof Condition) {
                $queryPlan->setStorageGetLimit($limit);
            }
        }
    }

    protected function setNativeOrderBy()
    {
        /** @var QueryPlan $queryPlan */
        $queryPlan = $this->plan->getExecutePlan();
        $orders = $queryPlan->getOrders();
        if (is_null($orders)) {
            return;
        }
        if (count($orders) > 1) {
            return;
        }
        $condition = $queryPlan->getCondition();
        $order = $orders[0];
        if ($order->getType() === 'colref') {
            if ($order->getValue() === 'id' && $order->getDirection() === 'ASC') { //todo fetch primary key from schema
                if (is_null($condition)) {
                    $queryPlan->setNativeOrder(true);
                }
            }
        }
    }
}
