<?php

namespace App\components\optimizers;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
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
        $this->setIndexSuggestion();
        $this->setCountAll();
        $this->setUsedColumns();

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
        $schemas = $queryPlan->getSchemas();
        if (!is_null($schemas)) {
            if (count($schemas) > 1) {
                return;
            }
        }

        //todo column agg udf

        $queryPlan->setStorageGetLimit($limit);
    }

    protected function setIndexSuggestion()
    {
        /** @var QueryPlan $queryPlan */
        $queryPlan = $this->plan->getExecutePlan();
        $condition = $queryPlan->getCondition();
        if (is_null($condition)) {
            return;
        }

        $schemas = $queryPlan->getSchemas();
        $tableNames = array_column($schemas, 'table');

        foreach ($tableNames as $tableName) {
            $this->setIndexSuggestionByCondition($tableName, $condition, $queryPlan);
        }
    }

    protected function setIndexSuggestionByCondition($schema, $condition, QueryPlan $queryPlan)
    {
        if ($condition instanceof ConditionTree) {
            foreach ($condition->getSubConditions() as $subCondition) {
                $this->setIndexSuggestionByCondition($schema, $subCondition, $queryPlan);
            }
        } elseif ($condition instanceof Condition) {
            $columnNames = [];
            foreach ($condition->getOperands() as $operand) {
                if ($operand->getType() === 'colref') {
                    $operandValue = $operand->getValue();
                    if (strpos($operandValue, '.')) {
                        list(, $columnName) = explode('.', $operandValue);
                    } else {
                        $columnName = $operandValue;
                    }

                    $columnNames[] = $columnName;
                }
            }

            foreach ($columnNames as $columnName) {
                if ($columnName === 'id') { //todo fetch primary key from schema meta data
                    $queryPlan->setOneIndexSuggestion(
                        $schema,
                        $columnName,
                        [
                            'indexName' => $schema,
                            'primaryIndex' => true,
                        ]
                    );
                } else {
                    $queryPlan->setOneIndexSuggestion(
                        $schema,
                        $columnName,
                        [
                            'indexName' => $schema . '.' . $columnName,
                            'primaryIndex' => false,
                        ]
                    );
                }
            }
        }
    }

    protected function setCountAll()
    {
        /** @var QueryPlan $queryPlan */
        $queryPlan = $this->plan->getExecutePlan();
        if (!is_null($queryPlan->getGroups())) {
            return;
        }
        $schemas = $queryPlan->getSchemas();
        if (!is_null($schemas)) {
            if (count($schemas) > 1) {
                return;
            }
        } else {
            return;
        }
        if (!is_null($queryPlan->getCondition())) {
            return;
        }
        $columns = $queryPlan->getColumns();
        foreach ($columns as $column) {
            if (!$column->isUdf()) {
                if ($column->getType() !== 'const') {
                    return;
                }
            } else {
                if ($column->getValue() !== 'count') {
                    return;
                }
                $subColumns = $column->getSubColumns();
                foreach ($subColumns as $subColumn) {
                    if ($subColumn->hasSubColumns()) {
                        return;
                    }
                    if ($subColumn->getType() === 'colref') {
                        if ($subColumn->getValue() !== '*') {
                            return;
                        }
                    } else {
                        if ($subColumn->getType() !== 'const') {
                            return;
                        }
                    }
                }
            }
        }
        $queryPlan->setCountAll(true);
    }

    protected function setUsedColumns()
    {
        /** @var QueryPlan $queryPlan */
        $queryPlan = $this->plan->getExecutePlan();
//        $queryPlan->setUsedColumns(['id']);

        //todo
    }
}
