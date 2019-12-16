<?php

namespace App\components\optimizers;

use App\components\elements\condition\Condition;
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
}
