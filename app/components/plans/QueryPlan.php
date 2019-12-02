<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;

class QueryPlan
{
    const JOIN_TYPE_HANDLER_MAPPING = [
        'JOIN' => 'innerJoinResultSet',
        'LEFT' => 'leftJoinResultSet',
        'RIGHT' => 'rightJoinResultSet',
    ];

    /** @var Ast */
    protected $ast;

    protected $columns;

    protected $schemas;

    protected $condition;

    public function __construct($ast)
    {
        $this->ast = $ast;

        $this->extractColumns();
        $this->extractSchemas();
        $this->extractConditions();
    }

    protected function extractColumns()
    {
        $select = $this->ast->getStmt()['SELECT'];
        $this->columns = array_column($select, 'base_expr');
    }

    protected function extractSchemas()
    {
        $from = $this->ast->getStmt()['FROM'];
        $this->schemas = $from;
//        var_dump($this->ast->getStmt());
    }

    protected function extractConditions()
    {
        $conditionExpr = $this->ast->getStmt()['WHERE'];
        $conditionTree = new ConditionTree();
        $condition = new Condition();
        foreach ($conditionExpr as $expr) {
            if ($expr['expr_type'] === 'colref') {
                $condition->addOperands($expr['base_expr']);
            } elseif ($expr['expr_type'] === 'operator') {
                if (!in_array($expr['base_expr'], ['and', 'or', 'not'])) {
                    $condition->setOperator($expr['base_expr']);
                } else {
                    if ($expr['base_expr'] === 'not') {
                        if (is_null($conditionTree->getLogicOperator())) {
                            $conditionTree->setLogicOperator('not');
                            $conditionTree->addSubConditions($condition);
                        } else {
                            $newConditionTree = new ConditionTree();
                            $newConditionTree->setLogicOperator('not');
                            $conditionTree->addSubConditions($newConditionTree);
                            $condition = new Condition();
                            $newConditionTree->addSubConditions($condition);
                        }
                    } elseif ($expr['base_expr'] === 'and') {
                        if (is_null($conditionTree->getLogicOperator())) {
                            $conditionTree->setLogicOperator('and');
                            $conditionTree->addSubConditions($condition);
                            $condition = new Condition();
                            $conditionTree->addSubConditions($condition);
                        } else {
                            if ($conditionTree->getLogicOperator() === 'or') {
                                $newConditionTree = new ConditionTree();
                                $newConditionTree->setLogicOperator('and');
                                $newConditionTree->addSubConditions($conditionTree->popSubCondition());
                                $conditionTree->addSubConditions($newConditionTree);
                                $conditionTree = $newConditionTree;
                                $condition = new Condition();
                                $conditionTree->addSubConditions($condition);
                            } elseif ($conditionTree->getLogicOperator() === 'not') {
                                $newConditionTree = new ConditionTree();
                                $newConditionTree->setLogicOperator('and');
                                $newConditionTree->addSubConditions($conditionTree);
                                $conditionTree = $newConditionTree;
                                $condition = new Condition();
                                $conditionTree->addSubConditions($condition);
                            } elseif ($conditionTree->getLogicOperator() === 'and') {
                                $condition = new Condition();
                                $conditionTree->addSubConditions($condition);
                            }
                        }
                    } elseif ($expr['base_expr'] === 'or') {
                        if (is_null($conditionTree->getLogicOperator())) {
                            $conditionTree->setLogicOperator('or');
                            $conditionTree->addSubConditions($condition);
                            $condition = new Condition();
                            $conditionTree->addSubConditions($condition);
                        } else {
                           if ($conditionTree->getLogicOperator() === 'and') {
                               $newConditionTree = new ConditionTree();
                               $newConditionTree->addSubConditions($conditionTree);
                               $conditionTree = $newConditionTree;
                               $condition = new Condition();
                               $conditionTree->addSubConditions($condition);
                           }
                        }
                    }
                }
            } elseif ($expr['expr_type'] === 'const') {
                $condition->addOperands($expr['base_expr']);
            }
        }

        if (!is_null($conditionTree->getLogicOperator())) {
            $this->condition = $conditionTree;
        } else {
            $this->condition = $condition;
        }
    }

    public function execute($storage)
    {
        $resultSet = $storage->get(
            $this->schemas[0]['table'],
            $this->condition
        );

        $resultSet = $this->columnsFilter($resultSet, $this->columns);

        return $resultSet;
    }

    protected function joinResultSet($left, $right, $type, $conditions)
    {
        $joinHandler = self::JOIN_TYPE_HANDLER_MAPPING[$type];
        return $this->{$joinHandler}($left, $right, $conditions);
    }

    protected function innerJoinResultSet($leftResultSet, $rightResultSet, $conditions)
    {
        $joinedResultSet = [];

        foreach ($leftResultSet as $leftIndex => $leftRow) {
            foreach ($rightResultSet as $rightIndex => $rightRow) {
                if ($this->matchJoinCondition($leftRow, $rightRow, $conditions)) {
                    $joinedResultSet[] = $leftRow + $rightRow;
                }
            }
        }

        return $joinedResultSet;
    }

    protected function leftJoinResultSet($leftResultSet, $rightResultSet, $conditions)
    {
        $joinedResultSet = [];

        foreach ($leftResultSet as $leftIndex => $leftRow) {
            if (count($rightResultSet) <= 0) {
                $joinedResultSet[] = $leftRow; //todo fetch schema
            }

            foreach ($rightResultSet as $rightIndex => $rightRow) {
                if ($this->matchJoinCondition($leftRow, $rightRow, $conditions)) {
                    $joinedResultSet[] = $leftRow + $rightRow;
                } else {
                    $joinedResultSet[] = $leftRow; //todo fetch schema
                }
            }
        }

        return $joinedResultSet;
    }

    protected function rightJoinResultSet($leftResultSet, $rightResultSet, $conditions)
    {
        return $this->leftJoinResultSet($rightResultSet, $leftResultSet, $conditions);
    }

    protected function matchJoinCondition($leftRow, $rightRow, $conditions)
    {
        foreach ($conditions as $leftField => $rightField) {
            if (!array_key_exists($leftField, $leftRow)) {
                return false;
            }
            if (!array_key_exists($rightField, $rightRow)) {
                return false;
            }

            if ($leftRow[$leftField] !== $rightRow[$rightField]) {
                return false;
            }
        }

        return true;
    }

    protected function columnsFilter($resultSet, $columns = ['*'])
    {
        if (!in_array('*', $columns)) {
            foreach ($resultSet as $i => $row) {
                foreach ($row as $k => $v) {
                    if (!in_array($k, $columns)) {
                        unset($row[$k]);
                    }
                }
                $resultSet[$i] = $row;
            }
        }

        return $resultSet;
    }
}
