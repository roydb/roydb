<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\elements\condition\Operand;
use App\components\storage\AbstractStorage;

class QueryPlan
{
    const JOIN_TYPE_HANDLER_MAPPING = [
        'JOIN' => 'innerJoinResultSet',
        'LEFT' => 'leftJoinResultSet',
        'RIGHT' => 'rightJoinResultSet',
    ];

    /** @var Ast */
    protected $ast;

    /** @var AbstractStorage */
    protected $storage;

    protected $columns;

    protected $schemas;

    protected $condition;

    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;
        $this->storage = $storage;

        $this->extractColumns();
        $this->extractSchemas();
        $this->condition = $this->extractConditions($ast->getStmt()['WHERE']);
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

    protected function extractConditions($conditionExpr)
    {
        $conditionTree = new ConditionTree();
        $condition = new Condition();
        foreach ($conditionExpr as $expr) {
            if ($expr['expr_type'] === 'colref') {
                $condition->addOperands(
                    (new Operand())->setType('colref')->setValue($expr['base_expr'])
                );
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
                $constExpr = $expr['base_expr'];
                if (strpos($constExpr, '"') === 0) {
                    $constExpr = substr($constExpr, 1);
                }
                if (strpos($constExpr, '"') === (strlen($constExpr) - 1)) {
                    $constExpr = substr($constExpr, 0, -1);
                }
                $condition->addOperands(
                    (new Operand())->setType('const')->setValue($constExpr)
                );
            }
        }

        if (!is_null($conditionTree->getLogicOperator())) {
            return $conditionTree;
        } else {
            return $condition;
        }
    }

    public function execute()
    {
        $resultSet = [];

        foreach ($this->schemas as $i => $schema) {
            if ($i > 0) {
                $resultSet = $this->joinResultSet($resultSet, $schema);
            } else {
                $resultSet = $this->storage->get(
                    $schema['table'],
                    $this->condition
                );
            }
        }

        $resultSet = $this->columnsFilter($resultSet, $this->columns);

        return $resultSet;
    }

    protected function joinResultSet($resultSet, $schema)
    {
        $joinHandler = self::JOIN_TYPE_HANDLER_MAPPING[$schema['join_type']];
        return $this->{$joinHandler}($resultSet, $schema);
    }

    protected function innerJoinResultSet($leftResultSet, $schema)
    {
        $joinedResultSet = [];

        foreach ($leftResultSet as $leftRow) {
            if ($schema['ref_type'] === 'ON') {
                $conditionTree = new ConditionTree();
                $conditionTree->setLogicOperator('and');
                if ($this->condition instanceof Condition) {
                    $operands = $this->condition->getOperands();
                    foreach ($operands as $operandIndex => $operand) {
                        if ($operand->getType() === 'colref') {
                            $operandValue = $operand->getValue();
                            if (array_key_exists($operandValue, $leftRow)) {
                                $operand->setValue($leftRow[$operandValue])->setType('const');
                            }
                        }
                    }
                } else {
                    //todo support condition tree
                }
                $conditionTree->addSubConditions($this->condition);
                $onCondition = $this->extractConditions($schema['ref_clause']);
                if ($onCondition instanceof Condition) {
                    $operands = $onCondition->getOperands();
                    foreach ($operands as $operandIndex => $operand) {
                        if ($operand->getType() === 'colref') {
                            $operandValue = $operand->getValue();
                            if (array_key_exists($operandValue, $leftRow)) {
                                $operand->setValue($leftRow[$operandValue])->setType('const');
                            }
                        }
                    }
                } else {
                    //todo support condition tree
                }
                $conditionTree->addSubConditions($onCondition);

                $rightResultSet = $this->storage->get(
                    $schema['table'],
                    $conditionTree
                );

                foreach ($rightResultSet as $rightRow) {
                    if ($this->joinConditionMatcher($leftRow, $rightRow, $onCondition)) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                    }
                }
            }
        }

        return $joinedResultSet;
    }

    protected function fillConditionWithResultSet($resultSet, Condition $condition)
    {
        $operands = $condition->getOperands();
        foreach ($operands as $operandIndex => $operand) {
            if ($operand->getType() === 'colref') {
                $operandValue = $operand->getValue();
                if (array_key_exists($operandValue, $resultSet)) {
                    $operand->setValue($resultSet[$operandValue])->setType('const');
                }
            }
        }
    }

    protected function leftJoinResultSet($leftResultSet, $schema)
    {
        $joinedResultSet = [];

        foreach ($leftResultSet as $leftRow) {
            $joined = false;

            if ($schema['ref_type'] === 'ON') {
                $conditionTree = new ConditionTree();
                $conditionTree->setLogicOperator('and');
                if ($this->condition instanceof Condition) {
                    $this->fillConditionWithResultSet($leftRow, $this->condition);
                } else {
                    //todo support condition tree
                }
                $conditionTree->addSubConditions($this->condition);
                $onCondition = $this->extractConditions($schema['ref_clause']);
                if ($onCondition instanceof Condition) {
                    $this->fillConditionWithResultSet($leftRow, $onCondition);
                } else {
                    //todo support condition tree
                }
                $conditionTree->addSubConditions($onCondition);

                $rightResultSet = $this->storage->get(
                    $schema['table'],
                    $conditionTree
                );

                foreach ($rightResultSet as $rightRow) {
                    if ($this->joinConditionMatcher($leftRow, $rightRow, $onCondition)) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                        $joined = true;
                    }
                }
            }

            if (!$joined) {
                $joinedResultSet[] = $leftRow; //todo fetch rightRow column from schema
            }
        }

        return $joinedResultSet;
    }

    protected function rightJoinResultSet($resultSet, $schema)
    {
        //todo

        return $resultSet;
    }

    protected function joinConditionMatcher($leftRow, $rightRow, $condition)
    {
        if ($condition instanceof Condition) {
            return $this->matchJoinCondition($leftRow, $rightRow, $condition);
        } else {
            return $this->matchJoinConditionTree($leftRow, $rightRow, $condition);
        }
    }

    protected function matchJoinCondition($leftRow, $rightRow, Condition $condition)
    {
        $operands = $condition->getOperands();
        foreach ($operands as $operandIndex => $operand) {
            if ($operand->getType() === 'colref') {
                $operandValue = $operand->getValue();
                if (array_key_exists($operandValue, $leftRow)) {
                    $operand->setValue($leftRow[$operandValue])->setType('const');
                } elseif (array_key_exists($operandValue, $rightRow)) {
                    $operand->setValue($rightRow[$operandValue])->setType('const');
                }
            }
        }

        if ($condition->getOperator() === '=') {
            $operands = $condition->getOperands();
            return $operands[0]->getValue() === $operands[1]->getValue();
        }

        //todo support more operators

        return false;
    }

    protected function matchJoinConditionTree($leftRow, $rightRow, ConditionTree $condition)
    {
        $result = true;
        $subConditions = $condition->getSubConditions();
        foreach ($subConditions as $i => $subCondition) {
            if ($subCondition instanceof Condition) {
                $subResult = $this->matchJoinCondition($leftRow, $rightRow, $subCondition);
            } else {
                $subResult = $this->joinConditionMatcher($leftRow, $rightRow, $subCondition);
            }
            if ($i === 0) {
                if ($condition->getLogicOperator() === 'not') {
                    $result = !$subResult;
                } else {
                    $result = $subResult;
                }
            } else {
                switch ($condition->getLogicOperator()) {
                    case 'and':
                        $result = ($result && $subResult);
                        break;
                    case 'or':
                        $result = ($result || $subResult);
                        break;
                    case 'not':
                        $result = ($result && (!$subResult));
                        break;
                }
            }
        }

        return $result;
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
