<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\consts\UDF;
use App\components\elements\Aggregation;
use App\components\elements\Column;
use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\elements\condition\Operand;
use App\components\elements\Group;
use App\components\elements\Order;
use App\components\math\OperatorHandler;
use App\components\storage\AbstractStorage;
use App\components\udf\Aggregate;
use App\components\udf\Math;

class QueryPlan
{
    const JOIN_HANDLERS = [
        'JOIN' => 'innerJoinResultSet',
        'LEFT' => 'leftJoinResultSet',
        'RIGHT' => 'rightJoinResultSet',
    ];

    const UDF = [
        'sin' => [Math::class, 'sin'],
        'cos' => [Math::class, 'cos'],
        'sqrt' => [Math::class, 'sqrt'],
        'pow' => [Math::class, 'pow'],
        'count' => [Aggregate::class, 'count'],
        'max' => [Aggregate::class, 'max'],
        'min' => [Aggregate::class, 'min'],
        'first' => [Aggregate::class, 'first'],
    ];

    /** @var Ast */
    protected $ast;

    /** @var AbstractStorage */
    protected $storage;

    /** @var Column[] */
    protected $columns = [];

    protected $schemas;

    /** @var Condition|ConditionTree|null  */
    protected $condition;

    /** @var Group[] */
    protected $groups;

    //todo
    protected $having;

    /** @var Order[] */
    protected $orders;

    protected $limit;

    //Optimization properties
    protected $storageGetLimit;
    protected $indexSuggestions;
    protected $nativeOrder;

    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;

//        var_dump($ast->getStmt());die;

        $this->storage = $storage;

        $this->columns = $this->extractColumns($ast->getStmt()['SELECT']);
        $this->extractSchemas();
        $this->condition = $this->extractWhereConditions();
        $this->extractGroups();
        $this->extractHaving();
        $this->extractOrders();
        $this->extractLimit();

        //todo sql校验
    }

    protected function extractWhereConditions()
    {
        $stmt = $this->ast->getStmt();
        if (!isset($stmt['WHERE'])) {
            return null;
        }
        return $this->extractConditions($this->ast->getStmt()['WHERE']);
    }

    protected function extractColumns($selectExpr)
    {
        $columns = [];
        foreach ($selectExpr as $columnExpr) {
            $column = (new Column())->setType($columnExpr['expr_type'])
                ->setValue($columnExpr['base_expr']);
            if (isset($columnExpr['alias']) && $columnExpr['alias'] !== false) {
                $column->setAlias($columnExpr['alias']);
            }
            if ($columnExpr['sub_tree'] !== false) {
                $column->setSubColumns($this->extractColumns($columnExpr['sub_tree']));
            }
            $columns[] = $column;
        }

        return $columns;
    }

    protected function extractSchemas()
    {
        $stmt = $this->ast->getStmt();
        if (isset($stmt['FROM'])) {
            $this->schemas = $stmt['FROM'];
        }
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
                        if ($condition->getOperator() === 'between') {
                            continue;
                        }
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

    protected function extractGroups()
    {
        $stmt = $this->ast->getStmt();
        if (!isset($stmt['GROUP'])) {
            return;
        }
        $groups = $this->ast->getStmt()['GROUP'];
        $this->groups = [];
        foreach ($groups as $group) {
            $this->groups[] = (new Group())->setType($group['expr_type'])
                ->setValue($group['base_expr']);
        }
    }

    protected function extractHaving()
    {
        //todo
    }

    protected function extractOrders()
    {
        $stmt = $this->ast->getStmt();
        if (!isset($stmt['ORDER'])) {
            return;
        }
        $orders = $this->ast->getStmt()['ORDER'];
        $this->orders = [];
        foreach ($orders as $order) {
            $this->orders[] = (new Order())->setType($order['expr_type'])
                ->setValue($order['base_expr'])
                ->setDirection($order['direction']);
        }
    }

    protected function extractLimit()
    {
        $stmt = $this->ast->getStmt();
        if (!isset($stmt['LIMIT'])) {
            return;
        }

        $this->limit = $stmt['LIMIT'];
    }

    /**
     * @param $row
     * @param Condition $condition
     * @return bool
     * @throws \Throwable
     */
    protected function filterConditionByIndexData($row, Condition $condition)
    {
        $operands = $condition->getOperands();

        $operandValues = [];
        foreach ($operands as $operand) {
            $operandType = $operand->getType();
            $operandValue = $operand->getValue();
            if ($operandType === 'colref') {
                $operandValues[] = $row[$operandValue];
            } else {
                $operandValues[] = $operandValue;
            }
        }

        return (new OperatorHandler())->calculateOperatorExpr($condition->getOperator(), ...$operandValues);
    }

    /**
     * @param $row
     * @param ConditionTree $conditionTree
     * @return bool
     * @throws \Throwable
     */
    protected function filterConditionTreeByIndexData($row, ConditionTree $conditionTree)
    {
        $subConditions = $conditionTree->getSubConditions();
        $result = true;
        foreach ($subConditions as $i => $subCondition) {
            if ($subCondition instanceof Condition) {
                $subResult = $this->filterConditionByIndexData($row, $subCondition);
            } else {
                $subResult = $this->filterConditionTreeByIndexData($row, $subCondition);
            }
            if ($i === 0) {
                if ($conditionTree->getLogicOperator() === 'not') {
                    $result = !$subResult;
                } else {
                    $result = $subResult;
                }
            } else {
                switch ($conditionTree->getLogicOperator()) {
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

    /**
     * @return array|mixed
     * @throws \Throwable
     */
    public function execute()
    {
        $resultSet = [];

        if (!is_null($this->schemas)) {
            $hasJoin = false;
            foreach ($this->schemas as $i => $schema) {
                if ($i > 0) {
                    $hasJoin = true;
                    $resultSet = $this->joinResultSet($resultSet, $schema);
                } else {
                    $resultSet = $this->storage->get(
                        $schema['table'],
                        $this->extractWhereConditions(),
                        $this->storageGetLimit,
                        $this->indexSuggestions
                    );
                }
            }

            if ($hasJoin) {
                $whereCondition = $this->extractWhereConditions();
                if (!is_null($whereCondition)) {
                    foreach ($resultSet as $i => $row) {
                        if ($whereCondition instanceof ConditionTree) {
                            if (!$this->filterConditionTreeByIndexData($row, $whereCondition)) {
                                unset($resultSet[$i]);
                            }
                        } else {
                            if (!$this->filterConditionByIndexData($row, $whereCondition)) {
                                unset($resultSet[$i]);
                            }
                        }
                    }
                    $resultSet = array_values($resultSet);
                }
            }
        } else {
            $resultSet[] = [];
        }

        $resultSet = $this->resultSetGroupFilter($resultSet);
        list($columns, $resultSet) = $this->resultSetUdfFilter($this->columns, $resultSet);
        $resultSet = $this->resultSetOrder($resultSet);
        $resultSet = $this->resultSetColumnsFilter($columns, $resultSet);
        $resultSet = $this->resultSetLimit($resultSet);

        return $resultSet;
    }

    protected function joinResultSet($resultSet, $schema)
    {
        //todo hash join
        $joinHandler = self::JOIN_HANDLERS[$schema['join_type']];
        return $this->{$joinHandler}($resultSet, $schema);
    }

    protected function fillConditionWithResultSet($resultRow, Condition $condition)
    {
        $filled = false;

        $operands = $condition->getOperands();
        foreach ($operands as $operandIndex => $operand) {
            if ($operand->getType() === 'colref') {
                $operandValue = $operand->getValue();
                if (strpos($operandValue, '.')) {
                    if (array_key_exists($operandValue, $resultRow)) {
                        $operand->setValue($resultRow[$operandValue])->setType('const');
                        $filled = true;
                    }
                }
            }
        }

        return $filled;
    }

    protected function fillConditionTreeWithResultSet($resultRow, ConditionTree $conditionTree)
    {
        $filled = false;

        foreach ($conditionTree->getSubConditions() as $subCondition) {
            if ($subCondition instanceof Condition) {
                if ($this->fillConditionWithResultSet($resultRow, $subCondition)) {
                    $filled = true;
                }
            } else {
                if ($this->fillConditionTreeWithResultSet($resultRow, $subCondition)) {
                    $filled = true;
                }
            }
        }

        return $filled;
    }

    /**
     * @param Condition $condition
     * @param $schema
     * @param $hashJoinField
     * @param bool $rightJoin
     * @return bool
     */
    protected function qualifiedHashJoin(Condition $condition, $schema, &$hashJoinField, $rightJoin = false)
    {
        $hashJoinField = null;
        if ($condition->getOperator() === '=') {
            $operands = $condition->getOperands();
            $operand1 = $operands[0];
            $operand1Type = $operand1->getType();
            $operand1Value = $operand1->getValue();
            $operand1Schema = null;
            if ($operand1Type === 'colref') {
                if (strpos($operand1Value, '.')) {
                    list($operand1Schema,) = explode('.', $operand1Value);
                }
            }
            $operand2 = $operands[1];
            $operand2Type = $operand2->getType();
            $operand2Value = $operand2->getValue();
            $operand2Schema = null;
            if ($operand2Type === 'colref') {
                if (strpos($operand2Value, '.')) {
                    list($operand2Schema,) = explode('.', $operand2Value);
                }
            }

            $schemaTable = $schema['table'];
            if ((!is_null($operand1Schema)) && (!is_null($operand2Schema))) {
                if (!$rightJoin) {
                    if (($operand1Schema === $schemaTable) && ($operand2Schema !== $schemaTable)) {
                        $hashJoinField = $operand2Value;
                    } elseif (($operand1Schema !== $schemaTable) && ($operand2Schema === $schemaTable)) {
                        $hashJoinField = $operand1Value;
                    }
                } else {
                    if (($operand1Schema !== $schemaTable) && ($operand2Schema === $schemaTable)) {
                        $hashJoinField = $operand2Value;
                    } elseif (($operand1Schema === $schemaTable) && ($operand2Schema !== $schemaTable)) {
                        $hashJoinField = $operand1Value;
                    }
                }
            }

            if (!is_null($hashJoinField)) {
                return true;
            }
        }

        return false;
    }

    protected function innerJoinResultSet($leftResultSet, $schema)
    {
        $hashJoinField = null;
        $leftResultHashMap = [];
        $onCondition = $this->extractConditions($schema['ref_clause']);
        if ($onCondition instanceof Condition) {
            $qualifiedHashJoin = $this->qualifiedHashJoin($onCondition, $schema, $hashJoinField);
        } else {
            $qualifiedHashJoin = false;
        }

        $joinedResultSet = [];

        $rightResultSetConditions = [];

        foreach ($leftResultSet as $leftRow) {
            if ($schema['ref_type'] === 'ON') {
                if ($qualifiedHashJoin) {
                    if (array_key_exists($hashJoinField, $leftRow)) {
                        $leftResultHashMap[$leftRow[$hashJoinField]][] = $leftRow;
                    }
                }

                $onCondition = $this->extractConditions($schema['ref_clause']);
                if ($onCondition instanceof Condition) {
                    $this->fillConditionWithResultSet($leftRow, $onCondition);
                } else {
                    $this->fillConditionTreeWithResultSet($leftRow, $onCondition);
                }

                $whereCondition = $this->extractWhereConditions();
                if (!is_null($whereCondition)) {
                    if ($whereCondition instanceof Condition) {
                        $this->fillConditionWithResultSet($leftRow, $whereCondition);
                    } else {
                        $this->fillConditionTreeWithResultSet($leftRow, $whereCondition);
                    }
                    $conditionTree = new ConditionTree();
                    $conditionTree->setLogicOperator('and');
                    $conditionTree->addSubConditions($whereCondition);
                    $conditionTree->addSubConditions($onCondition);
                    $rightResultSetCondition = $conditionTree;
                } else {
                    $rightResultSetCondition = $onCondition;
                }

                $rightResultSetConditions[] = $rightResultSetCondition;
            }
        }

        $rightResultSetConditionTree = new ConditionTree();
        $rightResultSetConditionTree->setLogicOperator('or')
            ->setSubConditions($rightResultSetConditions);

        $rightResultSet = $this->storage->get(
            $schema['table'],
            $rightResultSetConditionTree,
            $this->storageGetLimit,
            $this->indexSuggestions
        );

        if (!$qualifiedHashJoin) {
            foreach ($leftResultSet as $leftRow) {
                foreach ($rightResultSet as $rightRow) {
                    if ($this->joinConditionMatcher(
                        $leftRow,
                        $rightRow,
                        $this->extractConditions($schema['ref_clause'])
                    )) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                    }
                }
            }
        } else {
            $hashJoinValue = null;
            $onCondition = $this->extractConditions($schema['ref_clause']);
            $operands = $onCondition->getOperands();
            foreach ($rightResultSet as $rightRow) {
                foreach ($operands as $operandIndex => $operand) {
                    $operandValue = $operand->getValue();
                    if ($operand->getType() === 'colref') {
                        if ($operandValue !== $hashJoinField) {
                            if (array_key_exists($operandValue, $rightRow)) {
                                $hashJoinValue = $rightRow[$operandValue];
                            } else {
                                break 2;
                            }
                            break;
                        }
                    } else {
                        $hashJoinValue = $operandValue;
                        break;
                    }
                }
                if (array_key_exists($hashJoinValue, $leftResultHashMap)) {
                    foreach ($leftResultHashMap[$hashJoinValue] as $leftRow) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                    }
                }
            }
        }

        return $joinedResultSet;
    }

    protected function leftJoinResultSet($leftResultSet, $schema)
    {
        $hashJoinField = null;
        $leftResultHashMap = [];
        $onCondition = $this->extractConditions($schema['ref_clause']);
        if ($onCondition instanceof Condition) {
            $qualifiedHashJoin = $this->qualifiedHashJoin($onCondition, $schema, $hashJoinField);
        } else {
            $qualifiedHashJoin = false;
        }

        $joinedResultSet = [];

        $schemaTable = $schema['table'];
        $schemaMetaData = $this->storage->getSchemaMetaData($schemaTable);
        $schemaColumns = array_column($schemaMetaData['columns'], 'name');
        $emptyRightRow = [];
        foreach ($schemaColumns as $schemaColumn) {
            $emptyRightRow[$schemaColumn] = $emptyRightRow[$schemaTable . '.' . $schemaColumn] = null;
        }

        $rightResultSetConditions = [];

        foreach ($leftResultSet as $leftRow) {
            if ($schema['ref_type'] === 'ON') {
                if ($qualifiedHashJoin) {
                    if (array_key_exists($hashJoinField, $leftRow)) {
                        $leftResultHashMap[$leftRow[$hashJoinField]][] = $leftRow;
                    }
                }

                $onCondition = $this->extractConditions($schema['ref_clause']);
                if ($onCondition instanceof Condition) {
                    $this->fillConditionWithResultSet($leftRow, $onCondition);
                } else {
                    $this->fillConditionTreeWithResultSet($leftRow, $onCondition);
                }

                $whereCondition = $this->extractWhereConditions();
                if (!is_null($whereCondition)) {
                    if ($whereCondition instanceof Condition) {
                        $this->fillConditionWithResultSet($leftRow, $whereCondition);
                    } else {
                        $this->fillConditionTreeWithResultSet($leftRow, $whereCondition);
                    }
                    $conditionTree = new ConditionTree();
                    $conditionTree->setLogicOperator('and');
                    $conditionTree->addSubConditions($whereCondition);
                    $conditionTree->addSubConditions($onCondition);
                    $rightResultSetCondition = $conditionTree;
                } else {
                    $rightResultSetCondition = $onCondition;
                }

                $rightResultSetConditions[] = $rightResultSetCondition;
            }
        }

        $rightResultSetConditionTree = new ConditionTree();
        $rightResultSetConditionTree->setLogicOperator('or')
            ->setSubConditions($rightResultSetConditions);

        $rightResultSet = $this->storage->get(
            $schemaTable,
            $rightResultSetConditionTree,
            $this->storageGetLimit,
            $this->indexSuggestions
        );

        if (!$qualifiedHashJoin) {
            foreach ($leftResultSet as $leftRow) {
                $joined = false;

                foreach ($rightResultSet as $rightRow) {
                    if ($this->joinConditionMatcher($leftRow, $rightRow, $this->extractConditions($schema['ref_clause']))) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                        $joined = true;
                    }
                }

                if (!$joined) {
                    $joinedResultSet[] = $leftRow + $emptyRightRow;
                }
            }
        } else {
            $joinedLResultHashMap = [];
            $hashJoinValue = null;
            $onCondition = $this->extractConditions($schema['ref_clause']);
            $operands = $onCondition->getOperands();
            foreach ($rightResultSet as $rightRow) {
                foreach ($operands as $operandIndex => $operand) {
                    $operandValue = $operand->getValue();
                    if ($operand->getType() === 'colref') {
                        if ($operandValue !== $hashJoinField) {
                            if (array_key_exists($operandValue, $rightRow)) {
                                $hashJoinValue = $rightRow[$operandValue];
                            } else {
                                break 2;
                            }
                            break;
                        }
                    } else {
                        $hashJoinValue = $operandValue;
                        break;
                    }
                }
                if (array_key_exists($hashJoinValue, $leftResultHashMap)) {
                    $joinedLResultHashMap[$hashJoinValue] = [];
                    foreach ($leftResultHashMap[$hashJoinValue] as $leftRow) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                    }
                }
            }

            $unJoinedLRHashMap = array_diff_key($leftResultHashMap, $joinedLResultHashMap);

            foreach ($unJoinedLRHashMap as $hash => $leftRows) {
                foreach ($leftRows as $leftRow) {
                    $joinedResultSet[] = $leftRow + $emptyRightRow;
                }
            }
        }

        return $joinedResultSet;
    }

    protected function rightJoinResultSet($leftResultSet, $schema)
    {
        $hashJoinField = null;
        $rightResultHashMap = [];
        $onCondition = $this->extractConditions($schema['ref_clause']);
        if ($onCondition instanceof Condition) {
            $qualifiedHashJoin = $this->qualifiedHashJoin($onCondition, $schema, $hashJoinField, true);
        } else {
            $qualifiedHashJoin = false;
        }

        $joinedResultSet = [];

        $emptyLeftRow = [];
        foreach ($leftResultSet as $row) {
            foreach ($row as $key => $val) {
                $emptyLeftRow[$key] = null;
            }
        }

        $filledWithLeftResult = false;
        if (count($leftResultSet) > 0) {
            $whereCondition = $this->extractWhereConditions();
            if (!is_null($whereCondition)) {
                if ($whereCondition instanceof Condition) {
                    if ($this->fillConditionWithResultSet($leftResultSet[0], $whereCondition)) {
                        $filledWithLeftResult = true;
                    }
                } else {
                    if ($this->fillConditionTreeWithResultSet($leftResultSet[0], $whereCondition)) {
                        $filledWithLeftResult = true;
                    }
                }
            }
        }

        $schemaTable = $schema['table'];
        if (!$filledWithLeftResult) {
            $rightResultSet = $this->storage->get(
                $schemaTable,
                $this->extractWhereConditions(),
                $this->storageGetLimit,
                $this->indexSuggestions
            );
        } else {
            $rightResultSetConditions = [];

            $rightResultSet = [];
            foreach ($leftResultSet as $leftRow) {
                $whereCondition = $this->extractWhereConditions();
                if ($whereCondition instanceof Condition) {
                    $this->fillConditionWithResultSet($leftRow, $whereCondition);
                } else {
                    $this->fillConditionTreeWithResultSet($leftRow, $whereCondition);
                }

                $rightResultSetConditions[] = $whereCondition;
            }

            $rightResultSetConditionTree = new ConditionTree();
            $rightResultSetConditionTree->setLogicOperator('or')
                ->setSubConditions($rightResultSetConditions);

            $rightResultSet = array_merge($rightResultSet, $this->storage->get(
                $schemaTable,
                $rightResultSetConditions,
                $this->storageGetLimit,
                $this->indexSuggestions
            ));
        }

        if (!$qualifiedHashJoin) {
            foreach ($rightResultSet as $rightRow) {
                $joined = false;

                if ($schema['ref_type'] === 'ON') {
                    $onCondition = $this->extractConditions($schema['ref_clause']);
                    if ($onCondition instanceof Condition) {
                        $this->fillConditionWithResultSet($rightRow, $onCondition);
                    } else {
                        $this->fillConditionTreeWithResultSet($rightRow, $onCondition);
                    }

                    foreach ($leftResultSet as $leftRow) {
                        if ($this->joinConditionMatcher($leftRow, $rightRow, $onCondition)) {
                            $joinedResultSet[] = $leftRow + $rightRow;
                            $joined = true;
                        }
                    }
                }

                if (!$joined) {
                    $joinedResultSet[] = $emptyLeftRow + $rightRow;
                }
            }
        } else {
            foreach ($rightResultSet as $rightRow) {
                if ($schema['ref_type'] === 'ON') {
                    if ($qualifiedHashJoin) {
                        if (array_key_exists($hashJoinField, $rightRow)) {
                            $rightResultHashMap[$rightRow[$hashJoinField]][] = $rightRow;
                        }
                    }
                }
            }

            $joinedRResultHashMap = [];
            $hashJoinValue = null;
            $onCondition = $this->extractConditions($schema['ref_clause']);
            $operands = $onCondition->getOperands();
            foreach ($leftResultSet as $leftRow) {
                foreach ($operands as $operandIndex => $operand) {
                    $operandValue = $operand->getValue();
                    if ($operand->getType() === 'colref') {
                        if ($operandValue !== $hashJoinField) {
                            if (array_key_exists($operandValue, $leftRow)) {
                                $hashJoinValue = $leftRow[$operandValue];
                            } else {
                                break 2;
                            }
                            break;
                        }
                    } else {
                        $hashJoinValue = $operandValue;
                        break;
                    }
                }
                if (array_key_exists($hashJoinValue, $rightResultHashMap)) {
                    $joinedRResultHashMap[$hashJoinValue] = [];
                    foreach ($rightResultHashMap[$hashJoinValue] as $rightRow) {
                        $joinedResultSet[] = $leftRow + $rightRow;
                    }
                }
            }

            $unJoinedRRHashMap = array_diff_key($rightResultHashMap, $joinedRResultHashMap);

            foreach ($unJoinedRRHashMap as $hash => $rightRows) {
                foreach ($rightRows as $rightRow) {
                    $joinedResultSet[] = $emptyLeftRow + $rightRow;
                }
            }
        }

        return $joinedResultSet;
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

        $operandValues = [];
        foreach ($operands as $operandIndex => $operand) {
            $operandValue = $operand->getValue();
            if ($operand->getType() === 'colref') {
                if (array_key_exists($operandValue, $leftRow)) {
                    $operandValues[] = $leftRow[$operandValue];
                } elseif (array_key_exists($operandValue, $rightRow)) {
                    $operandValues[] = $rightRow[$operandValue];
                } else {
                    return false;
                }
            } else {
                $operandValues[] = $operandValue;
            }
        }

        return (new OperatorHandler())->calculateOperatorExpr($condition->getOperator(), ...$operandValues);

        //todo support more operators
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

    protected function aggregatedArrayToObject($aggregated, $oldDimension = [])
    {
        $aggregation = [];
        foreach ($aggregated as $dimension => $rows) {
            $newAggregation = new Aggregation();
            $newAggregation->setDimension($oldDimension);
            $newAggregation->addDimension($dimension);
            $newAggregation->setRows($rows);
            $aggregation[] = $newAggregation;
        }

        return $aggregation;
    }

    protected function resultSetGroupFilter($resultSet)
    {
        if (is_null($this->groups)) {
            return $resultSet;
        }

        foreach ($this->groups as $group) {
            $aggregated = [];
            foreach ($resultSet as $rowIndex => $row) {
                if ($row instanceof Aggregation) {
                    $aggregated = [];

                    foreach ($row->getRows() as $aggRowIndex => $aggRow) {
                        $aggregateField = $group->getValue();
                        if ($group->getType() === 'colref') {
                            $dimension = $aggRow[$aggregateField];
                        } else {
                            $dimension = $aggregateField;
                        }
                        $aggregated[$dimension][] = $aggRow;
                    }

                    foreach ($this->aggregatedArrayToObject($aggregated, $row->getDimension()) as $aggregation) {
                        $resultSet[] = $aggregation;
                    }

                    unset($resultSet[$rowIndex]);

                    $aggregated = [];
                } else {
                    $aggregateField = $group->getValue();
                    if ($group->getType() === 'colref') {
                        $dimension = $row[$aggregateField];
                    } else {
                        $dimension = $aggregateField;
                    }

                    $aggregated[$dimension][] = $row;
                }
            }

            if (count($aggregated) > 0) {
                $resultSet = $this->aggregatedArrayToObject($aggregated);
            }
        }

        return array_values($resultSet);
    }

    protected function getUdfColumnName(Column $column)
    {
        $udfParameters = [];

        foreach ($column->getSubColumns() as $subColumn) {
            if (!$subColumn->hasSubColumns()) {
                $udfParameters[] = $subColumn->getValue();
            } else {
                $udfParameters[] = $this->getUdfColumnName($subColumn);
            }
        }

        return $column->getValue() . '(' . implode(',', $udfParameters) . ')';
    }

    /**
     * @param $udfName
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return mixed
     * @throws \Exception
     */
    protected function executeUdf($udfName, $parameters, $row, $resultSet)
    {
        if (!array_key_exists($udfName, self::UDF)) {
            throw new \Exception('Invalid udf name');
        }

        $udf = self::UDF[$udfName];

        return call_user_func_array($udf, [$parameters, $row, $resultSet]);
    }

    /**
     * @param $udfName
     * @param $row
     * @param $resultSet
     * @param Column $column
     * @return mixed
     * @throws \Exception
     */
    protected function rowUdfFilter($udfName, $row, $resultSet, Column $column)
    {
        $udfParameters = [];
        foreach ($column->getSubColumns() as $subColumn) {
            if ($subColumn->hasSubColumns()) {
                $filtered = $this->rowUdfFilter($udfName, $row, $resultSet, $subColumn);
                $udfParameters[] = (new Column())->setValue($filtered)
                    ->setType('const');
            } else {
                $subColumnValue = $subColumn->getValue();
                $subColumnType = $subColumn->getType();
                if ($subColumnType === 'colref') {
                    if (in_array($udfName, UDF::AGGREGATE_UDF)) {
                        $udfParameters[] = (new Column())->setValue($subColumnValue)
                            ->setType('colref');
                    } else {
                        if ($subColumnValue !== '*') {
                            if ($row instanceof Aggregation) {
                                $udfParameters[] = (new Column())->setValue($subColumnValue)
                                    ->setType('colref');
                            } else {
                                $udfParameters[] = (new Column())->setValue($row[$subColumnValue])
                                    ->setType('const');
                            }
                        } else {
                            $udfParameters[] = (new Column())->setValue('*')
                                ->setType('colref');
                        }
                    }
                } else {
                    $udfParameters[] = (new Column())->setValue($subColumnValue)
                        ->setType($subColumnType);
                }
            }
        }

        return $this->executeUdf($udfName, $udfParameters, $row, $resultSet);
    }

    /**
     * @param Column[] $columns
     * @param $resultSet
     * @return array
     * @throws \Exception
     */
    protected function resultSetUdfFilter($columns, $resultSet)
    {
        $udfResultColumns = [];

        foreach ($columns as $columnIndex => $column) {
            foreach ($resultSet as $rowIndex => $row) {
                if ($row instanceof Aggregation) {
                    if (!$column->isUdf()) {
                        $firstUdfColumn = new Column();
                        $firstUdfColumn->setType('aggregate_function')
                            ->setValue('first')
                            ->setAlias($column->getAlias() ?? ['name' => $column->getValue()])
                            ->setSubColumns([$column]);
                        $column = $firstUdfColumn;
                        $columns[$columnIndex] = $column;
                    }
                }
                break;
            }

            if ($column->isUdf()) {
                $udfName = $column->getValue();
                $udfResultColumnName = $this->getUdfColumnName($column);
                $udfResultColumn = new Column();
                $udfResultColumn->setType('colref')
                    ->setValue($udfResultColumnName)
                    ->setAlias($column->getAlias());

                foreach ($resultSet as $rowIndex => $row) {
                    if (in_array($udfName, UDF::AGGREGATE_UDF)) {
                        if ((!is_object($row)) || (!($row instanceof Aggregation))) {
                            $resultSet = [(new Aggregation())->setRows($resultSet)];
                        }
                    }
                    break;
                }

                foreach ($resultSet as $rowIndex => $row) {
                    $filtered = $this->rowUdfFilter($udfName, $row, $resultSet, $column);
                    if (is_array($filtered)) {
                        $existedColumnNames = [];
                        foreach ($columns as $existedColumnIndex => $existedColumn) {
                            if (!$existedColumn->hasSubColumns()) {
                                $existedColumnNames[] = $existedColumn->getValue();
                            }
                        }
                        foreach ($udfResultColumns as $existedUdfColumnIndex => $existedUdfColumn) {
                            if (!$existedUdfColumn->hasSubColumns()) {
                                $existedColumnNames[] = $existedUdfColumn->getValue();
                            }
                        }
                        foreach ($filtered as $filteredKey => $filteredValue) {
                            if (!in_array($filteredKey, $existedColumnNames)) {
                                $columns[] = (new Column())->setValue($filteredKey)
                                    ->setType('colref');
                            }
                        }

                        if ($row instanceof Aggregation) {
                            $row->mergeAggregatedRow($filtered);
                        } else {
                            $row = array_merge($row, $filtered);
                            $resultSet[$rowIndex] = $row;
                        }
                        $udfResultColumn = null;
                    } else {
                        if ($row instanceof Aggregation) {
                            $row->setOneAggregatedRow($udfResultColumnName, $filtered);
                        } else {
                            $row[$udfResultColumnName] = $filtered;
                            $resultSet[$rowIndex] = $row;
                        }
                    }
                }

                if (!is_null($udfResultColumn)) {
                    $udfResultColumns[] = $udfResultColumn;
                }
            }
        }

        foreach ($resultSet as $rowIndex => $row) {
            if ($row instanceof Aggregation) {
                $resultSet[$rowIndex] = $row->getAggregatedRow();
            }
        }

        return [array_merge($columns, $udfResultColumns), $resultSet];
    }

    protected function resultSetOrder($resultSet)
    {
        if (is_null($this->orders)) {
            return $resultSet;
        }

        if ($this->nativeOrder === true) {
            return $resultSet;
        }

        $sortFuncParams = [];

        foreach ($this->orders as $order) {
            if ($order->getType() === 'const') {
                continue;
            }

            $sortFuncParams[] = array_column($resultSet, $order->getValue());
            $sortFuncParams[] = $order->getDirection() === 'ASC' ? SORT_ASC : SORT_DESC;
        }

        $sortFuncParams[] = &$resultSet;

        array_multisort(...$sortFuncParams);

        return $resultSet;
    }

    /**
     * @param Column[] $columns
     * @param $resultSet
     * @return mixed
     */
    protected function resultSetColumnsFilter($columns, $resultSet)
    {
        $columnNames = [];
        /** @var Column[] $constColumns */
        $constColumns = [];
        /** @var Column[] $aliasColumns */
        $aliasColumns = [];
        foreach ($columns as $column) {
            if (!$column->hasSubColumns()) {
                $columnAlias = $column->getAlias();
                $columnAliasName = isset($columnAlias) ? $columnAlias['name'] : null;
                $columnNames[] = $columnAliasName ?? $column->getValue();
                if (!is_null($columnAlias)) {
                    $aliasColumns[] = $column;
                }
                if ($column->getType() === 'const') {
                    $constColumns[] = $column;
                }
            }
        }
        foreach ($resultSet as $i => $row) {
            foreach ($constColumns as $constColumn) {
                $constColumnValue = $constColumn->getValue();
                $row[$constColumnValue] = $constColumnValue;
            }
            foreach ($aliasColumns as $aliasColumn) {
                $aliasColumnName = $aliasColumn->getAlias()['name'];
                $originColumnName = $aliasColumn->getValue();
                $row[$aliasColumnName] = $row[$originColumnName];
                unset($row[$originColumnName]);
            }
            if (!in_array('*', $columnNames)) {
                foreach ($row as $k => $v) {
                    if (!in_array($k, $columnNames)) {
                        unset($row[$k]);
                    }
                }
            }
            $resultSet[$i] = $row;
        }

        return $resultSet;
    }

    protected function resultSetLimit($resultSet)
    {
        if (!is_null($this->limit)) {
            $offset = ($this->limit['offset'] === '') ? 0 : ($this->limit['offset']);
            $limit = $this->limit['rowcount'];
            return array_slice($resultSet, $offset, $limit);
        }

        return $resultSet;
    }

    /**
     * @return Ast
     */
    public function getAst(): Ast
    {
        return $this->ast;
    }

    /**
     * @return AbstractStorage
     */
    public function getStorage(): AbstractStorage
    {
        return $this->storage;
    }

    /**
     * @return Column[]
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * @return mixed
     */
    public function getSchemas()
    {
        return $this->schemas;
    }

    /**
     * @return Condition|ConditionTree|null
     */
    public function getCondition()
    {
        return $this->condition;
    }

    /**
     * @return Group[]
     */
    public function getGroups(): ?array
    {
        return $this->groups;
    }

    /**
     * @return Order[]
     */
    public function getOrders(): ?array
    {
        return $this->orders;
    }

    /**
     * @return mixed
     */
    public function getLimit()
    {
        return $this->limit;
    }

    /**
     * @param Ast $ast
     * @return $this
     */
    public function setAst(Ast $ast): self
    {
        $this->ast = $ast;
        return $this;
    }

    /**
     * @param AbstractStorage $storage
     * @return $this
     */
    public function setStorage(AbstractStorage $storage): self
    {
        $this->storage = $storage;
        return $this;
    }

    /**
     * @param array $columns
     * @return $this
     */
    public function setColumns(array $columns): self
    {
        $this->columns = $columns;
        return $this;
    }

    /**
     * @param $schemas
     * @return $this
     */
    public function setSchemas($schemas): self
    {
        $this->schemas = $schemas;
        return $this;
    }

    /**
     * @param $condition
     * @return $this
     */
    public function setCondition($condition): self
    {
        $this->condition = $condition;
        return $this;
    }

    /**
     * @param array|null $groups
     * @return $this
     */
    public function setGroups(?array $groups): self
    {
        $this->groups = $groups;
        return $this;
    }

    /**
     * @param array|null $orders
     * @return $this
     */
    public function setOrders(?array $orders): self
    {
        $this->orders = $orders;
        return $this;
    }

    /**
     * @param $limit
     * @return $this
     */
    public function setLimit($limit): self
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * @param $storageGetLimit
     * @return $this
     */
    public function setStorageGetLimit($storageGetLimit): self
    {
        $this->storageGetLimit = $storageGetLimit;
        return $this;
    }

    /**
     * @param $indexSuggestions
     * @return $this
     */
    public function setIndexSuggestions($indexSuggestions): self
    {
        $this->indexSuggestions = $indexSuggestions;
        return $this;
    }

    /**
     * @param $schema
     * @param $column
     * @param $indexInfo
     * @return $this
     */
    public function setOneIndexSuggestion($schema, $column, $indexInfo): self
    {
        $this->indexSuggestions[$schema][$column] = $indexInfo;
        return $this;
    }

    /**
     * @param bool $nativeOrder
     * @return $this
     */
    public function setNativeOrder(bool $nativeOrder): self
    {
        $this->nativeOrder = $nativeOrder;
        return $this;
    }
}
