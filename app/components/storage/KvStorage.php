<?php

namespace App\components\storage;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\elements\condition\Operand;
use App\components\math\OperatorHandler;
use Co\Channel;
use SwFwLess\components\swoole\Scheduler;

abstract class KvStorage extends AbstractStorage
{
    protected $filterConditionCache = [];

    protected $schemaMetaCache = [];

    abstract protected function openBtree($name, $new = false);

    abstract protected function metaSchemaGet($btree, $schemaName);

    abstract protected function dataSchemaGetAll($btree, $indexName);

    abstract protected function dataSchemaGetById($btree, $id, $schema);

    abstract protected function dataSchemaScan(
        $btree, $indexName, &$startKey, &$endKey, $limit, $callback, &$skipFirst = false
    );

    abstract protected function dataSchemaMGet($btree, $schema, $idList);

    abstract protected function dataSchemaCountAll($btree, $schema);

    abstract protected function dataSchemaSet($btree, $indexName, $id, $value);

    abstract protected function dataSchemaDel($btree, $indexName, $id);

    /**
     * @param $schema
     * @return mixed|null
     * @throws \Throwable
     */
    public function getSchemaMetaData($schema)
    {
        $cache = Scheduler::withoutPreemptive(function () use ($schema) {
            if (array_key_exists($schema, $this->schemaMetaCache)) {
                return $this->schemaMetaCache[$schema];
            }

            return false;
        });

        if ($cache !== false) {
            return $cache;
        }

        $metaSchema = $this->openBtree('meta.schema');
        $schemaData = $this->metaSchemaGet($metaSchema, $schema);

        if (!$schemaData) {
            $result = null;
        } else {
            $result = json_decode($schemaData, true);
        }

        Scheduler::withoutPreemptive(function () use ($schema, $result) {
            $this->schemaMetaCache[$schema] = $result;
        });

        return $result;
    }

    /**
     * @param $schema
     * @return mixed
     * @throws \Throwable
     */
    protected function getPrimaryKeyBySchema($schema)
    {
        $metaSchema = $this->getSchemaMetaData($schema);
        if (!$metaSchema) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        return $metaSchema['pk'];
    }

    /**
     * @param $schema
     * @param $key
     * @param $start
     * @param $end
     * @return array
     * @throws \Throwable
     */
    protected function partitionByRange($schema, $key, $start, $end)
    {
        $schemaMeta = $this->getSchemaMetaData($schema);
        if (!$schemaMeta) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        if (!isset($schemaMeta['partition'])) {
            return null;
        }

        $partition = $schemaMeta['partition'];
        if ($partition['key'] !== $key) {
            return null;
        }

        $ranges = $partition['range'];

        $startPartitionIndex = null;
        $endPartitionIndex = null;
        foreach ($ranges as $rangeIndex => $range) {
            if ((($range['lower'] === '') || (($start !== '') && ($range['lower'] <= $start))) &&
                (($range['upper'] === '') || (($start === '') || ($range['upper'] >= $start)))
            ) {
                $startPartitionIndex = $rangeIndex;
            }
            if ((($range['lower'] === '') || (($end === '') || ($range['lower'] <= $end))) &&
                (($range['upper'] === '') || (($end !== '') && ($range['upper'] >= $end)))
            ) {
                $endPartitionIndex = $rangeIndex;
                break;
            }
        }

        if (is_null($startPartitionIndex)) {
            throw new \Exception('Invalid start partition index');
        }

        if (is_null($endPartitionIndex)) {
            throw new \Exception('Invalid end partition index');
        }

        return [$startPartitionIndex, $endPartitionIndex];
    }

    /**
     * @param $schema
     * @param $key
     * @param $start
     * @param $end
     * @return int|mixed
     * @throws \Throwable
     */
    protected function countPartitionByRange($schema, $key, $start, $end)
    {
        $partitions = $this->partitionByRange($schema, $key, $start, $end);

        if (is_null($partitions)) {
            return 0;
        }

        list($startPartitionIndex, $endPartitionIndex) = $partitions;

        return ($endPartitionIndex - $startPartitionIndex) + 1;
    }

    /**
     * @param $schema
     * @param $condition
     * @param bool $isNot
     * @return float|int|mixed
     * @throws \Throwable
     */
    protected function countPartitionByCondition($schema, $condition, bool $isNot = false)
    {
        if ($condition instanceof ConditionTree) {
            $logicOperator = $condition->getLogicOperator();
            $subConditions = $condition->getSubConditions();
            $isNot = $isNot || ($logicOperator === 'not');

            if ($logicOperator === 'and') {
                $costList = [];
                foreach ($subConditions as $subCondition) {
                    $cost = $this->countPartitionByCondition($schema, $subCondition, $isNot);
                    if ($cost > 0) {
                        $costList[] = $cost;
                    }
                }
                return (count($costList) > 0) ? min($costList) : 0;
            } elseif ($logicOperator === 'or') {
                $costList = [];
                foreach ($subConditions as $subCondition) {
                    $costList[] = $this->countPartitionByCondition($schema, $subCondition, $isNot);
                }
                return array_sum($costList);
            } elseif ($logicOperator === 'not') {
                $costList = [];
                foreach ($subConditions as $subCondition) {
                    if ($subCondition instanceof Condition) {
                        $cost = $this->countPartitionByCondition($schema, $subCondition, $isNot);
                        if ($cost > 0) {
                            $costList[] = $cost;
                        }
                    } else {
                        if ($isNot && ($subCondition->getLogicOperator() === 'not')) {
                            foreach ($subCondition as $subSubCondition) {
                                $cost = $this->countPartitionByCondition($schema, $subSubCondition);
                                if ($cost > 0) {
                                    $costList[] = $cost;
                                }
                            }
                        } else {
                            $cost = $this->countPartitionByCondition($schema, $subCondition, $isNot);
                            if ($cost > 0) {
                                $costList[] = $cost;
                            }
                        }
                    }
                }
                return (count($costList) > 0) ? min($costList) : 0;
            }

            return 0;
        }

        if ($condition instanceof Condition) {
            $cost = 0;

            $conditionOperator = $condition->getOperator();
            $operands = $condition->getOperands();

            if (in_array($conditionOperator, ['<', '<=', '=', '>', '>='])) {
                $operandValue1 = $operands[0]->getValue();
                $operandType1 = $operands[0]->getType();
                if ($operandType1 === 'colref') {
                    if (strpos($operandValue1, '.')) {
                        list(, $operandValue1) = explode('.', $operandValue1);
                    }
                }
                $operandValue2 = $operands[1]->getValue();
                $operandType2 = $operands[1]->getType();
                if ($operandType2 === 'colref') {
                    if (strpos($operandValue2, '.')) {
                        list(, $operandValue2) = explode('.', $operandValue2);
                    }
                }

                if ((($operandType1 === 'colref') && ($operandType2 === 'const')) ||
                    (($operandType1 === 'const') && ($operandType2 === 'colref'))
                ) {
                    if ((($operandType1 === 'colref') && ($operandType2 === 'const'))) {
                        $field = $operandValue1;
                        $conditionValue = $operandValue2;
                    } else {
                        $field = $operandValue2;
                        $conditionValue = $operandValue1;
                    }

                    $itStart = '';
                    $itEnd = '';

                    if ($conditionOperator === '=') {
                        if (!$isNot) {
                            $itStart = $conditionValue;
                            $itEnd = $conditionValue;
                        }
                    } elseif ($conditionOperator === '<') {
                        if ($isNot) {
                            $itStart = $conditionValue;
                        } else {
                            $itEnd = $conditionValue;
                        }
                    } elseif ($conditionOperator === '<=') {
                        if ($isNot) {
                            $itStart = $conditionValue;
                        } else {
                            $itEnd = $conditionValue;
                        }
                    } elseif ($conditionOperator === '>') {
                        if ($isNot) {
                            $itEnd = $conditionValue;
                        } else {
                            $itStart = $conditionValue;
                        }
                    } elseif ($conditionOperator === '>=') {
                        if ($isNot) {
                            $itEnd = $conditionValue;
                        } else {
                            $itStart = $conditionValue;
                        }
                    }

                    $cost = $this->countPartitionByRange($schema, $field, $itStart, $itEnd);
                }
            } elseif ($conditionOperator === 'between') {
                $operandValue1 = $operands[0]->getValue();
                $operandType1 = $operands[0]->getType();
                if ($operandType1 === 'colref') {
                    if (strpos($operandValue1, '.')) {
                        list(, $operandValue1) = explode('.', $operandValue1);
                    }
                }

                $operandValue2 = $operands[1]->getValue();
                $operandType2 = $operands[1]->getType();
                if ($operandType2 === 'colref') {
                    if (strpos($operandValue2, '.')) {
                        list(, $operandValue2) = explode('.', $operandValue2);
                    }
                }

                $operandValue3 = $operands[2]->getValue();
                $operandType3 = $operands[2]->getType();
                if ($operandType3 === 'colref') {
                    if (strpos($operandValue3, '.')) {
                        list(, $operandValue3) = explode('.', $operandValue3);
                    }
                }

                if ($operandType1 === 'colref' && $operandType2 === 'const' && $operandType3 === 'const') {
                    if ($isNot) {
                        $itStart = '';
                        $itEnd = $operands[1];
                        $cost = $this->countPartitionByRange($schema, $operandValue1, $itStart, $itEnd);

                        $itStart = $operands[2];
                        $itEnd = '';
                        $cost += $this->countPartitionByRange($schema, $operandValue1, $itStart, $itEnd);
                    } else {
                        $itStart = $operandValue2;
                        $itEnd = $operandValue3;

                        $cost = $this->countPartitionByRange($schema, $operandValue1, $itStart, $itEnd);
                    }
                }
            }

            return $cost;
        }

        return 0;
    }

    /**
     * @param $indexName
     * @param $partitionIndex
     * @return string
     */
    protected function getIndexPartitionName($indexName, $partitionIndex)
    {
        return $indexName . '.partition.' . ((string)$partitionIndex);
    }

    /**
     * @param $colName
     * @param $schema
     * @return bool
     * @throws \Throwable
     */
    protected function colrefBelongsToSchema($colName, $schema)
    {
        $schemaMetaData = $this->getSchemaMetaData($schema);
        if (!$schemaMetaData) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        $schemaColumns = array_column($schemaMetaData['columns'], 'name');

        if (strpos($colName, '.')) {
            list($operandSchema, $colName) = explode('.', $colName);
            if (($operandSchema !== $schema) ||
                (!in_array($colName, $schemaColumns))
            ) {
                return false;
            }
        } else {
            if (!in_array($colName, $schemaColumns)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param $schema
     * @param $condition
     * @param null $parentOperator
     * @return Condition|ConditionTree|mixed|null
     * @throws \Throwable
     */
    protected function filterConditionWithSchema($schema, $condition, $parentOperator = null)
    {
        if (is_null($condition)) {
            return null;
        }

        if ($condition instanceof Condition) {
            $operands = $condition->getOperands();

            foreach ($operands as $operand) {
                $operandType = $operand->getType();
                $operandValue = $operand->getValue();
                if ($operandType === 'colref') {
                    if (!$this->colrefBelongsToSchema($operandValue, $schema)) {
                        if (is_null($parentOperator)) {
                            return null;
                        } elseif ($parentOperator === 'and') {
                            return null;
                        } elseif ($parentOperator === 'not') {
                            $filteredConditionTree = new ConditionTree();
                            $filteredConditionTree->setLogicOperator('not');
                            $filteredCondition = (new Condition())->setOperator('=')
                                ->addOperands(
                                    (new Operand())->setType('const')
                                        ->setValue(1)
                                )
                                ->addOperands(
                                    (new Operand())->setType('const')
                                        ->setValue(1)
                                );
                            $filteredConditionTree->addSubConditions($filteredCondition);
                            return $filteredConditionTree;
                        } else {
                            return (new Condition())->setOperator('=')
                                ->addOperands(
                                    (new Operand())->setType('const')
                                        ->setValue(1)
                                )
                                ->addOperands(
                                    (new Operand())->setType('const')
                                        ->setValue(1)
                                );
                        }
                    }
                }
            }

            return $condition;
        }

        if ($condition instanceof ConditionTree) {
            $subConditions = $condition->getSubConditions();

            foreach ($subConditions as $i => $subCondition) {
                if (is_null($this->filterConditionWithSchema($schema, $subCondition, $condition->getLogicOperator()))) {
                    unset($subConditions[$i]);
                }
            }

            $subConditions = array_values($subConditions);

            if (count($subConditions) <= 0) {
                return null;
            }

            if (count($subConditions) === 1) {
                if ($condition->getLogicOperator() !== 'not') {
                    return $condition->setSubConditions($subConditions);
                } else {
                    return $subConditions[0];
                }
            } else {
                return $condition->setSubConditions($subConditions);
            }
        }

        return null;
    }

    /**
     * @param $schema
     * @param $condition
     * @param $limit
     * @param $indexSuggestions
     * @param $usedColumns
     * @return array
     * @throws \Throwable
     */
    public function get($schema, $condition, $limit, $indexSuggestions, $usedColumns)
    {
        $condition = $this->filterConditionWithSchema($schema, $condition);
        $indexData = $this->conditionFilter(
            $schema,
            $condition,
            $condition,
            $limit,
            $indexSuggestions,
            $usedColumns
        );

        foreach ($indexData as $i => $row) {
            foreach ($row as $column => $value) {
                $row[$schema . '.' . $column] = $value;
            }
            $indexData[$i] = $row;
        }

        if (!is_null($limit)) {
            $offset = ($limit['offset'] === '') ? 0 : ($limit['offset']);
            $limit = $limit['rowcount'];
            $indexData = array_slice($indexData, $offset, $limit);
        }

        return array_values($indexData);
    }

    /**
     * @param $schema
     * @return int
     * @throws \Throwable
     */
    public function countAll($schema)
    {
        if (is_null($this->getSchemaMetaData($schema))) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        $btree = $this->openBtree($schema);
        if ($btree === false) {
            return 0;
        }

        return $this->dataSchemaCountAll($btree, $schema);
    }

    /**
     * @param $schema
     * @param $limit
     * @return array|mixed
     * @throws \Throwable
     */
    protected function fetchAllPrimaryIndexData($schema, $limit)
    {
        if (is_null($this->getSchemaMetaData($schema))) {
            throw new \Exception('Schema '. $schema .' not exists');
        }

        $indexName = $schema;

        $index = $this->openBtree($indexName);
        if ($index === false) {
            return [];
        }

        if (is_null($limit)) {
            $indexData = $this->dataSchemaGetAll($index, $indexName);

            array_walk($indexData, function (&$val) {
                $val = json_decode($val, true);
            });

            return $indexData;
        }

        $itLimit = 10000; //must greater than 1
        if ($itLimit <= 1) {
            throw new \Exception('Scan limit must greater than 1');
        }

        $offsetLimitCount = null;
        if (!is_null($limit)) {
            $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
            $limitCount = $limit['rowcount'];
            $offsetLimitCount = $offset + $limitCount;
        }

        $indexData = [];
        $startKey = '';
        $endKey = '';
        $skipFirst = false;
        $this->dataSchemaScan(
            $index,
            $indexName,
            $startKey,
            $endKey,
            $itLimit,
            function ($subIndexData, $resultCount) use (
                &$skipFirst, &$startKey, $itLimit, $offsetLimitCount, &$indexData
            ) {
                array_walk($subIndexData, function (&$row, $key) use (&$startKey) {
                    $startKey = $key;
                    $row = json_decode($row, true);
                });

                $indexData = array_merge($indexData, $subIndexData);

                if ($resultCount < $itLimit) {
                    return false;
                }

                if (!is_null($offsetLimitCount)) {
                    if (count($indexData) >= $offsetLimitCount) {
                        return false;
                    }
                }

                if (!$skipFirst) {
                    $skipFirst = true;
                }

                return true;
            },
            $skipFirst
        );

        return $indexData;
    }

    /**
     * @param $id
     * @param $schema
     * @return mixed|null
     * @throws \Throwable
     */
    protected function fetchPrimaryIndexDataById($id, $schema)
    {
        if (is_null($this->getSchemaMetaData($schema))) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        $index = $this->openBtree($schema);
        if ($index === false) {
            return null;
        }

        $indexData = $this->dataSchemaGetById($index, $id, $schema);

        if (!$indexData) {
            return null;
        } else {
            return json_decode($indexData, true);
        }
    }

    /**
     * @param $schema
     * @param $row
     * @param Condition $condition
     * @return bool
     * @throws \Throwable
     */
    protected function filterConditionByIndexData($schema, $row, Condition $condition)
    {
        $operands = $condition->getOperands();

        $operandValues = [];

        foreach ($operands as $i => $operand) {
            $operandType = $operand->getType();
            $operandValue = $operand->getValue();
            if ($operandType === 'colref') {
                if (strpos($operandValue, '.')) {
                    list(, $operandValue) = explode('.', $operandValue);
                }
                if (!array_key_exists($operandValue, $row)) {
                    $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                }
                $operandValues[$i] = $row[$operandValue];
            } else {
                $operandValues[$i] = $operandValue;
            }
        }

        return (new OperatorHandler())->calculateOperatorExpr($condition->getOperator(), ...$operandValues);
    }

    /**
     * @param $schema
     * @param $row
     * @param ConditionTree $conditionTree
     * @return bool
     * @throws \Throwable
     */
    protected function filterConditionTreeByIndexData($schema, $row, ConditionTree $conditionTree)
    {
        $subConditions = $conditionTree->getSubConditions();
        $result = true;
        foreach ($subConditions as $i => $subCondition) {
            if ($subCondition instanceof Condition) {
                $subResult = $this->filterConditionByIndexData($schema, $row, $subCondition);
            } else {
                $subResult = $this->filterConditionTreeByIndexData($schema, $row, $subCondition);
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
     * @param $schema
     * @param $rootCondition
     * @param Condition $condition
     * @param $limit
     * @param $indexSuggestions
     * @param $isNot
     * @param $usedColumns
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterBasicCompareCondition(
        $schema,
        $rootCondition,
        Condition $condition,
        $limit,
        $indexSuggestions,
        $isNot,
        $usedColumns
    )
    {
        $operatorHandler = new OperatorHandler($isNot);
        $conditionOperator = $condition->getOperator();
        $operands = $condition->getOperands();
        $operandValue1 = $operands[0]->getValue();
        $operandType1 = $operands[0]->getType();
        if ($operandType1 === 'colref') {
            if (strpos($operandValue1, '.')) {
                list(, $operandValue1) = explode('.', $operandValue1);
            }
        }
        $operandValue2 = $operands[1]->getValue();
        $operandType2 = $operands[1]->getType();
        if ($operandType2 === 'colref') {
            if (strpos($operandValue2, '.')) {
                list(, $operandValue2) = explode('.', $operandValue2);
            }
        }

        if ((($operandType1 === 'colref') && ($operandType2 === 'const')) ||
            (($operandType1 === 'const') && ($operandType2 === 'colref'))
        ) {
            if ((($operandType1 === 'colref') && ($operandType2 === 'const'))) {
                $field = $operandValue1;
                $conditionValue = $operandValue2;
            } else {
                $field = $operandValue2;
                $conditionValue = $operandValue1;
            }

            if ($this->countPartitionByRange($schema, $field, '', '') > 0) {
                $itStart = '';
                $itEnd = '';

                $itLimit = 10000; //must greater than 1
                if ($itLimit <= 1) {
                    throw new \Exception('Scan limit must greater than 1');
                }

                $offset = null;
                $limitCount = null;
                $offsetLimitCount = null;
                if (!is_null($limit)) {
                    $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                    $limitCount = $limit['rowcount'];
                    $offsetLimitCount = $offset + $limitCount;
                }

                if ($conditionOperator === '=') {
                    if (!$isNot) {
                        $itStart = $conditionValue;
                        $itEnd = $conditionValue;
                    }
                } elseif ($conditionOperator === '<') {
                    if ($isNot) {
                        $itStart = $conditionValue;
                    } else {
                        $itEnd = $conditionValue;
                    }
                } elseif ($conditionOperator === '<=') {
                    if ($isNot) {
                        $itStart = $conditionValue;
                    } else {
                        $itEnd = $conditionValue;
                    }
                } elseif ($conditionOperator === '>') {
                    if ($isNot) {
                        $itEnd = $conditionValue;
                    } else {
                        $itStart = $conditionValue;
                    }
                } elseif ($conditionOperator === '>=') {
                    if ($isNot) {
                        $itEnd = $conditionValue;
                    } else {
                        $itStart = $conditionValue;
                    }
                }

                $partitions = $this->partitionByRange($schema, $field, $itStart, $itEnd);

                list($partitionStartIndex, $partitionEndIndex) = $partitions;

                $indexData = [];

                $usingPrimaryIndex = ($field === $this->getPrimaryKeyBySchema($schema));

                $coroutineTotal = 3;
                $coroutineCount = 0;
                $channel = new Channel($coroutineTotal);

                for ($partitionIndex = $partitionStartIndex; $partitionIndex <= $partitionEndIndex; ++$partitionIndex) {
                    go(function () use (
                        $usingPrimaryIndex, $schema, $field, $partitionIndex,
                        $itStart, $itEnd, $itLimit, $operatorHandler,
                        $conditionOperator, $conditionValue, $rootCondition,
                        $offsetLimitCount, $channel, $usedColumns
                    ) {
                        $indexName = $this->getIndexPartitionName(
                            $usingPrimaryIndex ? $schema : ($schema . '.' . $field),
                            $partitionIndex
                        );

                        $index = $this->openBtree($indexName);
                        if ($index === false) {
                            $channel->push([]);
                            return;
                        }

                        $indexData = [];
                        $skipFirst = false;
                        $this->dataSchemaScan(
                            $index,
                            $indexName,
                            $itStart,
                            $itEnd,
                            $itLimit,
                            function ($formattedResult, $resultCount) use (
                                &$indexData, $operatorHandler, $conditionOperator, $conditionValue,
                                $usingPrimaryIndex, $rootCondition, $schema, $offsetLimitCount, &$skipFirst,
                                &$itStart, $itLimit, $usedColumns
                            ) {
                                $subIndexData = [];

                                foreach ($formattedResult as $key => $data) {
                                    $itStart = $key;
                                    if (!$operatorHandler->calculateOperatorExpr(
                                        $conditionOperator,
                                        ...[$key, $conditionValue]
                                    )) {
                                        continue;
                                    }
                                    if ($usingPrimaryIndex) {
                                        $arrData = json_decode($data, true);
                                        $subIndexData[] = $arrData;
                                    } else {
                                        $subIndexData = array_merge($subIndexData, json_decode($data, true));
                                    }
                                }

                                //Filter by root condition
                                if (!$usingPrimaryIndex) {
                                    if (count($subIndexData) > 0) {
                                        $indexColumns = array_keys($subIndexData[0]);
                                        if (is_null($usedColumns) ||
                                            in_array('*', $usedColumns) ||
                                            (count(array_diff($usedColumns, $indexColumns)) > 0)
                                        ) {
                                            $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                                        }
                                    }
                                }
                                if ($rootCondition instanceof ConditionTree) {
                                    $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                        return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                                    });
                                }

                                $indexData = array_merge($indexData, $subIndexData);

                                if (!is_null($offsetLimitCount)) {
                                    if (count($indexData) >= $offsetLimitCount) {
                                        return false;
                                    }
                                }

                                //Check EOF
                                if ($resultCount < $itLimit) {
                                    return false;
                                }

                                if (!$skipFirst) {
                                    $skipFirst = true;
                                }

                                return true;
                            },
                            $skipFirst
                        );

                        $channel->push(array_values($indexData));
                    });

                    ++$coroutineCount;
                    if ($coroutineCount === $coroutineTotal) {
                        for ($coroutineIndex = 0; $coroutineIndex < $coroutineCount; ++$coroutineIndex) {
                            $indexData = array_merge($indexData, $channel->pop());
                            if (!is_null($offsetLimitCount)) {
                                if (count($indexData) >= $offsetLimitCount) {
                                    $coroutineCount = 0;
                                    break 2;
                                }
                            }
                        }
                        $coroutineCount = 0;
                    }
                }

                if ($coroutineCount > 0) {
                    for ($i = 0; $i < $coroutineCount; ++$i) {
                        $indexData = array_merge($indexData, $channel->pop());
                        if (!is_null($offsetLimitCount)) {
                            if (count($indexData) >= $offsetLimitCount) {
                                break;
                            }
                        }
                    }
                }

                return array_values($indexData);
            } else {
                list($indexName, $usingPrimaryIndex) = $this->selectIndex($schema, $field);
                $index = $this->openBtree($indexName);

                $itStart = '';
                $itEnd = '';

                $itLimit = 10000; //must greater than 1
                $offset = null;
                $limitCount = null;
                $offsetLimitCount = null;
                if (!is_null($limit)) {
                    $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                    $limitCount = $limit['rowcount'];
                    $offsetLimitCount = $offset + $limitCount;
                }

                if ((!$usingPrimaryIndex) || ($field === $this->getPrimaryKeyBySchema($schema))) {
                    if ($conditionOperator === '=') {
                        if (!$isNot) {
                            $itStart = $conditionValue;
                            $itEnd = $conditionValue;
                        }
                    } elseif ($conditionOperator === '<') {
                        if ($isNot) {
                            $itStart = $conditionValue;
                        } else {
                            $itEnd = $conditionValue;
                        }
                    } elseif ($conditionOperator === '<=') {
                        if ($isNot) {
                            $itStart = $conditionValue;
                        } else {
                            $itEnd = $conditionValue;
                        }
                    } elseif ($conditionOperator === '>') {
                        if ($isNot) {
                            $itEnd = $conditionValue;
                        } else {
                            $itStart = $conditionValue;
                        }
                    } elseif ($conditionOperator === '>=') {
                        if ($isNot) {
                            $itEnd = $conditionValue;
                        } else {
                            $itStart = $conditionValue;
                        }
                    }
                }

                $indexData = [];
                $skipFirst = false;
                $this->dataSchemaScan(
                    $index,
                    $indexName,
                    $itStart,
                    $itEnd,
                    $itLimit,
                    function ($formattedResult, $resultCount) use (
                        &$indexData, &$itStart, $usingPrimaryIndex, $conditionOperator,
                        $operatorHandler, $conditionValue, $field, $rootCondition,
                        $schema, $itLimit, &$skipFirst, $offsetLimitCount, $usedColumns
                    ) {
                        $subIndexData = [];

                        foreach ($formattedResult as $key => $data) {
                            $itStart = $key;

                            if (!$usingPrimaryIndex) {
                                if (!$operatorHandler->calculateOperatorExpr(
                                    $conditionOperator,
                                    ...[$key, $conditionValue]
                                )) {
                                    continue;
                                }
                            } else {
                                $arrData = json_decode($data, true);
                                if (!$operatorHandler->calculateOperatorExpr(
                                    $conditionOperator,
                                    ...[$arrData[$field], $conditionValue]
                                )) {
                                    continue;
                                }
                            }

                            if ($usingPrimaryIndex) {
                                $subIndexData[] = json_decode($data, true);
                            } else {
                                $subIndexData = array_merge($subIndexData, json_decode($data, true));
                            }
                        }

                        //Filter by root condition
                        if (!$usingPrimaryIndex) {
                            if (count($subIndexData) > 0) {
                                $indexColumns = array_keys($subIndexData[0]);
                                if (is_null($usedColumns) ||
                                    in_array('*', $usedColumns) ||
                                    (count(array_diff($usedColumns, $indexColumns)) > 0)
                                ) {
                                    $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                                }
                            }
                        }
                        if ($rootCondition instanceof ConditionTree) {
                            $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                            });
                        }

                        $indexData = array_merge($indexData, $subIndexData);

                        //Check EOF
                        if ($resultCount < $itLimit) {
                            return false;
                        }

                        if (!$skipFirst) {
                            $skipFirst = true;
                        }

                        if (!is_null($offsetLimitCount)) {
                            if (count($indexData) >= $offsetLimitCount) {
                                return false;
                            }
                        }

                        return true;
                    },
                    $skipFirst
                );

                return array_values($indexData);
            }
        } elseif ($operandType1 === 'const' && $operandType2 === 'const') {
            if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$operandValue1, $operandValue2])) {
                return $this->fetchAllPrimaryIndexData($schema, $limit);
            } else {
                return [];
            }
        } else {
            return [];
        }
    }

    /**
     * @param $schema
     * @param $rootCondition
     * @param Condition $condition
     * @param $limit
     * @param $indexSuggestions
     * @param $isNot
     * @param $usedColumns
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterBetweenCondition(
        $schema,
        $rootCondition,
        Condition $condition,
        $limit,
        $indexSuggestions,
        $isNot,
        $usedColumns
    )
    {
        $operatorHandler = new OperatorHandler($isNot);
        $operands = $condition->getOperands();

        $operandValue1 = $operands[0]->getValue();
        $operandType1 = $operands[0]->getType();
        if ($operandType1 === 'colref') {
            if (strpos($operandValue1, '.')) {
                list(, $operandValue1) = explode('.', $operandValue1);
            }
        }

        $operandValue2 = $operands[1]->getValue();
        $operandType2 = $operands[1]->getType();
        if ($operandType2 === 'colref') {
            if (strpos($operandValue2, '.')) {
                list(, $operandValue2) = explode('.', $operandValue2);
            }
        }

        $operandValue3 = $operands[2]->getValue();
        $operandType3 = $operands[2]->getType();
        if ($operandType3 === 'colref') {
            if (strpos($operandValue3, '.')) {
                list(, $operandValue3) = explode('.', $operandValue3);
            }
        }

        if ($operandType1 === 'colref' && $operandType2 === 'const' && $operandType3 === 'const') {
            if ($this->countPartitionByRange($schema, $operandValue1, '', '') > 0) {
                $itLimit = 10000; //must greater than 1
                $offset = null;
                $limitCount = null;
                $offsetLimitCount = null;
                if (!is_null($limit)) {
                    $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                    $limitCount = $limit['rowcount'];
                    $offsetLimitCount = $offset + $limitCount;
                }

                if ($isNot) {
                    $splitConditionTree = new ConditionTree();
                    $splitConditionTree->setLogicOperator('and')
                        ->addSubConditions(
                            (new Condition())->setOperator('<')
                                ->addOperands($operands[0])
                                ->addOperands($operands[1])
                        )
                        ->addSubConditions(
                            (new Condition())->setOperator('>')
                                ->addOperands($operands[0])
                                ->addOperands($operands[2])
                        );
                    return $this->filterConditionTree(
                        $schema,
                        $rootCondition,
                        $splitConditionTree,
                        $limit,
                        $indexSuggestions,
                        $usedColumns
                    );
                } else {
                    $itStart = $operandValue2;
                    $itEnd = $operandValue3;
                }

                $partitions = $this->partitionByRange($schema, $operandValue1, $itStart, $itEnd);

                list($partitionStartIndex, $partitionEndIndex) = $partitions;

                $indexData = [];

                $usingPrimaryIndex = ($operandValue1 === $this->getPrimaryKeyBySchema($schema));

                $coroutineTotal = 3;
                $coroutineCount = 0;
                $channel = new Channel($coroutineTotal);

                for ($partitionIndex = $partitionStartIndex; $partitionIndex <= $partitionEndIndex; ++$partitionIndex) {
                    go(function () use (
                        $usingPrimaryIndex, $schema, $operandValue1, $partitionIndex,
                        $channel, $itStart, $itEnd, $itLimit, $offsetLimitCount, $operatorHandler,
                        $operandValue2, $operandValue3, $rootCondition, $usedColumns
                    ) {
                        $indexName = $this->getIndexPartitionName(
                            $usingPrimaryIndex ? $schema : ($schema . '.' . $operandValue1),
                            $partitionIndex
                        );

                        $index = $this->openBtree($indexName);
                        if ($index === false) {
                            $channel->push([]);
                            return;
                        }

                        $indexData = [];
                        $skipFirst = false;
                        $this->dataSchemaScan(
                            $index,
                            $indexName,
                            $itStart,
                            $itEnd,
                            $itLimit,
                            function ($formattedResult, $resultCount) use (
                                &$indexData, $operatorHandler, $operandValue2, $operandValue3,
                                $usingPrimaryIndex, $schema, $rootCondition, $offsetLimitCount,
                                $itLimit, &$skipFirst, &$itStart, $usedColumns
                            ) {
                                $subIndexData = [];

                                foreach ($formattedResult as $key => $data) {
                                    $itStart = $key;

                                    if (!$operatorHandler->calculateOperatorExpr(
                                        'between',
                                        ...[$key, $operandValue2, $operandValue3]
                                    )) {
                                        continue;
                                    }

                                    if ($usingPrimaryIndex) {
                                        $subIndexData[] = json_decode($data, true);
                                    } else {
                                        $subIndexData = array_merge($subIndexData, json_decode($data, true));
                                    }
                                }

                                //Filter by root condition
                                if (!$usingPrimaryIndex) {
                                    if (count($subIndexData) > 0) {
                                        $indexColumns = array_keys($subIndexData[0]);
                                        if (is_null($usedColumns) ||
                                            in_array('*', $usedColumns) ||
                                            (count(array_diff($usedColumns, $indexColumns)) > 0)
                                        ) {
                                            $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                                        }
                                    }
                                }
                                if ($rootCondition instanceof ConditionTree) {
                                    $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                        return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                                    });
                                }
                                $indexData = array_merge($indexData, $subIndexData);

                                if (!is_null($offsetLimitCount)) {
                                    if (count($indexData) >= $offsetLimitCount) {
                                        return false;
                                    }
                                }

                                //EOF
                                if ($resultCount < $itLimit) {
                                    return false;
                                }

                                if (!$skipFirst) {
                                    $skipFirst = true;
                                }

                                return true;
                            },
                            $skipFirst
                        );

                        $channel->push(array_values($indexData));
                    });

                    ++$coroutineCount;
                    if ($coroutineCount === $coroutineTotal) {
                        for ($coroutineIndex = 0; $coroutineIndex < $coroutineCount; ++$coroutineIndex) {
                            $indexData = array_merge($indexData, $channel->pop());
                            if (!is_null($offsetLimitCount)) {
                                if (count($indexData) >= $offsetLimitCount) {
                                    $coroutineCount = 0;
                                    break 2;
                                }
                            }
                        }
                        $coroutineCount = 0;
                    }
                }

                if ($coroutineCount > 0) {
                    for ($i = 0; $i < $coroutineCount; ++$i) {
                        $indexData = array_merge($indexData, $channel->pop());
                        if (!is_null($offsetLimitCount)) {
                            if (count($indexData) >= $offsetLimitCount) {
                                break;
                            }
                        }
                    }
                }

                return array_values($indexData);
            } else {
                list($indexName, $usingPrimaryIndex) = $this->selectIndex($schema, $operandValue1);
                $index = $this->openBtree($indexName);

                $itStart = '';
                $itEnd = '';
                $itLimit = 10000; //must greater than 1
                $offset = null;
                $limitCount = null;
                $offsetLimitCount = null;
                if (!is_null($limit)) {
                    $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                    $limitCount = $limit['rowcount'];
                    $offsetLimitCount = $offset + $limitCount;
                }

                if ((!$usingPrimaryIndex) || ($operandValue1 === $this->getPrimaryKeyBySchema($schema))) {
                    if ($isNot) {
                        $splitConditionTree = new ConditionTree();
                        $splitConditionTree->setLogicOperator('and')
                            ->addSubConditions(
                                (new Condition())->setOperator('<')
                                    ->addOperands($operands[0])
                                    ->addOperands($operands[1])
                            )
                            ->addSubConditions(
                                (new Condition())->setOperator('>')
                                    ->addOperands($operands[0])
                                    ->addOperands($operands[2])
                            );
                        return $this->filterConditionTree(
                            $schema,
                            $rootCondition,
                            $splitConditionTree,
                            $limit,
                            $indexSuggestions,
                            $usedColumns
                        );
                    } else {
                        $itStart = $operandValue2;
                        $itEnd = $operandValue3;
                    }
                }

                $indexData = [];
                $skipFirst = false;
                $this->dataSchemaScan(
                    $index,
                    $indexName,
                    $itStart,
                    $itEnd,
                    $itLimit,
                    function ($formattedResult, $resultCount) use (
                        &$indexData, &$itStart, $usingPrimaryIndex, $operatorHandler,
                        $operandValue1, $operandValue2, $operandValue3, $schema, $rootCondition,
                        $itLimit, &$skipFirst, $offsetLimitCount, $usedColumns
                    ) {
                        $subIndexData = [];

                        foreach ($formattedResult as $key => $data) {
                            $itStart = $key;

                            if ($usingPrimaryIndex) {
                                $arrData = json_decode($data, true);
                                if (!$operatorHandler->calculateOperatorExpr(
                                    'between',
                                    ...[$arrData[$operandValue1], $operandValue2, $operandValue3]
                                )) {
                                    continue;
                                }
                            } else {
                                if (!$operatorHandler->calculateOperatorExpr(
                                    'between',
                                    ...[$key, $operandValue2, $operandValue3]
                                )) {
                                    continue;
                                }
                            }

                            if ($usingPrimaryIndex) {
                                $subIndexData[] = json_decode($data, true);
                            } else {
                                $subIndexData = array_merge($subIndexData, json_decode($data, true));
                            }
                        }

                        //Filter by root condition
                        if (!$usingPrimaryIndex) {
                            if (count($subIndexData) > 0) {
                                $indexColumns = array_keys($subIndexData[0]);
                                if (is_null($usedColumns) ||
                                    in_array('*', $usedColumns) ||
                                    (count(array_diff($usedColumns, $indexColumns)) > 0)
                                ) {
                                    $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                                }
                            }
                        }
                        if ($rootCondition instanceof ConditionTree) {
                            $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                            });
                        }
                        $indexData = array_merge($indexData, $subIndexData);

                        //EOF
                        if ($resultCount < $itLimit) {
                            return false;
                        }

                        if (!$skipFirst) {
                            $skipFirst = true;
                        }

                        if (!is_null($offsetLimitCount)) {
                            if (count($indexData) >= $offsetLimitCount) {
                                return false;
                            }
                        }

                        return true;
                    },
                    $skipFirst
                );

                return array_values($indexData);
            }
        } else {
            return [];
        }
    }

    /**
     * @param $schema
     * @param $rootCondition
     * @param Condition $condition
     * @param $limit
     * @param $indexSuggestions
     * @param $usedColumns
     * @param bool $isNot
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterCondition(
        $schema,
        $rootCondition,
        Condition $condition,
        $limit,
        $indexSuggestions,
        $usedColumns,
        bool $isNot = false
    )
    {
        $operandsCacheKey = [];
        foreach ($condition->getOperands() as $operand) {
            $operandsCacheKey[] = [
                'value' => $operand->getValue(),
                'type' => $operand->getType(),
            ];
        }
        $cacheKey = json_encode([
            'operator' => $condition->getOperator(),
            'operands' => $operandsCacheKey,
        ]);

        $cache = Scheduler::withoutPreemptive(function () use ($cacheKey) {
            if (array_key_exists($cacheKey, $this->filterConditionCache)) {
                return $this->filterConditionCache[$cacheKey];
            }

            return null;
        });

        if (!is_null($cache)) {
            return $cache;
        }

        $conditionOperator = $condition->getOperator();
        if (in_array($conditionOperator, ['<', '<=', '=', '>', '>='])) {
            $result = $this->filterBasicCompareCondition(
                $schema, $rootCondition, $condition, $limit, $indexSuggestions, $isNot, $usedColumns
            );
        } elseif ($conditionOperator === 'between') {
            $result = $this->filterBetweenCondition(
                $schema, $rootCondition, $condition, $limit, $indexSuggestions, $isNot, $usedColumns
            );
        } else {
            $result = [];
        }

        Scheduler::withoutPreemptive(function () use ($cacheKey, $result) {
            $this->filterConditionCache[$cacheKey] = $result;
        });

        return $result;
        //todo support more operators
    }

    /**
     * @param $schema
     * @param $rootCondition
     * @param ConditionTree $conditionTree
     * @param $limit
     * @param $indexSuggestions
     * @param $usedColumns
     * @param bool $isNot
     * @return array
     * @throws \Throwable
     */
    protected function filterConditionTree(
        $schema,
        $rootCondition,
        ConditionTree $conditionTree,
        $limit,
        $indexSuggestions,
        $usedColumns,
        bool $isNot = false
    )
    {
        $logicOperator = $conditionTree->getLogicOperator();

        $isNot = ($logicOperator === 'not') || $isNot;

        $result = [];

        $subConditions = $conditionTree->getSubConditions();

        if ($logicOperator === 'and') {
            if (count($subConditions) === 2) {
                $subCondition1Col = null;
                $subCondition1Operator = null;
                $subCondition1Value = null;
                $subCondition2Col = null;
                $subCondition2Operator = null;
                $subCondition2Value = null;
                $subCondition1 = $subConditions[0];
                $subCondition2 = $subConditions[1];
                if ($subCondition1 instanceof Condition) {
                    $subCondition1Operands = $subCondition1->getOperands();
                    if (($subCondition1Operands[0]->getType() === 'colref') &&
                        ($subCondition1Operands[1]->getType() === 'const')
                    ) {
                        $subCondition1Col = $subCondition1Operands[0]->getValue();
                        $subCondition1Value = $subCondition1Operands[1]->getValue();
                    }
                    if (($subCondition1Operands[0]->getType() === 'const') &&
                        ($subCondition1Operands[1]->getType() === 'colref')
                    ) {
                        $subCondition1Col = $subCondition1Operands[1]->getValue();
                        $subCondition1Value = $subCondition1Operands[0]->getValue();
                    }
                    $subCondition1Operator = $subCondition1->getOperator();
                }
                if ($subCondition2 instanceof Condition) {
                    $subCondition2Operands = $subCondition2->getOperands();
                    if (($subCondition2Operands[0]->getType() === 'colref') &&
                        ($subCondition2Operands[1]->getType() === 'const')
                    ) {
                        $subCondition2Col = $subCondition2Operands[0]->getValue();
                        $subCondition2Value = $subCondition2Operands[1]->getValue();
                    }
                    if (($subCondition2Operands[0]->getType() === 'const') &&
                        ($subCondition2Operands[1]->getType() === 'colref')
                    ) {
                        $subCondition2Col = $subCondition2Operands[1]->getValue();
                        $subCondition2Value = $subCondition2Operands[0]->getValue();
                    }
                    $subCondition2Operator = $subCondition2->getOperator();
                }

                if ((!is_null($subCondition1Col)) &&
                    (!is_null($subCondition1Operator)) &&
                    (!is_null($subCondition1Value)) &&
                    (!is_null($subCondition2Col)) &&
                    (!is_null($subCondition2Operator)) &&
                    (!is_null($subCondition2Value))
                ) {
                    if ($subCondition1Col === $subCondition2Col) {
                        if (($subCondition1Operator === '>=') && ($subCondition2Operator === '<=')) {
                            $subConditions = [
                                (new Condition())->setOperator('between')
                                    ->addOperands(
                                        (new Operand())->setType('colref')
                                            ->setValue($subCondition1Col)
                                    )
                                    ->addOperands(
                                        (new Operand())->setType('const')
                                            ->setValue($subCondition1Value)
                                    )
                                    ->addOperands(
                                        (new Operand())->setType('const')
                                            ->setValue($subCondition2Value)
                                    )
                            ];
                        }
                        if (($subCondition1Operator === '<=') && ($subCondition2Operator === '>=')) {
                            $subConditions = [
                                (new Condition())->setOperator('between')
                                    ->addOperands(
                                        (new Operand())->setType('colref')
                                            ->setValue($subCondition1Col)
                                    )
                                    ->addOperands(
                                        (new Operand())->setType('const')
                                            ->setValue($subCondition2Value)
                                    )
                                    ->addOperands(
                                        (new Operand())->setType('const')
                                            ->setValue($subCondition1Value)
                                    )
                            ];
                        }
                    }
                }
            }

            $costList = [];
            foreach ($subConditions as $subCondition) {
                if ($subCondition instanceof Condition) {
                    $cost = $this->countPartitionByCondition($schema, $subCondition, $isNot);
                    if ($cost > 0) {
                        $costList[] = $cost;
                    }
                } else {
                    if ($isNot && ($subCondition->getLogicOperator() === 'not')) {
                        foreach ($subCondition as $subSubCondition) {
                            $cost = $this->countPartitionByCondition($schema, $subSubCondition);
                            if ($cost > 0) {
                                $costList[] = $cost;
                            }
                        }
                    } else {
                        foreach ($subCondition as $subSubCondition) {
                            $cost = $this->countPartitionByCondition($schema, $subSubCondition, $isNot);
                            if ($cost > 0) {
                                $costList[] = $cost;
                            }
                        }
                    }
                }
            }

            $minCost = count($costList) > 0 ? min($costList) : 0;
            if ($minCost > 0) {
                $minCostConditionIndex = array_search($minCost, $costList);
                $subConditions = [$subConditions[$minCostConditionIndex]];
            } else {
                $subConditions = array_slice($subConditions, 0, 1);
            }
        }

        $coroutineTotal = 3;
        $coroutineCount = 0;
        $channel = new Channel($coroutineTotal);

        foreach ($subConditions as $i => $subCondition) {
            go(function () use (
                $subCondition, $schema, $limit, $indexSuggestions, $isNot, $channel, $rootCondition, $usedColumns
            ) {
                if ($subCondition instanceof Condition) {
                    $subResult = $this->filterCondition(
                        $schema,
                        $rootCondition,
                        $subCondition,
                        $limit,
                        $indexSuggestions,
                        $usedColumns,
                        $isNot
                    );
                } else {
                    if ($isNot && ($subCondition->getLogicOperator() === 'not')) {
                        $subResult = [];
                        foreach ($subCondition->getSubConditions() as $j => $subSubCondition) {
                            if ($subSubCondition instanceof Condition) {
                                $subResult = array_merge($subResult, $this->filterCondition(
                                    $schema,
                                    $rootCondition,
                                    $subSubCondition,
                                    $limit,
                                    $indexSuggestions,
                                    $usedColumns
                                ));
                            } else {
                                $subResult = array_merge($subResult, $this->filterConditionTree(
                                    $schema,
                                    $rootCondition,
                                    $subSubCondition,
                                    $limit,
                                    $indexSuggestions,
                                    $usedColumns
                                ));
                            }
                        }
                    } else {
                        $subResult = $this->filterConditionTree(
                            $schema,
                            $rootCondition,
                            $subCondition,
                            $limit,
                            $indexSuggestions,
                            $usedColumns,
                            $isNot
                        );
                    }
                }

                $channel->push($subResult);
            });

            ++$coroutineCount;
            if ($coroutineCount === $coroutineTotal) {
                for ($coroutineIndex = 0; $coroutineIndex < $coroutineCount; ++$coroutineIndex) {
                    $result = array_merge($result, $channel->pop());
                    if (!is_null($limit)) {
                        $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                        $limitCount = $limit['rowcount'];
                        $offsetLimitCount = $offset + $limitCount;
                        if (count($result) >= $offsetLimitCount) {
                            $coroutineCount = 0;
                            break 2;
                        }
                    }
                }
                $coroutineCount = 0;
            }
        }

        if ($coroutineCount > 0) {
            for ($i = 0; $i < $coroutineCount; ++$i) {
                $result = array_merge($result, $channel->pop());
            }
        }

        $idMap = [];
        foreach ($result as $i => $row) {
            if (in_array($row['id'], $idMap)) {
                unset($result[$i]);
            } else {
                $idMap[] = $row['id'];
            }
        }

        return array_values($result);
    }

    /**
     * @param $indexData
     * @param $schema
     * @return mixed
     * @throws \Throwable
     */
    protected function fetchAllColumnsByIndexData($indexData, $schema)
    {
        $index = $this->openBtree($schema);
        if ($index === false) {
            return [];
        }

        $idList = array_column($indexData, $this->getPrimaryKeyBySchema($schema));
        if (count($idList) <= 0) {
            return [];
        }

        $rows = $this->dataSchemaMGet($index, $schema, $idList);

        $rows = array_filter($rows);

        array_walk($rows, function (&$row) {
            $row = json_decode($row, true);
        });

        return array_values($rows);
    }

    /**
     * Fetching index data by single condition, then filtering index data by all conditions.
     *
     * @param $schema
     * @param $rootCondition
     * @param $condition
     * @param $limit
     * @param $indexSuggestions
     * @param $usedColumns
     * @return array
     * @throws \Throwable
     */
    protected function conditionFilter(
        $schema, $rootCondition, $condition, $limit, $indexSuggestions, $usedColumns
    )
    {
        if (!is_null($condition)) {
            if ($condition instanceof Condition) {
                $indexData = $this->filterCondition(
                    $schema,
                    $rootCondition,
                    $condition,
                    $limit,
                    $indexSuggestions,
                    $usedColumns
                );
            } else {
                $indexData = $this->filterConditionTree(
                    $schema,
                    $rootCondition,
                    $condition,
                    $limit,
                    $indexSuggestions,
                    $usedColumns
                );
            }
        } else {
            $indexData = $this->fetchAllPrimaryIndexData($schema, $limit);
        }

        return $indexData;
    }

    /**
     * @param $schema
     * @param $rows
     * @return int
     * @throws \Throwable
     */
    public function set($schema, $rows)
    {
        $affectedRows = 0;

        $schemaMeta = $this->getSchemaMetaData($schema);

        $pk = $schemaMeta['pk'];
        $pIndex = $this->openBtree($schema);
        foreach ($rows as $row) {
            if (!$this->dataSchemaSet($pIndex, $schema, $row[$pk], json_encode($row))) {
                continue;
            }

            if (isset($schemaMeta['index'])) {
                if (!$this->setIndex($schemaMeta, $schema, $row)) {
                    continue;
                }
            }

            if (isset($schemaMeta['partition'])) {
                if (!$this->setPartitionIndex($schemaMeta, $schema, $row)) {
                    continue;
                }
            }

            ++$affectedRows;
        }

        return $affectedRows;
    }

    protected function setIndex($schemaMeta, $schema, $row)
    {
        $pk = $schemaMeta['pk'];

        foreach ($schemaMeta['index'] as $indexConfig) {
            $indexBtree = $this->openBtree($schema . '.' . $indexConfig['name']);
            $indexPk = $indexConfig['columns'][0];
            //todo append or unique (atomic、batch put)
            if (!$this->dataSchemaSet(
                $indexBtree,
                $schema . '.' . $indexConfig['name'],
                $row[$indexPk],
                json_encode([[$pk => $row[$pk]]])
            )) {
                return false;
            }
        }

        return true;
    }

    protected function setPartitionIndex($schemaMeta, $schema, $row)
    {
        $pk = $schemaMeta['pk'];

        $partition = $schemaMeta['partition'];
        $partitionPk = $partition['key'];
        $partitionPkVal = $row[$partitionPk];

        $targetPartitionIndex = null;
        foreach ($partition['range'] as $rangeIndex => $range) {
            if ((($range['lower'] === '') || ($partitionPkVal >= $range['lower'])) &&
                (($range['upper'] === '') || ($partitionPkVal <= $range['upper']))
            ) {
                $targetPartitionIndex = $rangeIndex;
                break;
            }
        }

        if (!is_null($targetPartitionIndex)) {
            if ($partitionPk === $pk) {
                $partitionIndexName = $schema . '.partition.' . (string)$targetPartitionIndex;
                $partitionIndexData = json_encode($row);
            } else {
                $partitionIndexName = $schema . '.' . $partitionPk . '.partition.' . (string)$targetPartitionIndex;
                $partitionIndexData = json_encode([[$pk => $row[$pk]]]);
            }
            $partitionIndex = $this->openBtree($partitionIndexName);
            return $this->dataSchemaSet(
                $partitionIndex,
                $partitionIndexName,
                $partitionPkVal,
                $partitionIndexData
            );
        }

        return true;
    }

    /**
     * @param $schema
     * @param $column
     * @return array
     * @throws \Throwable
     */
    protected function selectIndex($schema, $column)
    {
        $schemaMetaData = $this->getSchemaMetaData($schema);
        if (is_null($schemaMetaData)) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        $pk = $schemaMetaData['pk'];

        if ($pk === $column) {
            return [$schema, true];
        }

        $indexMeta = $schemaMetaData['index'] ?? [];

        foreach ($indexMeta as $indexMetaData) {
            if (($indexMetaData['columns'][0] ?? null) === $column) {
                return [$schema . '.' . $indexMetaData['name'], false];
            }
        }

        return [$schema, true];
    }

    /**
     * @param $schema
     * @param $pkList
     * @return int
     * @throws \Throwable
     */
    public function del($schema, $pkList)
    {
        $schemaMetaData = $this->getSchemaMetaData($schema);
        if (is_null($schemaMetaData)) {
            throw new \Exception('Schema ' . $schema . ' not exists');
        }

        $deleted = 0;

        $pIndex = $this->openBtree($schema);
        foreach ($pkList as $pk) {
            if (isset($schemaMetaData['index'])) {
                if (!$this->delIndex($schemaMetaData, $schema, $pk)) {
                    continue;
                }
            }

            if (isset($schemaMetaData['partition'])) {
                if (!$this->delPartitionIndex($schemaMetaData, $schema, $pk)) {
                    continue;
                }
            }

            if (!$this->dataSchemaDel($pIndex, $schema, $pk)) {
                continue;
            }

            ++$deleted;
        }

        return $deleted;
    }

    /**
     * @param $schemaMeta
     * @param $schema
     * @param $pk
     * @return bool
     * @throws \Throwable
     */
    protected function delIndex($schemaMeta, $schema, $pk)
    {
        $row = $this->fetchPrimaryIndexDataById($pk, $schema);
        if (is_null($row)) {
            return false;
        }

        foreach ($schemaMeta['index'] as $indexConfig) {
            $indexBtree = $this->openBtree($schema . '.' . $indexConfig['name']);
            $indexPk = $indexConfig['columns'][0];
            //todo atomic、batch put
            $indexData = $this->dataSchemaGetById(
                $indexBtree,
                $row[$indexPk],
                $schema . '.' . $indexConfig['name']
            );
            if (!is_null($indexData)) {
                $indexRows = json_decode($indexData, true);
                foreach ($indexRows as $i => $indexRow) {
                    if ($indexRow[$schemaMeta['pk']] === $pk) {
                        unset($indexRows[$i]);
                    }
                }
                if (!$this->dataSchemaSet(
                    $indexBtree,
                    $schema . '.' . $indexConfig['name'],
                    $row[$indexPk],
                    json_encode($indexRows)
                )) {
                    return false;
                }
            } else {
                if (!$this->dataSchemaDel(
                    $indexBtree,
                    $schema . '.' . $indexConfig['name'],
                    $row[$indexPk]
                )) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @param $schemaMeta
     * @param $schema
     * @param $pk
     * @return bool
     * @throws \Throwable
     */
    protected function delPartitionIndex($schemaMeta, $schema, $pk)
    {
        $row = $this->fetchPrimaryIndexDataById($pk, $schema);
        if (is_null($row)) {
            return false;
        }

        $partition = $schemaMeta['partition'];
        $partitionPk = $partition['key'];
        $partitionPkVal = $row[$partitionPk];

        $targetPartitionIndex = null;
        foreach ($partition['range'] as $rangeIndex => $range) {
            if ((($range['lower'] === '') || ($partitionPkVal >= $range['lower'])) &&
                (($range['upper'] === '') || ($partitionPkVal <= $range['upper']))
            ) {
                $targetPartitionIndex = $rangeIndex;
                break;
            }
        }

        if (!is_null($targetPartitionIndex)) {
            if ($partitionPk === $schemaMeta['pk']) {
                $partitionIndexName = $schema . '.partition.' . (string)$targetPartitionIndex;
            } else {
                $partitionIndexName = $schema . '.' . $partitionPk . '.partition.' . (string)$targetPartitionIndex;
            }
            $partitionIndex = $this->openBtree($partitionIndexName);
            return $this->dataSchemaDel(
                $partitionIndex,
                $partitionIndexName,
                $partitionPkVal
            );
        }

        return true;
    }
}
