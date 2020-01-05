<?php

namespace App\components\storage\pika;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\elements\condition\Operand;
use App\components\math\OperatorHandler;
use App\components\storage\AbstractStorage;
use Co\Channel;
use SwFwLess\components\redis\RedisWrapper;
use SwFwLess\components\swoole\Scheduler;
use SwFwLess\facades\RedisPool;

class Pika extends AbstractStorage
{
    protected $filterConditionCache = [];

    protected $schemaMetaCache = [];

    /**
     * @param $index
     * @param $callback
     * @return mixed
     * @throws \Throwable
     */
    protected function safeUseIndex($index, $callback) {
        try {
            return call_user_func_array($callback, [$index]);
        } catch (\Throwable $e) {
            throw $e;
        } finally {
            RedisPool::release($index);
        }
    }

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
        $schemaData = $this->safeUseIndex($metaSchema, function (RedisWrapper $metaSchema) use ($schema) {
            return $metaSchema->hGet('meta.schema', $schema);
        });

        if ($schemaData === false) {
            $result = null;
        } else {
            $result = json_decode($schemaData, true);
        }

        Scheduler::withoutPreemptive(function () use ($schema, $result) {
            $this->schemaMetaCache[$schema] = $result;
        });

        return $result;
    }

    protected function getPrimaryKeyBySchema($schema)
    {
        //todo
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
     * @return array
     * @throws \Throwable
     */
    public function get($schema, $condition, $limit, $indexSuggestions)
    {
        $condition = $this->filterConditionWithSchema($schema, $condition);
        return $this->conditionFilter($schema, $condition, $condition, $limit, $indexSuggestions);
    }

    /**
     * @param $name
     * @param bool $new
     * @return bool|\SwFwLess\components\redis\RedisWrapper
     * @throws \Throwable
     */
    protected function openBtree($name, $new = false)
    {
        $redis = RedisPool::pick('pika');
        try {
            if (!$new) {
                //todo optimization exists result cache
                if (!$redis->exists($name)) {
                    return false;
                }
            }

            return $redis;
        } catch (\Throwable $e) {
            RedisPool::release($redis);
            throw $e;
        }
    }

    /**
     * @param $schema
     * @param $limit
     * @return array|mixed
     * @throws \Throwable
     */
    protected function fetchAllPrimaryIndexData($schema, $limit)
    {
        $indexName = $schema;

        $index = $this->openBtree($indexName);
        if ($index === false) {
            return [];
        }

        if (is_null($limit)) {
            $indexData = $this->safeUseIndex($index, function (RedisWrapper $index) use ($indexName) {
                return $index->hVals($indexName);
            });

            array_walk($indexData, function (&$val) {
                $val = json_decode($val, true);
            });

            return $indexData;
        }

        $itLimit = 10000; //must greater than 1
        $offsetLimitCount = null;
        if (!is_null($limit)) {
            $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
            $limitCount = $limit['rowcount'];
            $offsetLimitCount = $offset + $limitCount;
        }

        return $this->safeUseIndex($index, function (RedisWrapper $index) use (
            $indexName, $itLimit, $offsetLimitCount
        ) {
            $indexData = [];
            $startKey = '';
            while (($result = $index->rawCommand(
                'pkhscanrange',
                $index->_prefix($indexName),
                $startKey,
                '',
                'MATCH',
                '*',
                'LIMIT',
                $itLimit
            )) && isset($result[1])) {
                $skipFirst = ($startKey !== '');
                foreach ($result[1] as $key => $data) {
                    if ($skipFirst) {
                        if (in_array($key, [0, 1])) {
                            continue;
                        }
                    }

                    if ($key % 2 != 0) {
                        $indexData[] = json_decode($data, true);
                    } else {
                        $startKey = $data;
                    }
                }

                $resultCnt = count($result[1]);

                //EOF
                if ($resultCnt < (2 * $itLimit)) {
                    break;
                }

                if (!is_null($offsetLimitCount)) {
                    if (count($indexData) >= $offsetLimitCount) {
                        break;
                    }
                }
            }

            return $indexData;
        });
    }

    /**
     * @param $id
     * @param $schema
     * @return mixed|null
     * @throws \Throwable
     */
    protected function fetchPrimaryIndexDataById($id, $schema)
    {
        $index = $this->openBtree($schema);
        if ($index === false) {
            return null;
        }

        $indexData = $this->safeUseIndex($index, function (RedisWrapper $index) use ($id, $schema) {
            return $index->hGet($schema, $id);
        });

        if ($indexData === false) {
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
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterBasicCompareCondition(
        $schema,
        $rootCondition,
        Condition $condition,
        $limit,
        $indexSuggestions,
        $isNot
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

                $usingPrimaryIndex = ($field === 'id'); //todo fetch primary key from schema meta data

                $coroutineTotal = 3;
                $coroutineCount = 0;
                $channel = new Channel($coroutineTotal);

                for ($partitionIndex = $partitionStartIndex; $partitionIndex <= $partitionEndIndex; ++$partitionIndex) {
                    go(function () use (
                        $usingPrimaryIndex, $schema, $field, $partitionIndex,
                        $itStart, $itEnd, $itLimit, $operatorHandler,
                        $conditionOperator, $conditionValue, $rootCondition,
                        $offsetLimitCount, $channel
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

                        $subIndexData = $this->safeUseIndex($index, function (RedisWrapper $index) use (
                            $usingPrimaryIndex, $itStart, $itEnd,
                            $itLimit, $indexName, $operatorHandler,
                            $conditionOperator, $field, $conditionValue, $schema,
                            $rootCondition, $offsetLimitCount
                        ) {
                            $indexData = [];
                            $skipFirst = false;
                            while (($result = $index->rawCommand(
                                    'pkhscanrange',
                                    $index->_prefix($indexName),
                                    $itStart,
                                    $itEnd,
                                    'MATCH',
                                    '*',
                                    'LIMIT',
                                    $itLimit
                                )) && isset($result[1])) {
                                $subIndexData = [];

                                $formattedResult = [];
                                foreach ($result[1] as $key => $data) {
                                    if ($skipFirst) {
                                        if (in_array($key, [0, 1])) {
                                            continue;
                                        }
                                    }
                                    if ($key % 2 == 0) {
                                        $formattedResult[$data] = $result[1][$key + 1];
                                    }
                                }

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
                                    $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                                }
                                if ($rootCondition instanceof ConditionTree) {
                                    $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                        return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                                    });
                                }

                                $indexData = array_merge($indexData, $subIndexData);
                                if (!is_null($offsetLimitCount)) {
                                    if (count($indexData) >= $offsetLimitCount) {
                                        break;
                                    }
                                }

                                //Check EOF
                                if (count($result[1]) < (2 * $itLimit)) {
                                    break;
                                }

                                if (!$skipFirst) {
                                    $skipFirst = true;
                                }
                            }

                            return array_values($indexData);
                        });

                        $channel->push($subIndexData);
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
                $index = false;
                $indexName = null;
                $usingPrimaryIndex = false;
                $suggestIndex = $indexSuggestions[$schema][$field] ?? null;
                if (!is_null($suggestIndex)) {
                    $index = $this->openBtree($suggestIndex['indexName']);
                    if ($index !== false) {
                        $indexName = $suggestIndex['indexName'];
                        $usingPrimaryIndex = $suggestIndex['primaryIndex'];
                    }
                }
                if ($index === false) {
                    $index = $this->openBtree($schema . '.' . $field);
                    if ($index !== false) {
                        $indexName = $schema . '.' . $field;
                    }
                }
                if ($index === false) {
                    $usingPrimaryIndex = true;
                    $index = $this->openBtree($schema);
                    $indexName = $schema;
                }
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

                if ((!$usingPrimaryIndex) || ($field === 'id')) { //todo fetch primary key from schema meta data
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

                return $this->safeUseIndex($index, function (RedisWrapper $index) use (
                    $usingPrimaryIndex, $itStart, $itEnd, $itLimit, $offsetLimitCount,
                    $indexName, $operatorHandler, $conditionOperator, $field,
                    $conditionValue, $schema, $rootCondition
                ) {
                    $indexData = [];
                    $skipFirst = false;
                    while (($result = $index->rawCommand(
                            'pkhscanrange',
                            $index->_prefix($indexName),
                            $itStart,
                            $itEnd,
                            'MATCH',
                            '*',
                            'LIMIT',
                            $itLimit
                        )) && isset($result[1])) {
                        $subIndexData = [];

                        $formattedResult = [];
                        foreach ($result[1] as $key => $data) {
                            if ($skipFirst) {
                                if (in_array($key, [0, 1])) {
                                    continue;
                                }
                            }
                            if ($key % 2 == 0) {
                                $formattedResult[$data] = $result[1][$key + 1];
                            }
                        }

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
                            $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                        }
                        if ($rootCondition instanceof ConditionTree) {
                            $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                            });
                        }

                        $indexData = array_merge($indexData, $subIndexData);

                        //Check EOF
                        if (count($result[1]) < (2 * $itLimit)) {
                            break;
                        }

                        if (!$skipFirst) {
                            $skipFirst = true;
                        }

                        if (!is_null($offsetLimitCount)) {
                            if (count($indexData) >= $offsetLimitCount) {
                                break;
                            }
                        }
                    }

                    return array_values($indexData);
                });
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
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterBetweenCondition(
        $schema,
        $rootCondition,
        Condition $condition,
        $limit,
        $indexSuggestions,
        $isNot
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
                        $indexSuggestions
                    );
                } else {
                    $itStart = $operandValue2;
                    $itEnd = $operandValue3;
                }

                $partitions = $this->partitionByRange($schema, $operandValue1, $itStart, $itEnd);

                list($partitionStartIndex, $partitionEndIndex) = $partitions;

                $indexData = [];

                $usingPrimaryIndex = ($operandValue1 === 'id'); //todo fetch primary key from schema meta data

                $coroutineTotal = 3;
                $coroutineCount = 0;
                $channel = new Channel($coroutineTotal);

                for ($partitionIndex = $partitionStartIndex; $partitionIndex <= $partitionEndIndex; ++$partitionIndex) {
                    go(function () use (
                        $usingPrimaryIndex, $schema, $operandValue1, $partitionIndex,
                        $channel, $itStart, $itEnd, $itLimit, $offsetLimitCount, $operatorHandler,
                        $operandValue2, $operandValue3, $rootCondition
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

                        $subIndexData = $this->safeUseIndex($index, function (RedisWrapper $index) use (
                            $usingPrimaryIndex, $schema, $itStart, $itEnd, $itLimit, $offsetLimitCount, $operandValue1,
                            $indexName, $operatorHandler, $operandValue2, $operandValue3, $rootCondition
                        ) {
                            $indexData = [];
                            $skipFirst = false;

                            while (($result = $index->rawCommand(
                                    'pkhscanrange',
                                    $index->_prefix($indexName),
                                    $itStart,
                                    $itEnd,
                                    'MATCH',
                                    '*',
                                    'LIMIT',
                                    $itLimit
                                )) && isset($result[1])) {
                                $subIndexData = [];

                                $formattedResult = [];
                                foreach ($result[1] as $key => $data) {
                                    if ($skipFirst) {
                                        if (in_array($key, [0, 1])) {
                                            continue;
                                        }
                                    }
                                    if ($key % 2 == 0) {
                                        $formattedResult[$data] = $result[1][$key + 1];
                                    }
                                }

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
                                    $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                                }
                                if ($rootCondition instanceof ConditionTree) {
                                    $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                        return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                                    });
                                }
                                $indexData = array_merge($indexData, $subIndexData);

                                if (!is_null($offsetLimitCount)) {
                                    if (count($indexData) >= $offsetLimitCount) {
                                        break;
                                    }
                                }

                                $resultCnt = count($result[1]);

                                //EOF
                                if ($resultCnt < (2 * $itLimit)) {
                                    break;
                                }

                                if (!$skipFirst) {
                                    $skipFirst = true;
                                }
                            }

                            return array_values($indexData);
                        });

                        $channel->push($subIndexData);
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
                $index = false;
                $usingPrimaryIndex = false;
                $indexName = null;
                $suggestIndex = $indexSuggestions[$schema][$operandValue1] ?? null;
                if (!is_null($suggestIndex)) {
                    $index = $this->openBtree($suggestIndex['indexName']);
                    if ($index !== false) {
                        $indexName = $suggestIndex['indexName'];
                        $usingPrimaryIndex = $suggestIndex['primaryIndex'];
                    }
                }
                if ($index === false) {
                    $index = $this->openBtree($schema . '.' . $operandValue1);
                    if ($index !== false) {
                        $indexName = $schema . '.' . $operandValue1;
                    }
                }
                if ($index === false) {
                    $usingPrimaryIndex = true;
                    $index = $this->openBtree($schema);
                    $indexName = $schema;
                }
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

                if ((!$usingPrimaryIndex) || ($operandValue1 === 'id')) { //todo fetch primary key from schema meta data
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
                            $indexSuggestions
                        );
                    } else {
                        $itStart = $operandValue2;
                        $itEnd = $operandValue3;
                    }
                }

                return $this->safeUseIndex($index, function (RedisWrapper $index) use (
                    $usingPrimaryIndex, $schema, $itStart, $itEnd, $itLimit, $offsetLimitCount, $operandValue1,
                    $indexName, $operatorHandler, $operandValue2, $operandValue3, $rootCondition
                ) {
                    $indexData = [];
                    $skipFirst = false;

                    while (($result = $index->rawCommand(
                            'pkhscanrange',
                            $index->_prefix($indexName),
                            $itStart,
                            $itEnd,
                            'MATCH',
                            '*',
                            'LIMIT',
                            $itLimit
                        )) && isset($result[1])) {
                        $subIndexData = [];

                        $formattedResult = [];
                        foreach ($result[1] as $key => $data) {
                            if ($skipFirst) {
                                if (in_array($key, [0, 1])) {
                                    continue;
                                }
                            }
                            if ($key % 2 == 0) {
                                $formattedResult[$data] = $result[1][$key + 1];
                            }
                        }

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
                            $subIndexData = $this->fetchAllColumnsByIndexData($subIndexData, $schema);
                        }
                        if ($rootCondition instanceof ConditionTree) {
                            $subIndexData = array_filter($subIndexData, function ($row) use ($schema, $rootCondition) {
                                return $this->filterConditionTreeByIndexData($schema, $row, $rootCondition);
                            });
                        }
                        $indexData = array_merge($indexData, $subIndexData);

                        $resultCnt = count($result[1]);

                        //EOF
                        if ($resultCnt < (2 * $itLimit)) {
                            break;
                        }

                        if (!$skipFirst) {
                            $skipFirst = true;
                        }

                        if (!is_null($offsetLimitCount)) {
                            if (count($indexData) >= $offsetLimitCount) {
                                break;
                            }
                        }
                    }

                    return array_values($indexData);
                });
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
            $result = $this->filterBasicCompareCondition($schema, $rootCondition, $condition, $limit, $indexSuggestions, $isNot);
        } elseif ($conditionOperator === 'between') {
            $result = $this->filterBetweenCondition($schema, $rootCondition, $condition, $limit, $indexSuggestions, $isNot);
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
                $subCondition, $schema, $limit, $indexSuggestions, $isNot, $channel, $rootCondition
            ) {
                if ($subCondition instanceof Condition) {
                    $subResult = $this->filterCondition(
                        $schema,
                        $rootCondition,
                        $subCondition,
                        $limit,
                        $indexSuggestions,
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
                                    $indexSuggestions
                                ));
                            } else {
                                $subResult = array_merge($subResult, $this->filterConditionTree(
                                    $schema,
                                    $rootCondition,
                                    $subSubCondition,
                                    $limit,
                                    $indexSuggestions
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

        $idList = array_column($indexData, 'id'); //todo fetch primary key from schema meta data
        if (count($idList) <= 0) {
            return [];
        }

        $rows = $this->safeUseIndex($index, function (RedisWrapper $index) use ($idList, $schema) {
            return $index->hMGet($schema, $idList);
        });

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
     * @return array
     * @throws \Throwable
     */
    protected function conditionFilter($schema, $rootCondition, $condition, $limit, $indexSuggestions)
    {
        if (!is_null($condition)) {
            if ($condition instanceof Condition) {
                $indexData = $this->filterCondition($schema, $rootCondition, $condition, $limit, $indexSuggestions);
            } else {
                $indexData = $this->filterConditionTree($schema, $rootCondition, $condition, $limit, $indexSuggestions);
            }
        } else {
            $indexData = $this->fetchAllPrimaryIndexData($schema, $limit);
        }

        foreach ($indexData as $i => $row) {
            foreach ($row as $column => $value) {
                $row[$schema . '.' . $column] = $value;
            }
            $indexData[$i] = $row;
        }

        return array_values($indexData);
    }
}
