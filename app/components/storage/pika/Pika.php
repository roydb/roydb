<?php

namespace App\components\storage\pika;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\math\OperatorHandler;
use App\components\storage\AbstractStorage;
use SwFwLess\components\redis\RedisWrapper;
use SwFwLess\components\traits\Singleton;
use SwFwLess\facades\RedisPool;

class Pika extends AbstractStorage
{
    use Singleton;

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
        $metaSchema = $this->openBtree('meta.schema');
        $schemaData = $this->safeUseIndex($metaSchema, function (RedisWrapper $metaSchema) use ($schema) {
            return $metaSchema->hGet('meta.schema', $schema);
        });
        if ($schemaData === false) {
            return null;
        }

        return json_decode($schemaData, true);
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
        //todo $columns 应该是plan选择过的，因为某些字段不需要返回，但是查询条件可能需要用到
        return $this->conditionFilter($schema, $condition, $limit, $indexSuggestions);
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
     * @return array|mixed
     * @throws \Throwable
     */
    protected function fetchAllPrimaryIndexData($schema)
    {
        //todo optimize for storage get limit
        $index = $this->openBtree($schema);
        if ($index === false) {
            return [];
        }

        return $this->safeUseIndex($index, function (RedisWrapper $index) use ($schema) {
            $indexData = [];
            $startKey = '';
            while ($result = $index->rawCommand(
                'pkhscanrange',
                $index->_prefix($schema),
                $startKey,
                '',
                'MATCH',
                '*',
                'LIMIT',
                100
            )) {
                if (isset($result[1])) {
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
                            $startKey = $key;
                        }
                    }
                } else {
                    break;
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
        foreach ($operands as $operand) {
            $operandType = $operand->getType();
            $operandValue = $operand->getValue();
            if ($operandType === 'colref') {
                if (strpos($operandValue, '.')) {
                    list($operandSchema, $operandValue) = explode('.', $operandValue);
                    if ($operandSchema !== $schema) {
                        return true;
                    }
                }
                if (!array_key_exists($operandValue, $row)) {
                    $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                }
                if (!array_key_exists($operandValue, $row)) {
                    return true;
                }
                $operandValues[] = $row[$operandValue];
            } else {
                $operandValues[] = $operandValue;
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
     * @param Condition $condition
     * @param $limit
     * @param $indexSuggestions
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterBasicCompareCondition($schema, Condition $condition, $limit, $indexSuggestions)
    {
        $operatorHandler = new OperatorHandler();
        $conditionOperator = $condition->getOperator();
        $operands = $condition->getOperands();
        $operandValue1 = $operands[0]->getValue();
        $operandType1 = $operands[0]->getType();
        if ($operandType1 === 'colref') {
            if (strpos($operandValue1, '.')) {
                list($operandSchema1, $operandValue1) = explode('.', $operandValue1);
                if ($operandSchema1 !== $schema) {
                    return [];
                }
            }
        }
        $operandValue2 = $operands[1]->getValue();
        $operandType2 = $operands[1]->getType();
        if ($operandType2 === 'colref') {
            if (strpos($operandValue2, '.')) {
                list($operandSchema2, $operandValue2) = explode('.', $operandValue2);
                if ($operandSchema2 !== $schema) {
                    return [];
                }
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

            $usingPrimaryIndex = false;
            $index = $this->openBtree($schema . '.' . $field);
            if ($index === false) {
                $usingPrimaryIndex = true;
                $index = $this->openBtree($schema);
            }
            $itStart = '';
            $itEnd = '';
            $skipStart = false;
            $skipEnd = false;
            $itLimit = 100;
            $offset = null;
            $limitCount = null;
            $offsetLimitCount = null;
            if (!is_null($limit)) {
                $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                $itLimit = $limitCount = $limit['rowcount'];
                $offsetLimitCount = $offset + $limitCount;
                if ($skipStart) {
                    $offsetLimitCount += 1;
                }
                if ($skipEnd) {
                    $offsetLimitCount += 1;
                }
            }

            if (!$usingPrimaryIndex) {
                if ($conditionOperator === '=') {
                    $itStart = $conditionValue;
                    $itEnd = $conditionValue;
                } elseif ($conditionOperator === '<') {
                    $itEnd = $conditionValue;
                    $skipEnd = true;
                } elseif ($conditionOperator === '<=') {
                    $itEnd = $conditionValue;
                } elseif ($conditionOperator === '>') {
                    $itStart = $conditionValue;
                    $skipStart = true;
                } elseif ($conditionOperator === '>=') {
                    $itStart = $conditionValue;
                }
            }

            return $this->safeUseIndex($index, function (RedisWrapper $index) use (
                $usingPrimaryIndex, $schema, $itStart, $itEnd, $skipStart, $skipEnd,
                $itLimit, $offsetLimitCount, $field
            ) {
                $indexData = [];
                $skipFirst = false;
                while (($result = $index->rawCommand(
                    'pkhscanrange',
                    $usingPrimaryIndex ? $index->_prefix($schema) : $index->_prefix($schema . '.' . $field),
                    $itStart,
                    $itEnd,
                    'MATCH',
                    '*',
                    'LIMIT',
                    $itLimit
                )) && isset($result[1])) {
                    foreach ($result[1] as $key => $data) {
                        if ($skipFirst && in_array($key, [0, 1])) {
                            continue;
                        }

                        if ($key % 2 != 0) {
                            if ($usingPrimaryIndex) {
                                $indexData[] = json_decode($data, true);
                            } else {
                                $indexData = array_merge($indexData, json_decode($data, true));
                            }
                        } else {
                            $itStart = $data;
                        }
                    }

                    $resultCnt = count($result[1]);

                    if ($skipFirst) {
                        if ($resultCnt <= 2) {
                            break;
                        }
                    } else {
                        if ($resultCnt <= 0) {
                            break;
                        }
                    }

                    if (($resultCnt > 1) && (!$skipFirst)) {
                        $skipFirst = true;
                    }

                    if (!is_null($offsetLimitCount)) {
                        if (count($indexData) >= $offsetLimitCount) {
                            break;
                        }
                    }
                }

                if ($skipStart) {
                    array_shift($indexData);
                }
                if ($skipEnd) {
                    array_pop($indexData);
                }

                return array_values($indexData);
            });
        } elseif ($operandType1 === 'const' && $operandType2 === 'const') {
            if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$operandValue1, $operandValue2])) {
                return $this->fetchAllPrimaryIndexData($schema);
            } else {
                return [];
            }
        } else {
            return [];
        }
    }

    /**
     * @param $schema
     * @param Condition $condition
     * @param $limit
     * @param $indexSuggestions
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterBetweenCondition($schema, Condition $condition, $limit, $indexSuggestions)
    {
        $operands = $condition->getOperands();

        $operandValue1 = $operands[0]->getValue();
        $operandType1 = $operands[0]->getType();
        if ($operandType1 === 'colref') {
            if (strpos($operandValue1, '.')) {
                list($operandSchema1, $operandValue1) = explode('.', $operandValue1);
                if ($operandSchema1 !== $schema) {
                    return [];
                }
            }
        }

        $operandValue2 = $operands[1]->getValue();
        $operandType2 = $operands[1]->getType();
        if ($operandType2 === 'colref') {
            if (strpos($operandValue2, '.')) {
                list($operandSchema2, $operandValue2) = explode('.', $operandValue2);
                if ($operandSchema2 !== $schema) {
                    return [];
                }
            }
        }

        $operandValue3 = $operands[2]->getValue();
        $operandType3 = $operands[2]->getType();
        if ($operandType3 === 'colref') {
            if (strpos($operandValue3, '.')) {
                list($operandSchema3, $operandValue3) = explode('.', $operandValue3);
                if ($operandSchema3 !== $schema) {
                    return [];
                }
            }
        }

        if ($operandType1 === 'colref' && $operandType2 === 'const' && $operandType3 === 'const') {
            $usingPrimaryIndex = false;
            $index = $this->openBtree($schema . '.' . $operandValue1);
            if ($index === false) {
                $usingPrimaryIndex = true;
                $index = $this->openBtree($schema);
            }
            $itStart = '';
            $itEnd = '';
            $itLimit = 100;
            $offset = null;
            $limitCount = null;
            $offsetLimitCount = null;
            if (!is_null($limit)) {
                $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                $itLimit = $limitCount = $limit['rowcount'];
                $offsetLimitCount = $offset + $limitCount;
            }

            if (!$usingPrimaryIndex) {
                $itStart = $operandValue2;
                $itEnd = $operandValue3;
            }

            return $this->safeUseIndex($index, function (RedisWrapper $index) use (
                $usingPrimaryIndex, $schema, $itStart, $itEnd, $itLimit, $offsetLimitCount, $operandValue1
            ) {
                $indexData = [];
                $skipFirst = false;
                while (($result = $index->rawCommand(
                        'pkhscanrange',
                        $usingPrimaryIndex ?
                            $index->_prefix($schema) :
                            $index->_prefix($schema . '.' . $operandValue1),
                        $itStart,
                        $itEnd,
                        'MATCH',
                        '*',
                        'LIMIT',
                        $itLimit
                    )) && isset($result[1])) {
                    foreach ($result[1] as $key => $data) {
                        if ($skipFirst && in_array($key, [0, 1])) {
                            continue;
                        }

                        if ($key % 2 != 0) {
                            if ($usingPrimaryIndex) {
                                $indexData[] = json_decode($data, true);
                            } else {
                                $indexData = array_merge($indexData, json_decode($data, true));
                            }
                        } else {
                            $itStart = $data;
                        }
                    }

                    $resultCnt = count($result[1]);

                    if ($skipFirst) {
                        if ($resultCnt <= 2) {
                            break;
                        }
                    } else {
                        if ($resultCnt <= 0) {
                            break;
                        }
                    }

                    if (($resultCnt > 1) && (!$skipFirst)) {
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
        } else {
            return [];
        }
    }

    /**
     * @param $schema
     * @param Condition $condition
     * @param $limit
     * @param $indexSuggestions
     * @return array|mixed
     * @throws \Throwable
     */
    protected function filterCondition($schema, Condition $condition, $limit, $indexSuggestions)
    {
        $conditionOperator = $condition->getOperator();
        if (in_array($conditionOperator, ['<', '<=', '=', '>', '>='])) {
            return $this->filterBasicCompareCondition($schema, $condition, $limit, $indexSuggestions);
        } elseif ($conditionOperator === 'between') {
            return $this->filterBetweenCondition($schema, $condition, $limit, $indexSuggestions);
        }

        return $this->fetchAllPrimaryIndexData($schema);

        //todo support more operators
    }

    /**
     * @param $schema
     * @param ConditionTree $conditionTree
     * @param $limit
     * @param $indexSuggestions
     * @return array
     * @throws \Throwable
     */
    protected function filterConditionTree($schema, ConditionTree $conditionTree, $limit, $indexSuggestions)
    {
        $result = [];

        foreach ($conditionTree->getSubConditions() as $i => $subCondition) {
            if ($subCondition instanceof Condition) {
                $subResult = $this->filterCondition($schema, $subCondition, $limit, $indexSuggestions);
            } else {
                $subResult = $this->filterConditionTree($schema, $subCondition, $limit, $indexSuggestions);
            }

            $result = array_merge($result, $subResult);
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
     * Fetching index data by single condition, then filtering index data by all conditions.
     *
     * @param $schema
     * @param $condition
     * @param $limit
     * @param $indexSuggestions
     * @return array
     * @throws \Throwable
     */
    protected function conditionFilter($schema, $condition, $limit, $indexSuggestions)
    {
        //todo choose idx using plan, maybe using optimizer ?
        if (!is_null($condition)) {
            if ($condition instanceof Condition) {
                $indexData = $this->filterCondition($schema, $condition, $limit, $indexSuggestions);
            } else {
                $indexData = $this->filterConditionTree($schema, $condition, $limit, $indexSuggestions);
                foreach ($indexData as $i => $row) {
                    if (!$this->filterConditionTreeByIndexData($schema, $row, $condition)) {
                        unset($indexData[$i]);
                    }
                }
            }

            foreach ($indexData as $i => $row) {
                $indexData[$i] = $this->fetchPrimaryIndexDataById($row['id'], $schema);
            }
        } else {
            $indexData = $this->fetchAllPrimaryIndexData($schema);
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
