<?php

namespace App\components\storage\leveldb;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\math\OperatorHandler;
use App\components\storage\AbstractStorage;
use SwFwLess\components\traits\Singleton;

class LevelDB extends AbstractStorage
{
    use Singleton;

    protected $btreeMap = [];

    private function __construct()
    {
        $this->openBtree('meta.schema');
        $this->openBtree('test');
        $this->openBtree('test.name');
        $this->openBtree('test2');
        $this->openBtree('test2.name');
    }

    public function getSchemaMetaData($schema)
    {
        $metaSchema = $this->openBtree('meta.schema');
        $schemaData = $metaSchema->get($schema);
        if ($schemaData === false) {
            return null;
        }

        return json_decode($schemaData, true);
    }

    public function get($schema, $condition, $limit, $indexSuggestions)
    {
        //todo $columns 应该是plan选择过的，因为某些字段不需要返回，但是查询条件可能需要用到
        return $this->conditionFilter($schema, $condition, $limit, $indexSuggestions);
    }

    protected function openBtree($name, $new = false)
    {
        if (isset($this->btreeMap[$name])) {
            return $this->btreeMap[$name];
        }

        $btreePath = \SwFwLess\facades\File::storagePath() . '/btree/' . $name;
        if ((!$new) && (!is_dir($btreePath))) {
            return false;
        }

        /* default open options */
        $options = array(
            'create_if_missing' => true,	// if the specified database didn't exist will create a new one
            'error_if_exists'	=> false,	// if the opened database exsits will throw exception
            'paranoid_checks'	=> false,
            'block_cache_size'	=> 8 * (2 << 20),
            'write_buffer_size' => 4<<20,
            'block_size'		=> 4096,
            'max_open_files'	=> 1000,
            'block_restart_interval' => 16,
            'compression'		=> LEVELDB_SNAPPY_COMPRESSION,
            'comparator'		=> NULL,   // any callable parameter which returns 0, -1, 1
        );
        /* default readoptions */
        $readoptions = array(
            'verify_check_sum'	=> false,
            'fill_cache'		=> true,
            'snapshot'			=> null
        );

        /* default write options */
        $writeoptions = array(
            'sync' => false
        );

        return $this->btreeMap[$name] = new \LevelDB($btreePath, $options, $readoptions, $writeoptions);
    }

    protected function fetchAllPrimaryIndexData($schema)
    {
        //todo optimize for storage get limit
        $index = $this->openBtree($schema);
        if ($index === false) {
            return [];
        }
        $indexData = array();
        $it = new \LevelDBIterator($index);
        foreach($it as $key => $value) {
            $indexData[] = json_decode($value, true);
        }
        return $indexData;
    }

    protected function fetchPrimaryIndexDataById($id, $schema)
    {
        $index = $this->openBtree($schema);
        if ($index === false) {
            return null;
        }
        $indexData = $index->get($id);
        if ($indexData === false) {
            return null;
        } else {
            return json_decode($indexData, true);
        }
    }

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

        if ($operandType1 === 'colref' && $operandType2 === 'const') {
            $usingPrimaryIndex = false;
            $index = $this->openBtree($schema . '.' . $operandValue1);
            if ($index === false) {
                $usingPrimaryIndex = true;
                $index = $this->openBtree($schema);
            }
            $indexData = [];
            $matched = false;
            $nextIt = new \LevelDBIterator($index);
            if (!$usingPrimaryIndex) {
                if ($conditionOperator === '=') {
                    if ($index->get($operandValue2) !== false) {
                        $nextIt->seek($operandValue2);
                    }
                }
            }
            for (; $nextIt->valid(); $nextIt->next()) {
                $row = json_decode($nextIt->current(), true);
                if ($usingPrimaryIndex) {
                    if (!array_key_exists($operandValue1, $row)) {
                        break;
                    }
                    $indexColumnValue = $row[$operandValue1];
                } else {
                    $indexColumnValue = $nextIt->key();
                }
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$indexColumnValue, $operandValue2])) {
                    if ($usingPrimaryIndex) {
                        $indexData[] = $row;
                    } else {
                        $indexData = array_merge($indexData, $row);
                    }
                    $matched = true;
                    if (!is_null($limit)) {
                        $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                        $limitCount = $limit['rowcount'];
                        if (count($indexData) === ($offset + $limitCount)) {
                            break;
                        }
                    }
                } else {
                    if ($matched && ($conditionOperator === '=')) {
                        break;
                    }
                }
            }
            return $indexData;
        } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
            $usingPrimaryIndex = false;
            $index = $this->openBtree($schema . '.' . $operandValue2);
            if ($index === false) {
                $usingPrimaryIndex = true;
                $index = $this->openBtree($schema);
            }
            $indexData = [];
            $matched = false;
            $nextIt = new \LevelDBIterator($index);
            if (!$usingPrimaryIndex) {
                if ($conditionOperator === '=') {
                    if ($index->get($operandValue1) !== false) {
                        $nextIt->seek($operandValue1);
                    }
                }
            }
            for (; $nextIt->valid(); $nextIt->next()) {
                $row = json_decode($nextIt->current(), true);
                if ($usingPrimaryIndex) {
                    if (!array_key_exists($operandValue2, $row)) {
                        break;
                    }
                    $indexColumnValue = $row[$operandValue2];
                } else {
                    $indexColumnValue = $nextIt->key();
                }
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$indexColumnValue, $operandValue1])) {
                    if ($usingPrimaryIndex) {
                        $indexData[] = $row;
                    } else {
                        $indexData = array_merge($indexData, $row);
                    }
                    $matched = true;
                    if (!is_null($limit)) {
                        $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                        $limitCount = $limit['rowcount'];
                        if (count($indexData) === ($offset + $limitCount)) {
                            break;
                        }
                    }
                } else {
                    if ($matched && ($conditionOperator === '=')) {
                        break;
                    }
                }
            }
            return $indexData;
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

    protected function filterBetweenCondition($schema, Condition $condition, $limit, $indexSuggestions)
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
            $indexData = [];
            $nextIt = new \LevelDBIterator($index);
            for (; $nextIt->valid(); $nextIt->next()) {
                $row = json_decode($nextIt->current(), true);
                if ($usingPrimaryIndex) {
                    if (!array_key_exists($operandValue1, $row)) {
                        break;
                    }
                    $indexColumnValue = $row[$operandValue1];
                } else {
                    $indexColumnValue = $nextIt->key();
                }
                if ($operatorHandler->calculateOperatorExpr(
                    $conditionOperator,
                    ...[$indexColumnValue, $operandValue2, $operandValue3]
                )) {
                    if ($usingPrimaryIndex) {
                        $indexData[] = $row;
                    } else {
                        $indexData = array_merge($indexData, $row);
                    }
                    if (!is_null($limit)) {
                        $offset = $limit['offset'] === '' ? 0 : $limit['offset'];
                        $limitCount = $limit['rowcount'];
                        if (count($indexData) === ($offset + $limitCount)) {
                            break;
                        }
                    }
                }
            }
            return $indexData;
        } else {
            return [];
        }
    }

    protected function filterCondition($schema, Condition $condition, $limit, $indexSuggestions)
    {
        $conditionOperator = $condition->getOperator();
        if (in_array($conditionOperator, ['<', '<=', '=', '>', '>='])) {
            return $this->filterBasicCompareCondition($schema, $condition, $limit, $indexSuggestions);
        } elseif ($conditionOperator === 'between') {
            return $this->filterBetweenCondition($schema, $condition, $limit, $indexSuggestions);
        }

        return [];

        //todo support more operators
    }

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
