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

    public function get($schema, $condition)
    {
        //todo $columns 应该是plan选择过的，因为某些字段不需要返回，但是查询条件可能需要用到
        return $this->conditionFilter($schema, $condition);
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

    protected function filterBasicCompareCondition($schema, Condition $condition)
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
                    return $this->fetchAllPrimaryIndexData($schema);
                }
            }
        }
        $operandValue2 = $operands[1]->getValue();
        $operandType2 = $operands[1]->getType();
        if ($operandType2 === 'colref') {
            if (strpos($operandValue2, '.')) {
                list($operandSchema2, $operandValue2) = explode('.', $operandValue2);
                if ($operandSchema2 !== $schema) {
                    return $this->fetchAllPrimaryIndexData($schema);
                }
            }
        }

        if ($operandType1 === 'colref' && $operandType2 === 'const') {
            $index = $this->openBtree($schema . '.' . $operandValue1);
            if ($index === false) {
                return $this->fetchAllPrimaryIndexData($schema);
            }
            $indexData = [];
            $nextIt = new \LevelDBIterator($index);
            for (; $nextIt->valid(); $nextIt->next()) {
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$nextIt->key(), $operandValue2])) {
                    $indexData = array_merge($indexData, json_decode($nextIt->current(), true));
                }
            }
            return $indexData;
        } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
            $index = $this->openBtree($schema . '.' . $operandValue2);
            if ($index === false) {
                return $this->fetchAllPrimaryIndexData($schema);
            }
            $indexData = [];
            $nextIt = new \LevelDBIterator($index);
            for (; $nextIt->valid(); $nextIt->next()) {
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$nextIt->key(), $operandValue1])) {
                    $indexData = array_merge(json_decode($nextIt->current(), true));
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
            return $this->fetchAllPrimaryIndexData($schema);
        }
    }

    protected function filterBetweenCondition($schema, Condition $condition)
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
                    return $this->fetchAllPrimaryIndexData($schema);
                }
            }
        }

        $operandValue2 = $operands[1]->getValue();
        $operandType2 = $operands[1]->getType();
        if ($operandType2 === 'colref') {
            if (strpos($operandValue2, '.')) {
                list($operandSchema2, $operandValue2) = explode('.', $operandValue2);
                if ($operandSchema2 !== $schema) {
                    return $this->fetchAllPrimaryIndexData($schema);
                }
            }
        }

        $operandValue3 = $operands[2]->getValue();
        $operandType3 = $operands[2]->getType();
        if ($operandType3 === 'colref') {
            if (strpos($operandValue3, '.')) {
                list($operandSchema3, $operandValue3) = explode('.', $operandValue3);
                if ($operandSchema3 !== $schema) {
                    return $this->fetchAllPrimaryIndexData($schema);
                }
            }
        }

        if ($operandType1 === 'colref' && $operandType2 === 'const' && $operandType3 === 'const') {
            $index = $this->openBtree($schema . '.' . $operandValue1);
            if ($index === false) {
                return $this->fetchAllPrimaryIndexData($schema);
            }
            $indexData = [];
            $nextIt = new \LevelDBIterator($index);
            for (; $nextIt->valid(); $nextIt->next()) {
                if ($operatorHandler->calculateOperatorExpr(
                    $conditionOperator,
                    ...[$nextIt->key(), $operandValue2, $operandValue3]
                )) {
                    $indexData = array_merge(json_decode($nextIt->current(), true));
                }
            }
            return $indexData;
        } else {
            return $this->fetchAllPrimaryIndexData($schema);
        }

        //todo support more situations
    }

    protected function filterCondition($schema, Condition $condition)
    {
        $conditionOperator = $condition->getOperator();
        if (in_array($conditionOperator, ['<', '<=', '=', '>', '>='])) {
            return $this->filterBasicCompareCondition($schema, $condition);
        } elseif ($conditionOperator === 'between') {
            return $this->filterBetweenCondition($schema, $condition);
        }

        return $this->fetchAllPrimaryIndexData($schema);

        //todo support more operators
    }

    protected function filterConditionTree($schema, ConditionTree $conditionTree)
    {
        $result = [];

        foreach ($conditionTree->getSubConditions() as $i => $subCondition) {
            if ($subCondition instanceof Condition) {
                $subResult = $this->filterCondition($schema, $subCondition);
            } else {
                $subResult = $this->filterConditionTree($schema, $subCondition);
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
     * @return array
     */
    protected function conditionFilter($schema, $condition)
    {
        //todo choose idx using plan, maybe using optimizer ?
        if (!is_null($condition)) {
            if ($condition instanceof Condition) {
                $indexData = $this->filterCondition($schema, $condition);
                foreach ($indexData as $i => $row) {
                    if (!$this->filterConditionByIndexData($schema, $row, $condition)) {
                        unset($indexData[$i]);
                    }
                }
            } else {
                $indexData = $this->filterConditionTree($schema, $condition);
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
