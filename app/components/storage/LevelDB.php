<?php

namespace App\components\storage;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\math\OperatorHandler;

class LevelDB extends AbstractStorage
{
    protected $btreeMap = [];

    public function get($schema, $condition, $columns = ['*'])
    {
        return $this->conditionFilter($schema, $condition, $columns);
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
        //todo bugfix 支持一个 name=foo 对应多条记录
        $index = $this->openBtree($schema);
        if ($index === false) {
            return [];
        }
        $indexData = array();
        $it = new \LevelDBIterator($index); // equals to： $it = $db->getIterator();
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
        if ($condition->getOperator() === '=') {
            $operands = $condition->getOperands();
            $operandValue1 = $operands[0]->getValue();
            $operandType1 = $operands[0]->getType();
            if ($operandType1 === 'colref') {
                if (strpos($operandValue1, '.')) {
                    list($operandSchema1, $operandValue1) = explode('.', $operandValue1);
                    if ($operandSchema1 !== $schema) {
                        return true;
                    }
                }
            }
            $operandValue2 = $operands[1]->getValue();
            $operandType2 = $operands[1]->getType();
            if ($operandType2 === 'colref') {
                if (strpos($operandValue2, '.')) {
                    list($operandSchema2, $operandValue2) = explode('.', $operandValue2);
                    if ($operandSchema2 !== $schema) {
                        return true;
                    }
                }
            }

            if ($operandType1 === 'colref' && $operandType2 === 'const') {
                if (!array_key_exists($operandValue1, $row)) {
                    $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                }
                if (!array_key_exists($operandValue1, $row)) {
                    return true;
                }
                if ($row[$operandValue1] !== $operandValue2) {
                    return false;
                }
            } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
                if (!array_key_exists($operandValue2, $row)) {
                    $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                }
                if (!array_key_exists($operandValue2, $row)) {
                    return true;
                }
                if ($row[$operandValue2] !== $operandValue1) {
                    return false;
                }
            } elseif ($operandType1 === 'colref' && $operandType2 === 'colref') {
                $backToPrimaryIndex = false;
                if (!$backToPrimaryIndex) {
                    if (!array_key_exists($operandValue1, $row)) {
                        $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                    }
                }
                if (!array_key_exists($operandValue1, $row)) {
                    return true;
                }
                if (!$backToPrimaryIndex) {
                    if (!array_key_exists($operandValue2, $row)) {
                        $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                    }
                }
                if (!array_key_exists($operandValue2, $row)) {
                    return true;
                }
                if ($row[$operandValue1] !== $row[$operandValue2]) {
                    return false;
                }
            }
        }

        return true;
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

    protected function filterCondition($schema, Condition $condition)
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
                return [];
            }
            $indexData = [];
            $prevIt = new \LevelDBIterator($index);
            if ($index->get($operandValue2) === false) {
                $prevIt->last();
            } else {
                $prevIt->seek($operandValue2);
            }
            for(; $prevIt->valid(); $prevIt->prev()) {
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$prevIt->key(), $operandValue2])) {
                    $indexData[] = json_decode($prevIt->current(), true);
                } else {
                    //todo const range operator
                    if (in_array($conditionOperator, ['=', '<', '<=', '>', '>='])) {
                        break;
                    }
                }
            }
            $nextIt = new \LevelDBIterator($index);
            if ($index->get($operandValue2) !== false) {
                $nextIt->seek($operandValue2);
            } else {
                $nextIt->rewind();
            }
            for(; $nextIt->valid();$nextIt->next()) {
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$nextIt->key(), $operandValue2])) {
                    $indexData[] = json_decode($nextIt->current(), true);
                } else {
                    //todo const range operator
                    if (in_array($conditionOperator, ['=', '<', '<=', '>', '>='])) {
                        break;
                    }
                }
            }
            return $indexData;
        } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
            $index = $this->openBtree($schema . '.' . $operandValue2);
            if ($index === false) {
                return [];
            }
            $indexData = [];
            $prevIt = new \LevelDBIterator($index);
            if ($index->get($operandValue1) === false) {
                $prevIt->last();
            } else {
                $prevIt->seek($operandValue1);
            }
            for(; $prevIt->valid(); $prevIt->prev()) {
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$operandValue1, $prevIt->key()])) {
                    $indexData[] = json_decode($prevIt->current(), true);
                } else {
                    //todo const range operator
                    if (in_array($conditionOperator, ['=', '<', '<=', '>', '>='])) {
                        break;
                    }
                }
            }
            $nextIt = new \LevelDBIterator($index);
            if ($index->get($operandValue1) !== false) {
                $nextIt->seek($operandValue1);
            } else {
                $nextIt->rewind();
            }
            for(; $nextIt->valid();$nextIt->next()) {
                if ($operatorHandler->calculateOperatorExpr($conditionOperator, ...[$nextIt->key(), $operandValue1])) {
                    $indexData[] = json_decode($nextIt->current(), true);
                } else {
                    //todo const range operator
                    if (in_array($conditionOperator, ['=', '<', '<=', '>', '>='])) {
                        break;
                    }
                }
            }
            //todo optimizen 范围类型、等于类型的查询，不满足条件时终止
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

        return $result;
    }

    protected function conditionFilter($schema, $condition, $columns = ['*'])
    {
        //todo choose idx using plan

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

        if (in_array('*', $columns)) {
            foreach ($indexData as $i => $row) {
                $indexData[$i] = $this->fetchPrimaryIndexDataById($row['id'], $schema);
            }
        } else {
            foreach ($indexData as $i => $row) {
                foreach ($columns as $column) {
                    if (!array_key_exists($column, $indexData[0])) {
                        $indexData[$i] = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                        break;
                    }
                }
            }
        }

        $idMap = [];
        foreach ($indexData as $i => $row) {
            if (in_array($row['id'], $idMap)) {
                unset($indexData[$i]);
                continue;
            } else {
                $idMap[] = $row['id'];
            }
            foreach ($row as $column => $value) {
                $row[$schema . '.' . $column] = $value;
            }
            $indexData[$i] = $row;
        }

        return array_values($indexData);
    }
}
