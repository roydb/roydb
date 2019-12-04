<?php

namespace App\components\storage;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;

class File extends AbstractStorage
{
    public function get($schema, $condition, $columns = ['*'])
    {
        return $this->conditionFilter($schema, $condition, $columns);
    }

    protected function openBtree($name, $new = false)
    {
        $btreePath = \SwFwLess\facades\File::storagePath() . '/btree/' . $name;
        if ((!$new) && (!file_exists($btreePath))) {
            return false;
        }

        return \btree::open($btreePath);
    }

    protected function fetchAllPrimaryIndexData($schema)
    {
        $index = $this->openBtree($schema);
        if ($index === false) {
            return [];
        }
        $indexData = array();
        foreach ($index->leaves() as $ptr) {
            list(, $leaf) = $index->node($ptr);
            array_walk($leaf, function (&$val) {
                $val = json_decode($val, true);
            });
            $indexData = array_merge($indexData, array_values($leaf));
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
        if (is_null($indexData)) {
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
        if ($condition->getOperator() === '=') {
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
                $indexData = $index->get($operandValue2);
                if (is_null($indexData)) {
                    return [];
                } else {
                    return [json_decode($indexData, true)];
                }
            } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
                $index = $this->openBtree($schema . '.' . $operandValue2);
                if ($index === false) {
                    return [];
                }
                $indexData = $index->get($operandValue1);
                if (is_null($indexData)) {
                    return [];
                } else {
                    return [json_decode($indexData, true)];
                }
            } elseif ($operandType1 === 'const' && $operandType2 === 'const') {
                if ($operandValue1 === $operandValue2) {
                    return $this->fetchAllPrimaryIndexData($schema);
                } else {
                    return [];
                }
            } else {
                return $this->fetchAllPrimaryIndexData($schema);
            }
        }

        //todo support more operators

        return [];
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
