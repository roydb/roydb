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

    protected function fetchAllPrimaryIndexData($schema)
    {
        $index = \btree::open(
            \SwFwLess\facades\File::storagePath() . '/btree/' . $schema
        );
        $indexData = array();
        foreach ($index->leaves() as $ptr) {
            list(, $leaf) = $index->node($ptr);
            array_push($indexData, json_decode($leaf, true));
        }
        return $indexData;
    }

    protected function fetchPrimaryIndexDataById($id, $schema)
    {
        $index = \btree::open(
            \SwFwLess\facades\File::storagePath() . '/btree/' . $schema
        );
        $indexData = $index->get($id);
        if (is_null($indexData)) {
            return null;
        } else {
            return json_decode($indexData, true);
        }
    }

    protected function filterConditionByIndexData($schema, $indexData, Condition $condition)
    {
        foreach ($indexData as $i => $row) {
            if ($condition->getOperator() === '=') {
                $operands = $condition->getOperands();
                $operandValue1 = $operands[0]->getValue();
                $operandType1 = $operands[0]->getType();
                if ($operandType1 === 'colref') {
                    if (strpos($operandValue1, '.')) {
                        list($operandSchema1, $operandValue1) = explode('.', $operandValue1);
                        if ($operandSchema1 !== $schema) {
                            continue;
                        }
                    }
                }
                $operandValue2 = $operands[1]->getValue();
                $operandType2 = $operands[1]->getType();
                if ($operandType2 === 'colref') {
                    if (strpos($operandValue2, '.')) {
                        list($operandSchema2, $operandValue2) = explode('.', $operandValue2);
                        if ($operandSchema2 !== $schema) {
                            continue;
                        }
                    }
                }

                if ($operandType1 === 'colref' && $operandType2 === 'const') {
                    if (!array_key_exists($operandValue1, $row)) {
                        $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                        $indexData[$i] = $row;
                    }
                    if (!array_key_exists($operandValue1, $row)) {
                        continue;
                    }
                    if ($row[$operandValue1] !== $operandValue2) {
                        unset($indexData[$i]);
                    }
                } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
                    if (!array_key_exists($operandValue2, $row)) {
                        $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                        $indexData[$i] = $row;
                    }
                    if (!array_key_exists($operandValue2, $row)) {
                        continue;
                    }
                    if ($row[$operandValue2] !== $operandValue1) {
                        unset($indexData[$i]);
                    }
                } elseif ($operandType1 === 'colref' && $operandType2 === 'colref') {
                    $backToPrimaryIndex = false;
                    if (!$backToPrimaryIndex) {
                        if (!array_key_exists($operandValue1, $row)) {
                            $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                            $indexData[$i] = $row;
                        }
                    }
                    if (!array_key_exists($operandValue1, $row)) {
                        continue;
                    }
                    if (!$backToPrimaryIndex) {
                        if (!array_key_exists($operandValue2, $row)) {
                            $row = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                            $indexData[$i] = $row;
                        }
                    }
                    if (!array_key_exists($operandValue2, $row)) {
                        continue;
                    }
                    if ($row[$operandValue1] !== $row[$operandValue2]) {
                        unset($indexData[$i]);
                    }
                }
            }
        }

        //todo support more operators

        return array_values($indexData);
    }

    protected function filterConditionTreeByIndexData($schema, $indexData, ConditionTree $conditionTree)
    {
        //todo support more operators

        return [];
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
                $index = \btree::open(
                    \SwFwLess\facades\File::storagePath() . '/btree/' . $schema . '.' . $operandValue1
                );
                $indexData = $index->get($operandValue2);
                if (is_null($indexData)) {
                    return [];
                } else {
                    return [json_decode($indexData, true)];
                }
            } elseif ($operandType1 === 'const' && $operandType2 === 'colref') {
                $index = \btree::open(
                    \SwFwLess\facades\File::storagePath() . '/btree/' . $schema . '.' . $operandValue2
                );
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
        //todo support more operators

        return [];
    }

    protected function conditionFilter($schema, $condition, $columns = ['*'])
    {
        //todo choose idx using plan

        if ($condition instanceof Condition) {
            $indexData = $this->filterCondition($schema, $condition);
            $indexData = $this->filterConditionByIndexData($schema, $indexData, $condition);
        } else {
            $indexData = $this->filterConditionTree($schema, $condition);
            $indexData = $this->filterConditionTreeByIndexData($schema, $indexData, $condition);
        }

        if (in_array('*', $columns)) {
            foreach ($indexData as $i => $row) {
                $indexData[$i] = $this->fetchPrimaryIndexDataById($row['id'], $schema);
            }
        } else {
            if (count($indexData) > 0) {
                foreach ($columns as $column) {
                    if (!array_key_exists($column, $indexData[0])) {
                        foreach ($indexData as $i => $row) {
                            $indexData[$i] = $this->fetchPrimaryIndexDataById($row['id'], $schema);
                        }
                        break;
                    }
                }
            }
        }

        foreach ($indexData as $i => $row) {
            foreach ($row as $column => $value) {
                $row[$schema . '.' . $column] = $value;
            }
            $indexData[$i] = $row;
        }

        return $indexData;
    }
}
