<?php

namespace App\components\storage;

use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;

class File
{
    public function get($schema, $condition)
    {
        return $this->conditionFilter($schema, $condition);
    }

    protected function filterCondition($schema, Condition $condition)
    {
        if ($condition->getOperator() === '=') {
            $operands = $condition->getOperands();
            $fieldIdx = \btree::open(
                \SwFwLess\facades\File::storagePath() . '/btree/' . $schema . '.' . $operands[0]
            );

            if ($fieldIdx === false) {
                return null;
            }

            $index = $fieldIdx->get($operands[1]);
            if (is_null($index)) {
                return null;
            }
            $index = json_decode($index, true);

            return $index;
        }

        //todo support more operators

        return null;
    }

    protected function filterConditionTree($schema, ConditionTree $conditionTree)
    {
        //todo support more operators

        return null;
    }

    public function conditionFilter($schema, $condition)
    {
        $resultSet = [];

        //todo choose idx using plan

        $index = null;
        if ($condition instanceof Condition) {
            $index = $this->filterCondition($schema, $condition);
        } else {
            $index = $this->filterConditionTree($schema, $condition);
        }

        if (!is_null($index)) {
            $primaryIdx = \btree::open(
                \SwFwLess\facades\File::storagePath() . '/btree/' . $schema
            );
            if ($primaryIdx === false) {
                return $resultSet;
            }
            $row = $primaryIdx->get($index['id']);
            if (is_null($row)) {
                return $resultSet;
            }
            $row = json_decode($row, true);
            $resultSet[] = $row;
        }

        return $resultSet;
    }
}
