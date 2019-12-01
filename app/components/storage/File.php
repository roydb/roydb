<?php

namespace App\components\storage;

class File
{
    const TEST_ORIGIN_DATA = [
        1 => ['id' => 1, 'name' => 'foo', 'type' => 1]
    ];

    const IDX_SET = [
        'test' => [
            'name' => [
                'foo' => ['id' => 1],
            ],
        ],
    ];

    public function get($schema, $conditions, $columns = ['*'])
    {
        $resultSet = $this->conditionFilter($schema, $conditions);

        return $this->columnsFilter($resultSet, $columns);
    }

    public function conditionFilter($schema, $conditions)
    {
        $resultSet = [];

        if (!array_key_exists($schema, self::IDX_SET)) {
            return $resultSet;
        }

        $schemaIdx = self::IDX_SET[$schema];

        foreach ($conditions as $field => $condition) {
            if (!array_key_exists($field, $schemaIdx)) {
                continue;
            }

            $fieldIdx = $schemaIdx[$field];
            if (!array_key_exists($condition, $fieldIdx)) {
                continue;
            }

            $index = $fieldIdx[$condition];
            $subConditions = $conditions;
            unset($subConditions[$field]);
            if ($this->checkSubConditions($index, $subConditions)) {
                $row = self::TEST_ORIGIN_DATA[$index['id']];
                $resultSet[] = $row;
            }
        }

        return $resultSet;
    }

    protected function checkSubConditions($index, $subConditions)
    {
        foreach ($subConditions as $subField => $subCondition) {
            if ((!array_key_exists($subField, $index)) ||
                ($index[$subField] !== $subCondition)
            ) {
                return false;
            }
        }

        return true;
    }

    protected function columnsFilter($resultSet, $columns = ['*'])
    {
        if (!in_array('*', $columns)) {
            foreach ($resultSet as $i => $row) {
                foreach ($row as $k => $v) {
                    if (!in_array($k, $columns)) {
                        unset($row[$k]);
                    }
                }
                $resultSet[$i] = $row;
            }
        }

        return $resultSet;
    }
}
