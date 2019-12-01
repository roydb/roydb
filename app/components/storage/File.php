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
            $row = self::TEST_ORIGIN_DATA[$index['id']];
            foreach ($conditions as $subField => $subCondition) {
                if ($subField === $field) {
                    continue;
                }
                if ((!array_key_exists($subField, $index)) ||
                    ($index[$subField] !== $subCondition)
                ) {
                    $row = null;
                    break;
                }
            }
            if (is_array($row)) {
                $resultSet[] = $row;
            }
        }

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
