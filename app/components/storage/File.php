<?php

namespace App\components\storage;

class File
{
    public function get($schema, $conditions)
    {
        return $this->conditionFilter($schema, $conditions);
    }

    public function conditionFilter($schema, $conditions)
    {
        $resultSet = [];

        //todo choose idx using plan

        foreach ($conditions as $field => $condition) {
            $fieldIdx = \btree::open(
                \SwFwLess\facades\File::storagePath() . '/btree/' . $schema . '.' . $field
            );

            if ($fieldIdx === false) {
                continue;
            }

            $index = $fieldIdx->get($condition);
            if (is_null($index)) {
                continue;
            }
            $index = json_decode($index, true);

            $subConditions = $conditions;
            unset($subConditions[$field]);
            if ($this->checkSubConditions($index, $subConditions)) {
                $primaryIdx = \btree::open(
                    \SwFwLess\facades\File::storagePath() . '/btree/' . $schema
                );
                if ($primaryIdx === false) {
                    continue;
                }
                $row = $primaryIdx->get($index['id']);
                if (is_null($row)) {
                    continue;
                }
                $row = json_decode($row, true);
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
}
