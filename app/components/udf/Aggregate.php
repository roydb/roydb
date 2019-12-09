<?php

namespace App\components\udf;

use App\components\elements\Column;

class Aggregate
{
    public static function count($parameters, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return count($resultSet);
        } else {
            if ($columnValue === '*') {
                return count($resultSet);
            } else {
                return count(array_filter(array_column($resultSet, $columnValue), function ($value) {
                    return !is_null($value);
                }));
            }
        }
    }

    public static function max($parameters, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return $columnValue;
        } else {
            if ($columnValue === '*') {
                return max(array_column($resultSet, 'id')); //todo fetch primary key from schema meta data
            } else {
                return max(array_column($resultSet, $columnValue));
            }
        }
    }

    public static function min($parameters, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return $columnValue;
        } else {
            if ($columnValue === '*') {
                return min(array_column($resultSet, 'id')); //todo fetch primary key from schema meta data
            } else {
                return min(array_column($resultSet, $columnValue));
            }
        }
    }
}
