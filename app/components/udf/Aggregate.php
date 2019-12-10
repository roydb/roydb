<?php

namespace App\components\udf;

use App\components\elements\Aggregation;
use App\components\elements\Column;

class Aggregate
{
    public static function count($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();

        if ($row instanceof Aggregation) {
            $resultSet = $row->getRows();
        }

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

    public static function max($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return $columnValue;
        } else {
            if ($columnValue === '*') {
                $columnValue = 'id'; //todo fetch primary key from schema meta data
            }

            if ($row instanceof Aggregation) {
                return max(array_column($row->getRows(), $columnValue));
            } else {
                return max(array_column($resultSet, $columnValue));
            }
        }
    }

    public static function min($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return $columnValue;
        } else {
            if ($columnValue === '*') {
                $columnValue = 'id'; //todo fetch primary key from schema meta data
            }

            if ($row instanceof Aggregation) {
                return min(array_column($row->getRows(), $columnValue));
            } else {
                return min(array_column($resultSet, $columnValue));
            }
        }
    }

    public static function first($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return $columnValue;
        } else {
            if ($columnValue === '*') {
                $columnValue = 'id'; //todo fetch primary key from schema meta data
            }
            if ($row instanceof Aggregation) {
                return $row->getFirstRow()[$columnValue];
            } else {
                return $resultSet[0][$columnValue];
            }
        }
    }
}
