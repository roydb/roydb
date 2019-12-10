<?php

namespace App\components\udf;

use App\components\elements\Aggregation;
use App\components\elements\Column;

class Math
{
    public static function sin($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];
        if ($column->getType() === 'const') {
            return sin($column->getValue());
        } else {
            if ($row instanceof Aggregation) {
                $degree = $row->getFirstRow()[$column->getValue()];
            } else {
                $degree = $row[$column->getValue()];
            }
            return sin($degree);
        }
    }

    public static function cos($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];
        if ($column->getType() === 'const') {
            return cos($column->getValue());
        } else {
            if ($row instanceof Aggregation) {
                $degree = $row->getFirstRow()[$column->getValue()];
            } else {
                $degree = $row[$column->getValue()];
            }
            return cos($degree);
        }
    }

    public static function sqrt($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];
        if ($column->getType() === 'const') {
            return sqrt($column->getValue());
        } else {
            if ($row instanceof Aggregation) {
                $number = $row->getFirstRow()[$column->getValue()];
            } else {
                $number = $row[$column->getValue()];
            }
            return sqrt($number);
        }
    }

    public static function pow($parameters, $row, $resultSet)
    {
        /** @var Column $baseColumn */
        $baseColumn = $parameters[0];
        if ($baseColumn->getType() === 'const') {
            $base = $baseColumn->getValue();
        } else {
            if ($row instanceof Aggregation) {
                $base = $row->getFirstRow()[$baseColumn->getValue()];
            } else {
                $base = $row[$baseColumn->getValue()];
            }
        }

        /** @var Column $expColumn */
        $expColumn = $parameters[1];
        if ($expColumn->getType() === 'const') {
            $exp = $expColumn->getValue();
        } else {
            if ($row instanceof Aggregation) {
                $exp = $row->getFirstRow()[$expColumn->getValue()];
            } else {
                $exp = $row[$expColumn->getValue()];
            }
        }

        return pow($base, $exp);
    }
}
